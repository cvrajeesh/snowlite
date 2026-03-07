//! Database connection — the primary public API surface.
//!
//! [`Connection`] wraps a `rusqlite::Connection` and intercepts every SQL
//! statement, passing it through the [`Translator`] before execution.

use std::path::Path;

use rusqlite::types::ValueRef;

use crate::error::Error;
use crate::row::Row;
use crate::translator::rewriter::{Translator, TranslatorConfig};
use crate::{Result, Value};

/// Connection configuration.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Translator configuration — controls how Snowflake SQL is rewritten.
    pub translator: TranslatorConfig,
}

impl Config {
    pub fn new() -> Self {
        Config::default()
    }

    /// Enable schema prefixes: `public.orders` becomes `public__orders`.
    pub fn with_schema_prefix(mut self) -> Self {
        self.translator.use_schema_prefix = true;
        self
    }

    /// Use `DROP TABLE IF EXISTS` before `CREATE TABLE` instead of
    /// `CREATE TABLE IF NOT EXISTS` when translating `CREATE OR REPLACE TABLE`.
    pub fn with_drop_before_create(mut self) -> Self {
        self.translator.drop_before_create = true;
        self
    }
}

/// A connection to a local SQLite database that understands Snowflake SQL.
///
/// This is the main entry point for the crate.  It wraps a `rusqlite::Connection`
/// and transparently translates Snowflake SQL to SQLite SQL before execution.
///
/// # Thread safety
///
/// `Connection` is **not** `Send` or `Sync` — this mirrors `rusqlite::Connection`.
/// For concurrent access, create one `Connection` per thread/task.
pub struct Connection {
    inner: rusqlite::Connection,
    translator: Translator,
}

impl Connection {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Open an in-memory SQLite database with default configuration.
    ///
    /// The database is destroyed when the `Connection` is dropped.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_config(Config::default())
    }

    /// Open an in-memory SQLite database with the given configuration.
    pub fn open_in_memory_with_config(config: Config) -> Result<Self> {
        let inner = rusqlite::Connection::open_in_memory()?;
        Self::init(inner, config)
    }

    /// Open a file-backed SQLite database at `path` with default configuration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    /// Open a file-backed SQLite database at `path` with the given configuration.
    pub fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let inner = rusqlite::Connection::open(path)?;
        Self::init(inner, config)
    }

    // ── SQL execution ────────────────────────────────────────────────────────

    /// Execute a statement that does not return rows (DDL, DML, etc.).
    ///
    /// Returns the number of rows affected.
    ///
    /// ```rust,no_run
    /// # use local_db::Connection;
    /// # fn main() -> local_db::Result<()> {
    /// let conn = Connection::open_in_memory()?;
    /// conn.execute("CREATE TABLE t (id INTEGER)", &[])?;
    /// conn.execute("INSERT INTO t VALUES (?)", &[&1i64])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> Result<usize> {
        match self.translator.translate(sql)? {
            None => Ok(0), // no-op
            Some(translated) => {
                // Some translations may produce two statements (e.g. DROP + CREATE)
                let stmts = crate::translator::rewriter::split_statements(&translated);
                let mut total = 0usize;
                for stmt in stmts {
                    let stmt = stmt.trim();
                    if stmt.is_empty() {
                        continue;
                    }
                    total += self.inner.execute(stmt, params)?;
                }
                Ok(total)
            }
        }
    }

    /// Execute a query and return all matching rows.
    ///
    /// ```rust,no_run
    /// # use local_db::{Connection, Row};
    /// # fn main() -> local_db::Result<()> {
    /// let conn = Connection::open_in_memory()?;
    /// # conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])?;
    /// let rows = conn.query("SELECT id, name FROM t WHERE id > ?", &[&0i64])?;
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> Result<Vec<Row>> {
        let translated = match self.translator.translate(sql)? {
            None => return Ok(vec![]),
            Some(t) => t,
        };

        let mut stmt = self.inner.prepare(&translated)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let rows = stmt
            .query_map(params, |row| {
                let values: Vec<Value> = (0..column_names.len())
                    .map(|i| sqlite_value_to_value(row.get_ref(i).unwrap_or(ValueRef::Null)))
                    .collect();
                Ok(values)
            })?
            .map(|r| {
                r.map(|values| Row::new(column_names.clone(), values))
                    .map_err(Error::from)
            })
            .collect::<Result<Vec<Row>>>()?;

        Ok(rows)
    }

    /// Execute a query and return only the first row, or `None` if the result
    /// set is empty.
    pub fn query_one(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<Option<Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// Execute a batch of semicolon-separated SQL statements.
    ///
    /// Useful for running schema migration scripts.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let stmts = self.translator.translate_batch(sql)?;
        for stmt in stmts {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                self.inner.execute_batch(stmt)?;
            }
        }
        Ok(())
    }

    // ── Transactions ─────────────────────────────────────────────────────────

    /// Begin a transaction, run `f`, and commit if `f` returns `Ok`.
    /// If `f` returns `Err`, the transaction is rolled back.
    pub fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        self.inner.execute_batch("BEGIN")?;
        match f(self) {
            Ok(v) => {
                self.inner.execute_batch("COMMIT")?;
                Ok(v)
            }
            Err(e) => {
                let _ = self.inner.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    // ── Raw access ───────────────────────────────────────────────────────────

    /// Access the underlying `rusqlite::Connection` directly.
    ///
    /// Use this to register custom functions, load extensions, etc.
    pub fn raw(&self) -> &rusqlite::Connection {
        &self.inner
    }

    // ── Private ──────────────────────────────────────────────────────────────

    fn init(inner: rusqlite::Connection, config: Config) -> Result<Self> {
        // Enable WAL mode for better concurrent read performance
        inner.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        // Register custom SQLite functions that Snowflake has but SQLite lacks
        register_custom_functions(&inner)?;
        Ok(Connection {
            inner,
            translator: Translator::with_config(config.translator),
        })
    }
}

// ── Custom SQLite function registrations ────────────────────────────────────

fn register_custom_functions(conn: &rusqlite::Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;

    // REGEXP support (used by REGEXP_LIKE / RLIKE)
    conn.create_scalar_function(
        "regexp",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pattern: String = ctx.get(0)?;
            let text: String = ctx.get(1)?;
            let re = regex::Regex::new(&pattern).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    e.to_string(),
                )))
            })?;
            Ok(re.is_match(&text))
        },
    )?;

    // SPLIT_PART(string, delimiter, part_number)
    conn.create_scalar_function(
        "split_part",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let s: String = ctx.get(0)?;
            let delim: String = ctx.get(1)?;
            let n: i64 = ctx.get(2)?;
            let parts: Vec<&str> = s.split(delim.as_str()).collect();
            let idx = if n > 0 { (n - 1) as usize } else { 0 };
            Ok(parts.get(idx).map(|s| s.to_string()).unwrap_or_default())
        },
    )?;

    // STRTOK(string, delimiters, part_number)
    conn.create_scalar_function(
        "strtok",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let s: String = ctx.get(0)?;
            let delims: String = ctx.get(1)?;
            let n: i64 = ctx.get(2)?;
            let delim_chars: Vec<char> = delims.chars().collect();
            let parts: Vec<&str> = s
                .split(|c| delim_chars.contains(&c))
                .filter(|p| !p.is_empty())
                .collect();
            let idx = if n > 0 { (n - 1) as usize } else { 0 };
            Ok(parts.get(idx).map(|s| s.to_string()).unwrap_or_default())
        },
    )?;

    // OBJECT_CONSTRUCT(k1, v1, k2, v2, ...)  — variadic version
    conn.create_scalar_function("object_construct", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let mut map = serde_json::Map::new();
        let n = ctx.len();
        let mut i = 0;
        while i + 1 < n {
            let key: String = ctx.get(i)?;
            let val: rusqlite::types::Value = ctx.get(i + 1)?;
            let json_val = sqlite_value_ref_to_json(val);
            map.insert(key, json_val);
            i += 2;
        }
        Ok(serde_json::Value::Object(map).to_string())
    })?;

    // ARRAY_CONSTRUCT(v1, v2, ...) — variadic
    conn.create_scalar_function("array_construct", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let mut arr = Vec::new();
        for i in 0..ctx.len() {
            let val: rusqlite::types::Value = ctx.get(i)?;
            arr.push(sqlite_value_ref_to_json(val));
        }
        Ok(serde_json::Value::Array(arr).to_string())
    })?;

    // GET_PATH(variant_col, path_string)
    conn.create_scalar_function(
        "get_path",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path: String = ctx.get(1)?;
            let json: serde_json::Value =
                serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
            let result = path.split('.').fold(&json, |acc, key| {
                acc.get(key).unwrap_or(&serde_json::Value::Null)
            });
            Ok(result.to_string())
        },
    )?;

    // AS_OBJECT / AS_ARRAY / AS_VARCHAR — passthrough for VARIANT casts
    conn.create_scalar_function(
        "as_object",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| ctx.get::<String>(0),
    )?;
    conn.create_scalar_function(
        "as_array",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| ctx.get::<String>(0),
    )?;
    conn.create_scalar_function(
        "as_varchar",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| ctx.get::<String>(0),
    )?;

    // TRY_PARSE_JSON — same as passthrough for local testing
    conn.create_scalar_function(
        "try_parse_json",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| ctx.get::<Option<String>>(0),
    )?;

    Ok(())
}

fn sqlite_value_ref_to_json(val: rusqlite::types::Value) -> serde_json::Value {
    match val {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
        rusqlite::types::Value::Real(r) => serde_json::json!(r),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => {
            serde_json::Value::String(format!("<{} bytes>", b.len()))
        }
    }
}

fn sqlite_value_to_value(val: ValueRef<'_>) -> Value {
    match val {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(b) => Value::Text(String::from_utf8_lossy(b).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}
