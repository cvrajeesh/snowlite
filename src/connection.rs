//! Database connection — the primary public API surface.
//!
//! [`Connection`] wraps a `rusqlite::Connection` and intercepts every SQL
//! statement, passing it through the [`Translator`] before execution.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::Regex;
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
    /// When `true`, `PUT FILE` tracks the local file path so that a subsequent
    /// `COPY INTO table FROM @stage` can actually read the CSV file and insert
    /// its rows.  Disabled by default so that existing no-op tests are unaffected.
    pub load_staged_files: bool,
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

    /// Enable real CSV file loading for staged files.
    ///
    /// When active, `PUT FILE:///path @stage` records the local path, and a
    /// subsequent `COPY INTO table FROM @stage FILE_FORMAT=(TYPE='CSV')` reads
    /// the file and inserts its rows into the table via SQLite.
    ///
    /// Without this option (the default) both commands remain silent no-ops,
    /// which is what most existing tests expect.
    pub fn with_stage_loading(mut self) -> Self {
        self.load_staged_files = true;
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
    /// Whether real CSV file staging is enabled (see [`Config::with_stage_loading`]).
    stage_loading: bool,
    /// Tracks files staged via `PUT FILE` when [`stage_loading`] is `true`.
    /// Maps normalised stage name → ordered list of local [`PathBuf`]s.
    staged_files: RefCell<HashMap<String, Vec<PathBuf>>>,
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
    /// # use snowlite::Connection;
    /// # fn main() -> snowlite::Result<()> {
    /// let conn = Connection::open_in_memory()?;
    /// conn.execute("CREATE TABLE t (id INTEGER)", &[])?;
    /// conn.execute("INSERT INTO t VALUES (?)", &[&1i64])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> Result<usize> {
        // When stage loading is enabled, intercept PUT FILE and COPY INTO before
        // the regular translator so that staged CSV files are actually loaded.
        if self.stage_loading {
            if let Some(result) = self.try_stage_put_file(sql) {
                return result;
            }
            if let Some(result) = self.try_copy_into_from_stage(sql) {
                return result;
            }
        }
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
    /// # use snowlite::{Connection, Row};
    /// # fn main() -> snowlite::Result<()> {
    /// let conn = Connection::open_in_memory()?;
    /// # conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])?;
    /// let rows = conn.query("SELECT id, name FROM t WHERE id > ?", &[&0i64])?;
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<Vec<Row>> {
        let translated = match self.translator.translate(sql)? {
            None => return Ok(vec![]),
            Some(t) => t,
        };

        let mut stmt = self.inner.prepare(&translated)?;
        let column_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

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
            stage_loading: config.load_staged_files,
            staged_files: RefCell::new(HashMap::new()),
        })
    }

    // ── Stage file loading ───────────────────────────────────────────────────

    /// If `sql` is a `PUT FILE` statement, record the local path under the
    /// stage name and return `Ok(0)`.  Returns `None` if the statement does not
    /// match the `PUT FILE` pattern.
    fn try_stage_put_file(&self, sql: &str) -> Option<Result<usize>> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            // PUT FILE:///local/path @stage_name [options]
            Regex::new(r"(?i)^\s*PUT\s+FILE://([^\s]+)\s+@([\w./]+)").unwrap()
        });
        let caps = RE.captures(sql.trim())?;
        let file_path = PathBuf::from(caps.get(1)?.as_str());
        // Only track files that actually exist so that fake paths in existing
        // no-op tests do not accidentally get staged.
        if !file_path.exists() {
            return Some(Ok(0));
        }
        let stage_name = normalize_stage_name(caps.get(2)?.as_str());
        self.staged_files.borrow_mut().entry(stage_name).or_default().push(file_path);
        Some(Ok(0))
    }

    /// If `sql` is an inbound `COPY INTO table FROM @stage` statement **and**
    /// that stage has files tracked by a prior `PUT FILE`, load those CSV files
    /// into the table and return the number of rows inserted.
    ///
    /// Returns `None` if the pattern does not match or no staged files exist,
    /// allowing the call to fall through to the regular no-op translator.
    fn try_copy_into_from_stage(&self, sql: &str) -> Option<Result<usize>> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            // Inbound: COPY INTO table FROM @stage [options]
            // Does NOT match outbound COPY INTO @stage FROM table because the
            // first captured group requires word characters (no leading @).
            Regex::new(r"(?i)^\s*COPY\s+INTO\s+([\w.]+)\s+FROM\s+@([\w./]+)").unwrap()
        });
        let caps = RE.captures(sql.trim())?;
        let table_raw = caps.get(1)?.as_str();
        let stage_ref = caps.get(2)?.as_str();

        // Strip schema prefix from table name — use the last dotted component.
        let table = table_raw.split('.').next_back().unwrap_or(table_raw);
        let stage_name = normalize_stage_name(stage_ref);

        // Only proceed when this stage actually has tracked files.
        let files = {
            let map = self.staged_files.borrow();
            map.get(&stage_name).cloned()?
        };

        // Skip non-CSV formats so that staged files with e.g. TYPE='JSON' remain
        // no-ops and do not cause unexpected row insertions.
        if !is_csv_format(sql) {
            return Some(Ok(0));
        }

        let skip_header = parse_skip_header(sql);
        let mut total = 0usize;
        for path in &files {
            match load_csv_into_table(&self.inner, table, path, skip_header) {
                Ok(n) => total += n,
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(total))
    }
}

// ── Stage file loading helpers ───────────────────────────────────────────────

/// Normalise a stage reference to a lookup key: strip any path suffix after
/// the first `/` and convert to lower-case.
///
/// Examples:
/// - `"my_stage"` → `"my_stage"`
/// - `"My_Stage/path/prefix"` → `"my_stage"`
/// - `"mydb.public.my_stage"` → `"mydb.public.my_stage"`
fn normalize_stage_name(stage_ref: &str) -> String {
    stage_ref.split('/').next().unwrap_or(stage_ref).to_lowercase()
}

/// Load rows from a CSV `file` into `table`.
///
/// `skip_header` specifies how many leading lines to discard (usually 0 or 1).
/// All field values are passed as `TEXT`; SQLite's type-affinity rules coerce
/// them to `INTEGER` / `REAL` as needed.  Non-existent files are silently
/// skipped (returns `Ok(0)`) so that fake paths in no-op tests are harmless.
fn load_csv_into_table(
    conn: &rusqlite::Connection,
    table: &str,
    file: &Path,
    skip_header: usize,
) -> Result<usize> {
    use std::io::{BufRead, BufReader};

    if !file.exists() {
        return Ok(0);
    }

    let reader = BufReader::new(std::fs::File::open(file)?);
    let lines: Vec<String> = reader.lines().collect::<std::io::Result<_>>()?;

    let data: Vec<&str> = lines.iter().skip(skip_header).map(String::as_str).collect();
    if data.is_empty() {
        return Ok(0);
    }

    // Parse the first data row once to determine the expected column count,
    // then reuse the parsed values for the first INSERT.
    let first_row = parse_csv_row(data[0]);
    let col_count = first_row.len();
    if col_count == 0 {
        return Ok(0);
    }

    let placeholders = std::iter::repeat_n("?", col_count).collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {table} VALUES ({placeholders})");
    let mut stmt = conn.prepare(&insert_sql)?;

    let mut count = 0usize;
    for (i, line) in data.iter().enumerate() {
        // Reuse the already-parsed first row; parse remaining rows on demand.
        let values = if i == 0 { first_row.clone() } else { parse_csv_row(line) };
        if values.iter().all(|v| v.is_empty()) && line.trim().is_empty() {
            continue;
        }
        // Pad short rows with NULL; truncate over-long rows.
        let row: Vec<rusqlite::types::Value> = (0..col_count)
            .map(|i| match values.get(i) {
                Some(v) if !v.is_empty() => rusqlite::types::Value::Text(v.clone()),
                _ => rusqlite::types::Value::Null,
            })
            .collect();
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            row.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
        stmt.execute(refs.as_slice())?;
        count += 1;
    }
    Ok(count)
}

/// Parse a single CSV line into a `Vec<String>`, respecting RFC 4180 quoting:
/// fields may be enclosed in `"…"`, and a literal `"` inside quotes is
/// represented as `""`.
fn parse_csv_row(line: &str) -> Vec<String> {
    let line = line.trim_end_matches('\r');
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes => in_quotes = true,
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    chars.next(); // consume escaped double-quote
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                fields.push(std::mem::take(&mut field));
            }
            _ => field.push(c),
        }
    }
    fields.push(field);
    fields
}

/// Extract the `SKIP_HEADER = n` value from a `COPY INTO` SQL string.
/// Returns `0` if the option is absent.
fn parse_skip_header(sql: &str) -> usize {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)SKIP_HEADER\s*=\s*(\d+)").unwrap());
    RE.captures(sql)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0)
}

/// Returns `true` when the `COPY INTO` SQL either specifies `TYPE = 'CSV'`
/// explicitly, or omits the `TYPE` option entirely (default assumption: CSV).
/// Returns `false` for any other explicit format such as JSON, PARQUET, etc.
fn is_csv_format(sql: &str) -> bool {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)TYPE\s*=\s*'([^']+)'").unwrap());
    match RE.captures(sql).and_then(|c| c.get(1)) {
        None => true,
        Some(m) => m.as_str().eq_ignore_ascii_case("CSV"),
    }
}

// ── Custom SQLite function registrations ────────────────────────────────────

fn build_regex(pattern: &str) -> std::result::Result<regex::Regex, rusqlite::Error> {
    regex::RegexBuilder::new(pattern)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|_| {
            rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid or too complex regular expression",
            )))
        })
}

// ── MEDIAN aggregate ─────────────────────────────────────────────────────────

struct MedianAgg;

struct MedianAccum {
    values: Vec<f64>,
}

impl rusqlite::functions::Aggregate<MedianAccum, Option<f64>> for MedianAgg {
    fn init(&self, _ctx: &mut rusqlite::functions::Context<'_>) -> rusqlite::Result<MedianAccum> {
        Ok(MedianAccum { values: Vec::new() })
    }

    fn step(
        &self,
        ctx: &mut rusqlite::functions::Context<'_>,
        acc: &mut MedianAccum,
    ) -> rusqlite::Result<()> {
        let val: Option<f64> = ctx.get(0)?;
        if let Some(v) = val {
            acc.values.push(v);
        }
        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut rusqlite::functions::Context<'_>,
        acc: Option<MedianAccum>,
    ) -> rusqlite::Result<Option<f64>> {
        match acc {
            None => Ok(None),
            Some(a) if a.values.is_empty() => Ok(None),
            Some(mut a) => {
                a.values.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
                let n = a.values.len();
                let median = if n % 2 == 1 {
                    a.values[n / 2]
                } else {
                    (a.values[n / 2 - 1] + a.values[n / 2]) / 2.0
                };
                Ok(Some(median))
            }
        }
    }
}

fn register_custom_functions(conn: &rusqlite::Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;
    let det = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;

    // LOG(x) — natural logarithm; bundled SQLite does not enable SQLITE_ENABLE_MATH_FUNCTIONS.
    // Registered as "log" so that:
    //   LN(x) → LOG(x)                         uses this function (ln)
    //   LOG(base, x) → (LOG(x) / LOG(base))    change-of-base via ln is correct
    conn.create_scalar_function("log", 1, det, |ctx| {
        let x: f64 = ctx.get(0)?;
        if x <= 0.0 {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "LOG argument must be positive"),
            )));
        }
        Ok(x.ln())
    })?;

    // REGEXP support (used by RLIKE / REGEXP operator — SQLite calls regexp(pattern, text))
    conn.create_scalar_function("regexp", 2, det, |ctx| {
        let pattern: String = ctx.get(0)?;
        let text: String = ctx.get(1)?;
        Ok(build_regex(&pattern)?.is_match(&text))
    })?;

    // REGEXP_LIKE(text, pattern) — Snowflake function form (note: arg order differs from regexp())
    conn.create_scalar_function("regexp_like", 2, det, |ctx| {
        let text: String = ctx.get(0)?;
        let pattern: String = ctx.get(1)?;
        Ok(build_regex(&pattern)?.is_match(&text))
    })?;

    // REGEXP_REPLACE(text, pattern, replacement [, position [, occurrence]])
    conn.create_scalar_function("regexp_replace", -1, det, |ctx| {
        if ctx.len() < 3 {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "REGEXP_REPLACE requires at least 3 arguments"),
            )));
        }
        let text: String = ctx.get(0)?;
        let pattern: String = ctx.get(1)?;
        let replacement: String = ctx.get(2)?;
        let re = build_regex(&pattern)?;
        Ok(re.replace_all(&text, replacement.as_str()).into_owned())
    })?;

    // REGEXP_SUBSTR(text, pattern [, position [, occurrence]])
    conn.create_scalar_function("regexp_substr", -1, det, |ctx| {
        if ctx.len() < 2 {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "REGEXP_SUBSTR requires at least 2 arguments"),
            )));
        }
        let text: String = ctx.get(0)?;
        let pattern: String = ctx.get(1)?;
        let position: usize = if ctx.len() > 2 {
            (ctx.get::<i64>(2)? as usize).saturating_sub(1)
        } else {
            0
        };
        let occurrence: usize = if ctx.len() > 3 { ctx.get::<i64>(3)? as usize } else { 1 };
        let re = build_regex(&pattern)?;
        let search_in = &text[position.min(text.len())..];
        let mut count = 0usize;
        for m in re.find_iter(search_in) {
            count += 1;
            if count == occurrence {
                return Ok(Some(m.as_str().to_string()));
            }
        }
        Ok(None)
    })?;

    // LPAD(str, len [, pad_str])
    conn.create_scalar_function("lpad", -1, det, |ctx| {
        let s: String = ctx.get(0)?;
        let len: i64 = ctx.get(1)?;
        let pad: String = if ctx.len() > 2 { ctx.get(2)? } else { " ".to_string() };
        if pad.is_empty() {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "LPAD pad string cannot be empty"),
            )));
        }
        let len = len as usize;
        let s_chars: Vec<char> = s.chars().collect();
        if s_chars.len() >= len {
            return Ok(s_chars[..len].iter().collect::<String>());
        }
        let needed = len - s_chars.len();
        let padding: String = pad.chars().cycle().take(needed).collect();
        Ok(format!("{}{}", padding, s))
    })?;

    // RPAD(str, len [, pad_str])
    conn.create_scalar_function("rpad", -1, det, |ctx| {
        let s: String = ctx.get(0)?;
        let len: i64 = ctx.get(1)?;
        let pad: String = if ctx.len() > 2 { ctx.get(2)? } else { " ".to_string() };
        if pad.is_empty() {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "RPAD pad string cannot be empty"),
            )));
        }
        let len = len as usize;
        let s_chars: Vec<char> = s.chars().collect();
        if s_chars.len() >= len {
            return Ok(s_chars[..len].iter().collect::<String>());
        }
        let needed = len - s_chars.len();
        let padding: String = pad.chars().cycle().take(needed).collect();
        Ok(format!("{}{}", s, padding))
    })?;

    // INITCAP(str) — capitalise first letter of each whitespace-delimited word
    conn.create_scalar_function("initcap", 1, det, |ctx| {
        let s: String = ctx.get(0)?;
        let result = s
            .split_whitespace()
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => {
                        first.to_uppercase().to_string() + &chars.as_str().to_lowercase()
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        Ok(result)
    })?;

    // REPEAT(str, n)
    conn.create_scalar_function("repeat", 2, det, |ctx| {
        let s: String = ctx.get(0)?;
        let n: i64 = ctx.get(1)?;
        if n <= 0 {
            return Ok(String::new());
        }
        Ok(s.repeat(n as usize))
    })?;

    // REVERSE(str)
    conn.create_scalar_function("reverse", 1, det, |ctx| {
        let s: String = ctx.get(0)?;
        Ok(s.chars().rev().collect::<String>())
    })?;

    // CONCAT_WS(sep, s1, s2, ...) — join non-NULL args with separator
    conn.create_scalar_function("concat_ws", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        if ctx.is_empty() {
            return Ok(String::new());
        }
        let sep: String = ctx.get(0)?;
        let parts: Vec<String> = (1..ctx.len())
            .filter_map(|i| ctx.get::<Option<String>>(i).ok().flatten())
            .collect();
        Ok(parts.join(&sep))
    })?;

    // SPLIT_PART(string, delimiter, part_number)
    conn.create_scalar_function("split_part", 3, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let s: String = ctx.get(0)?;
        let delim: String = ctx.get(1)?;
        let n: i64 = ctx.get(2)?;
        if n <= 0 {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "SPLIT_PART part number must be positive",
                ),
            )));
        }
        let parts: Vec<&str> = s.split(delim.as_str()).collect();
        let idx = (n - 1) as usize;
        Ok(parts.get(idx).map(|s| s.to_string()).unwrap_or_default())
    })?;

    // STRTOK(string, delimiters, part_number)
    conn.create_scalar_function("strtok", 3, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let s: String = ctx.get(0)?;
        let delims: String = ctx.get(1)?;
        let n: i64 = ctx.get(2)?;
        if n <= 0 {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "STRTOK part number must be positive",
                ),
            )));
        }
        let delim_chars: Vec<char> = delims.chars().collect();
        let parts: Vec<&str> = s.split(|c| delim_chars.contains(&c)).filter(|p| !p.is_empty()).collect();
        let idx = (n - 1) as usize;
        Ok(parts.get(idx).map(|s| s.to_string()).unwrap_or_default())
    })?;

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
    conn.create_scalar_function("get_path", 2, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let json_str: String = ctx.get(0)?;
        let path: String = ctx.get(1)?;
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
        // Limit path depth to prevent excessive iteration on malicious input
        const MAX_PATH_DEPTH: usize = 64;
        let result = path
            .split('.')
            .take(MAX_PATH_DEPTH)
            .fold(&json, |acc, key| acc.get(key).unwrap_or(&serde_json::Value::Null));
        Ok(result.to_string())
    })?;

    // AS_OBJECT / AS_ARRAY / AS_VARCHAR — passthrough for VARIANT casts
    conn.create_scalar_function("as_object", 1, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        ctx.get::<String>(0)
    })?;
    conn.create_scalar_function("as_array", 1, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        ctx.get::<String>(0)
    })?;
    conn.create_scalar_function("as_varchar", 1, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        ctx.get::<String>(0)
    })?;

    // TRY_PARSE_JSON — same as passthrough for local testing
    conn.create_scalar_function("try_parse_json", 1, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        ctx.get::<Option<String>>(0)
    })?;

    // TO_DATE(str [, format]) — ignore the optional Snowflake format string; use SQLite DATE()
    conn.create_scalar_function("to_date", -1, det, |ctx| {
        if ctx.is_empty() {
            return Ok(None);
        }
        let s: Option<String> = ctx.get(0)?;
        match s {
            None => Ok(None),
            Some(v) => {
                // Ask SQLite to normalise the value; return it as a date string
                Ok(Some(v))
            }
        }
    })?;

    // TO_CHAR(val [, format]) — with format string, map common Snowflake tokens to strftime
    conn.create_scalar_function("to_char", -1, det, |ctx| {
        if ctx.is_empty() {
            return Ok(None);
        }
        let val: Option<String> = ctx.get(0)?;
        let val = match val {
            None => return Ok(None),
            Some(v) => v,
        };
        if ctx.len() == 1 {
            return Ok(Some(val));
        }
        let fmt: String = ctx.get(1)?;
        // Map Snowflake format tokens to strftime equivalents
        let strftime_fmt = fmt
            .replace("YYYY", "%Y")
            .replace("YY", "%y")
            .replace("MM", "%m")
            .replace("DD", "%d")
            .replace("HH24", "%H")
            .replace("HH", "%H")
            .replace("MI", "%M")
            .replace("SS", "%S");
        // Use SQLite's strftime via a best-effort Rust implementation
        // Parse the date/datetime value and apply the format
        let result = apply_strftime(&strftime_fmt, &val);
        Ok(Some(result))
    })?;

    // NEXT_DAY(date, dayname) — return the next occurrence of dayname after date
    conn.create_scalar_function("next_day", 2, det, |ctx| {
        let date: String = ctx.get(0)?;
        let dayname: String = ctx.get(1)?;
        let target_dow = match dayname.to_lowercase().as_str() {
            "sunday" | "sun"    => 0u32,
            "monday" | "mon"    => 1,
            "tuesday" | "tue"   => 2,
            "wednesday" | "wed" => 3,
            "thursday" | "thu"  => 4,
            "friday" | "fri"    => 5,
            "saturday" | "sat"  => 6,
            _ => return Err(rusqlite::Error::UserFunctionError(Box::new(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "unknown day name"),
            ))),
        };
        // Parse date in YYYY-MM-DD format
        let parts: Vec<&str> = date.trim().split('-').collect();
        if parts.len() < 3 {
            return Ok(None);
        }
        let (y, m, d) = (
            parts[0].parse::<i32>().unwrap_or(2000),
            parts[1].parse::<u32>().unwrap_or(1),
            parts[2][..2.min(parts[2].len())].parse::<u32>().unwrap_or(1),
        );
        // Compute day-of-week using Tomohiko Sakamoto's algorithm
        static T: [i32; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
        let year = if m < 3 { y - 1 } else { y };
        let dow = ((year + year/4 - year/100 + year/400 + T[(m as usize)-1] + d as i32) % 7) as u32;
        let days_ahead = ((target_dow + 7 - dow) % 7) as i32;
        let days_ahead = if days_ahead == 0 { 7 } else { days_ahead };
        // Add days_ahead to the date
        let result = add_days_to_date(y, m, d, days_ahead);
        Ok(Some(result))
    })?;

    // CONVERT_TIMEZONE — SQLite has no timezone support; return the timestamp unchanged
    conn.create_scalar_function("convert_timezone", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        // 2-arg form: (target_tz, timestamp) — return timestamp
        // 3-arg form: (source_tz, target_tz, timestamp) — return timestamp
        if ctx.len() >= 2 {
            let ts: String = ctx.get(ctx.len() - 1)?;
            Ok(ts)
        } else {
            ctx.get::<String>(0)
        }
    })?;

    // ARRAY_SLICE(arr, start, end) — return a sub-array [start..end) (0-indexed, Snowflake convention)
    conn.create_scalar_function("array_slice", 3, det, |ctx| {
        let arr_str: String = ctx.get(0)?;
        let start: i64 = ctx.get(1)?;
        let end: i64 = ctx.get(2)?;
        let arr: serde_json::Value = serde_json::from_str(&arr_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        if let serde_json::Value::Array(items) = arr {
            let len = items.len() as i64;
            let s = start.max(0).min(len) as usize;
            let e = end.max(0).min(len) as usize;
            let sliced = serde_json::Value::Array(items[s..e].to_vec());
            Ok(sliced.to_string())
        } else {
            Ok("[]".to_string())
        }
    })?;

    // ARRAY_APPEND(arr, val) — append val to JSON array
    conn.create_scalar_function("array_append", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let arr_str: String = ctx.get(0)?;
        let val: rusqlite::types::Value = ctx.get(1)?;
        let mut arr: serde_json::Value = serde_json::from_str(&arr_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        if let serde_json::Value::Array(ref mut items) = arr {
            items.push(sqlite_value_ref_to_json(val));
        }
        Ok(arr.to_string())
    })?;

    // ARRAY_CONCAT(arr1, arr2) — concatenate two JSON arrays
    conn.create_scalar_function("array_concat", 2, det, |ctx| {
        let arr1_str: String = ctx.get(0)?;
        let arr2_str: String = ctx.get(1)?;
        let arr1: serde_json::Value = serde_json::from_str(&arr1_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        let arr2: serde_json::Value = serde_json::from_str(&arr2_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        let mut combined = Vec::new();
        if let serde_json::Value::Array(a) = arr1 { combined.extend(a); }
        if let serde_json::Value::Array(b) = arr2 { combined.extend(b); }
        Ok(serde_json::Value::Array(combined).to_string())
    })?;

    // ARRAY_COMPACT(arr) — remove NULL elements from JSON array
    conn.create_scalar_function("array_compact", 1, det, |ctx| {
        let arr_str: String = ctx.get(0)?;
        let arr: serde_json::Value = serde_json::from_str(&arr_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        if let serde_json::Value::Array(items) = arr {
            let compacted: Vec<serde_json::Value> = items
                .into_iter()
                .filter(|v| !v.is_null())
                .collect();
            Ok(serde_json::Value::Array(compacted).to_string())
        } else {
            Ok("[]".to_string())
        }
    })?;

    // ARRAY_UNIQUE(arr) — remove duplicate elements from JSON array (preserves first occurrence)
    conn.create_scalar_function("array_unique", 1, det, |ctx| {
        let arr_str: String = ctx.get(0)?;
        let arr: serde_json::Value = serde_json::from_str(&arr_str)
            .unwrap_or(serde_json::Value::Array(vec![]));
        if let serde_json::Value::Array(items) = arr {
            let mut seen = std::collections::HashSet::new();
            let unique: Vec<serde_json::Value> = items
                .into_iter()
                .filter(|v| seen.insert(v.to_string()))
                .collect();
            Ok(serde_json::Value::Array(unique).to_string())
        } else {
            Ok("[]".to_string())
        }
    })?;

    // SNOWFLAKE_TYPEOF(variant) — return Snowflake-style type name for JSON values
    // Registered as snowflake_typeof to avoid clashing with SQLite's built-in typeof()
    conn.create_scalar_function("snowflake_typeof", 1, det, |ctx| {
        let val: Option<String> = ctx.get(0)?;
        match val {
            None => Ok("NULL".to_string()),
            Some(s) => {
                let type_name = match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(serde_json::Value::Array(_))  => "ARRAY",
                    Ok(serde_json::Value::Object(_)) => "OBJECT",
                    Ok(serde_json::Value::String(_)) => "TEXT",
                    Ok(serde_json::Value::Number(_)) => "INTEGER",
                    Ok(serde_json::Value::Bool(_))   => "BOOLEAN",
                    Ok(serde_json::Value::Null)      => "NULL",
                    Err(_) => "TEXT",
                };
                Ok(type_name.to_string())
            }
        }
    })?;

    // OBJECT_KEYS(obj) — return a JSON array of the object's top-level keys
    conn.create_scalar_function("object_keys", 1, det, |ctx| {
        let obj_str: String = ctx.get(0)?;
        let obj: serde_json::Value = serde_json::from_str(&obj_str)
            .unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(map) = obj {
            let keys: Vec<serde_json::Value> = map.keys()
                .map(|k| serde_json::Value::String(k.clone()))
                .collect();
            Ok(serde_json::Value::Array(keys).to_string())
        } else {
            Ok("[]".to_string())
        }
    })?;

    // STRIP_NULL_VALUE(obj) — remove keys with null values from a JSON object
    conn.create_scalar_function("strip_null_value", 1, det, |ctx| {
        let obj_str: String = ctx.get(0)?;
        let obj: serde_json::Value = serde_json::from_str(&obj_str)
            .unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(map) = obj {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .collect();
            Ok(serde_json::Value::Object(filtered).to_string())
        } else {
            Ok(obj_str)
        }
    })?;

    // MEDIAN(expr) — custom aggregate; returns the median of non-NULL values
    conn.create_aggregate_function(
        "median",
        1,
        FunctionFlags::SQLITE_UTF8,
        MedianAgg,
    )?;

    // WIDTH_BUCKET(val, min, max, num_buckets) — equiwidth histogram bucket assignment
    // Returns 0 if val < min, num_buckets+1 if val >= max, else bucket 1..num_buckets
    conn.create_scalar_function("width_bucket", 4, det, |ctx| {
        let val: f64 = ctx.get(0)?;
        let min: f64 = ctx.get(1)?;
        let max: f64 = ctx.get(2)?;
        let buckets: i64 = ctx.get(3)?;
        if val < min {
            return Ok(0i64);
        }
        if val >= max {
            return Ok(buckets + 1);
        }
        let bucket = ((val - min) / (max - min) * buckets as f64).floor() as i64 + 1;
        Ok(bucket)
    })?;

    Ok(())
}

/// Apply a strftime-style format string to a date/datetime string value.
///
/// Handles the common tokens used by Snowflake's TO_CHAR format strings,
/// after they have been mapped to strftime equivalents.
fn apply_strftime(fmt: &str, val: &str) -> String {
    // Parse the date portion (YYYY-MM-DD)
    let parts: Vec<&str> = val.trim().split('T').next()
        .unwrap_or(val)
        .split(' ')
        .next()
        .unwrap_or(val)
        .split('-')
        .collect();
    let year  = parts.first().copied().unwrap_or("0000");
    let month = parts.get(1).copied().unwrap_or("01");
    let day   = parts.get(2).and_then(|d| d.get(..2)).unwrap_or("01");

    // Parse the time portion (HH:MM:SS)
    let time_part = val.trim().split_once(' ').map(|x| x.1).unwrap_or("00:00:00");
    let tparts: Vec<&str> = time_part.split(':').collect();
    let hour   = tparts.first().copied().unwrap_or("00");
    let minute = tparts.get(1).copied().unwrap_or("00");
    let second = tparts.get(2).and_then(|s| s.get(..2)).unwrap_or("00");

    fmt.replace("%Y", year)
        .replace("%y", &year[year.len().saturating_sub(2)..])
        .replace("%m", month)
        .replace("%d", day)
        .replace("%H", hour)
        .replace("%M", minute)
        .replace("%S", second)
}

/// Add `days` to a date given as (year, month, day) and return "YYYY-MM-DD".
fn add_days_to_date(y: i32, m: u32, d: u32, days: i32) -> String {
    // Convert to Julian day number, add, convert back
    let jdn = date_to_jdn(y, m, d) + days;
    let (ny, nm, nd) = jdn_to_date(jdn);
    format!("{:04}-{:02}-{:02}", ny, nm, nd)
}

fn date_to_jdn(y: i32, m: u32, d: u32) -> i32 {
    let a = (14 - m as i32) / 12;
    let yr = y + 4800 - a;
    let mo = m as i32 + 12 * a - 3;
    d as i32 + (153 * mo + 2) / 5 + 365 * yr + yr / 4 - yr / 100 + yr / 400 - 32045
}

fn jdn_to_date(jdn: i32) -> (i32, u32, u32) {
    let a = jdn + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - 146097 * b / 4;
    let d_val = (4 * c + 3) / 1461;
    let e = c - 1461 * d_val / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = 100 * b + d_val - 4800 + m / 10;
    (year, month as u32, day as u32)
}

fn sqlite_value_ref_to_json(val: rusqlite::types::Value) -> serde_json::Value {
    match val {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
        rusqlite::types::Value::Real(r) => serde_json::json!(r),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => serde_json::Value::String(format!("<{} bytes>", b.len())),
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
