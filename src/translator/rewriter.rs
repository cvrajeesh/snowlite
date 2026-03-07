//! The top-level [`Translator`] struct that orchestrates all rewriting passes.

use crate::Result;

use super::functions;
use super::identifiers;
use super::noop;
use super::types;

/// Configuration for the SQL translator.
#[derive(Debug, Clone, Default)]
pub struct TranslatorConfig {
    /// When `true`, two-part identifiers `schema.table` become `schema__table`
    /// instead of just `table`.  Useful when tests use multiple schemas.
    pub use_schema_prefix: bool,

    /// When `true`, `CREATE OR REPLACE TABLE` is translated to
    /// `DROP TABLE IF EXISTS …; CREATE TABLE …` (two statements).
    /// When `false` (default), it becomes `CREATE TABLE IF NOT EXISTS …`.
    pub drop_before_create: bool,
}


/// Translates Snowflake SQL to SQLite SQL.
///
/// Create one `Translator` per test suite (or even per test) and reuse it.
/// The translator is cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct Translator {
    config: TranslatorConfig,
}

impl Translator {
    /// Create a new translator with default configuration.
    pub fn new() -> Self {
        Translator::default()
    }

    /// Create a translator with custom configuration.
    pub fn with_config(config: TranslatorConfig) -> Self {
        Translator { config }
    }

    /// Translate a single Snowflake SQL statement.
    ///
    /// Returns `Ok(None)` if the statement is a known no-op.
    /// Returns `Ok(Some(sqlite_sql))` on success.
    pub fn translate(&self, sql: &str) -> Result<Option<String>> {
        let trimmed = sql.trim();
        if noop::is_noop(trimmed) {
            log::debug!("Dropping no-op statement: {}", &trimmed[..trimmed.len().min(80)]);
            return Ok(None);
        }

        let mut out = trimmed.to_owned();

        // 1. Rewrite CREATE OR REPLACE — do before type rewriting so we don't
        //    accidentally alter the table name.
        if self.config.drop_before_create {
            out = self.rewrite_create_or_replace_with_drop(&out);
        } else {
            out = functions::rewrite_create_or_replace(&out);
        }

        // 2. Strip fully-qualified identifiers
        out = identifiers::strip_qualifiers(&out, self.config.use_schema_prefix);

        // 3. Rewrite Snowflake type names to SQLite affinities (DDL only)
        out = types::rewrite_types(&out);

        // 4. Rewrite Snowflake functions / operators
        out = functions::rewrite_functions(&out);

        // 5. Strip Snowflake-specific column options that SQLite doesn't support
        out = self.strip_snowflake_options(&out);

        log::trace!("Translated:\n  IN:  {trimmed}\n  OUT: {out}");
        Ok(Some(out))
    }

    /// Translate a batch of semicolon-separated Snowflake SQL statements.
    ///
    /// No-op statements are silently dropped from the output.
    pub fn translate_batch(&self, sql: &str) -> Result<Vec<String>> {
        split_statements(sql)
            .into_iter()
            .filter_map(|stmt| {
                let stmt = stmt.trim();
                if stmt.is_empty() {
                    return None;
                }
                match self.translate(stmt) {
                    Ok(None) => None,
                    Ok(Some(s)) => Some(Ok(s)),
                    Err(e) => Some(Err(e)),
                }
            })
            .collect()
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    fn rewrite_create_or_replace_with_drop(&self, sql: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)\bCREATE\s+OR\s+REPLACE\s+TABLE\s+([^\s(]+)")
                .expect("valid CREATE OR REPLACE regex")
        });

        if let Some(caps) = RE.captures(sql) {
            let table_name = &caps[1];
            let drop = format!("DROP TABLE IF EXISTS {table_name}");
            let create = RE
                .replace(sql, format!("CREATE TABLE {table_name}"))
                .into_owned();
            format!("{drop};\n{create}")
        } else {
            sql.to_owned()
        }
    }

    /// Remove Snowflake-specific column/table options that SQLite rejects.
    fn strip_snowflake_options(&self, sql: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static PATTERNS: Lazy<Vec<(Regex, &'static str)>> = Lazy::new(|| {
            vec![
                // AUTOINCREMENT keyword (Snowflake uses it differently from SQLite)
                (
                    Regex::new(r"(?i)\bAUTOINCREMENT\b").unwrap(),
                    "",
                ),
                // Snowflake DEFAULT sequences: DEFAULT <sequence>.NEXTVAL
                (
                    Regex::new(r"(?i)\bDEFAULT\s+\S+\.NEXTVAL\b").unwrap(),
                    "",
                ),
                // COMMENT = '...'
                (
                    Regex::new(r"(?i)\bCOMMENT\s*=\s*'[^']*'").unwrap(),
                    "",
                ),
                // CLUSTER BY (...)
                (
                    Regex::new(r"(?i)\bCLUSTER\s+BY\s*\([^)]*\)").unwrap(),
                    "",
                ),
                // ENABLE_SCHEMA_EVOLUTION / DATA_RETENTION_TIME_IN_DAYS / etc.
                (
                    Regex::new(r"(?i)\b(?:ENABLE_SCHEMA_EVOLUTION|DATA_RETENTION_TIME_IN_DAYS|CHANGE_TRACKING|COPY\s+GRANTS|WITH\s+MASKING\s+POLICY)\s*=\s*\S+").unwrap(),
                    "",
                ),
                // COLLATE 'utf8'  (SQLite has limited collation support)
                (
                    Regex::new(r"(?i)\bCOLLATE\s+'\S+'").unwrap(),
                    "",
                ),
                // ON DELETE / ON UPDATE actions that SQLite doesn't support
                // (SQLite does support basic FK actions but let's keep it safe)
                // Actually SQLite supports ON DELETE CASCADE etc., so skip this.
            ]
        });

        let mut result = sql.to_owned();
        for (re, replacement) in PATTERNS.iter() {
            result = re.replace_all(&result, *replacement).into_owned();
        }
        result
    }
}

/// Split a SQL string on `;` boundaries, respecting string literals.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut start = 0;
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        match bytes[i] {
            b'-' if !in_single && !in_double && !in_block_comment && i + 1 < len && bytes[i + 1] == b'-' => {
                in_line_comment = true;
                i += 2;
                continue;
            }
            b'\n' if in_line_comment => {
                in_line_comment = false;
            }
            b'/' if !in_single && !in_double && !in_line_comment && i + 1 < len && bytes[i + 1] == b'*' => {
                in_block_comment = true;
                i += 2;
                continue;
            }
            b'*' if in_block_comment && i + 1 < len && bytes[i + 1] == b'/' => {
                in_block_comment = false;
                i += 2;
                continue;
            }
            b'\'' if !in_double && !in_line_comment && !in_block_comment => {
                in_single = !in_single;
            }
            b'"' if !in_single && !in_line_comment && !in_block_comment => {
                in_double = !in_double;
            }
            b';' if !in_single && !in_double && !in_line_comment && !in_block_comment => {
                let stmt = &sql[start..i];
                if !stmt.trim().is_empty() {
                    statements.push(stmt);
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    let tail = &sql[start..];
    if !tail.trim().is_empty() {
        statements.push(tail);
    }
    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    fn translator() -> Translator {
        Translator::new()
    }

    #[test]
    fn noop_use_database() {
        assert!(translator().translate("USE DATABASE mydb").unwrap().is_none());
    }

    #[test]
    fn create_or_replace_table() {
        let sql = "CREATE OR REPLACE TABLE orders (id INTEGER, name TEXT)";
        let out = translator().translate(sql).unwrap().unwrap();
        assert!(out.contains("CREATE TABLE IF NOT EXISTS orders"));
    }

    #[test]
    fn full_qualified_identifier() {
        let sql = "SELECT * FROM mydb.public.orders";
        let out = translator().translate(sql).unwrap().unwrap();
        assert_eq!(out.trim(), "SELECT * FROM orders");
    }

    #[test]
    fn variant_type() {
        let sql = "CREATE TABLE t (data VARIANT, id NUMBER(18,0))";
        let out = translator().translate(sql).unwrap().unwrap();
        assert!(out.contains("TEXT"), "got: {out}");
        assert!(out.contains("INTEGER"), "got: {out}");
    }

    #[test]
    fn split_statements_basic() {
        let stmts = split_statements("SELECT 1; SELECT 2; SELECT 3");
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn split_statements_with_semicolons_in_strings() {
        let stmts = split_statements("INSERT INTO t VALUES ('a;b'); SELECT 1");
        assert_eq!(stmts.len(), 2);
    }
}
