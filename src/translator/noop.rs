//! Detection of Snowflake-specific statements that have no SQLite equivalent
//! and should be silently ignored during integration testing.

use once_cell::sync::Lazy;
use regex::Regex;

/// Patterns for statements that should be treated as no-ops.
static NOOP_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    let patterns = [
        // Session / context management
        r"(?i)^\s*USE\s+(DATABASE|SCHEMA|WAREHOUSE|ROLE)\b",
        r"(?i)^\s*ALTER\s+SESSION\b",
        r"(?i)^\s*ALTER\s+WAREHOUSE\b",
        r"(?i)^\s*ALTER\s+ACCOUNT\b",
        // Warehouse / compute management
        r"(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?WAREHOUSE\b",
        r"(?i)^\s*DROP\s+WAREHOUSE\b",
        r"(?i)^\s*SUSPEND\s+WAREHOUSE\b",
        r"(?i)^\s*RESUME\s+WAREHOUSE\b",
        // SHOW commands
        r"(?i)^\s*SHOW\s+(TABLES|SCHEMAS|DATABASES|WAREHOUSES|ROLES|GRANTS|COLUMNS|OBJECTS|VIEWS|PROCEDURES|FUNCTIONS|STAGES|PIPES|STREAMS|TASKS)\b",
        // COPY / stage operations
        r"(?i)^\s*COPY\s+INTO\b",
        r"(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?(STAGE|PIPE|STREAM|TASK)\b",
        r"(?i)^\s*DROP\s+(STAGE|PIPE|STREAM|TASK)\b",
        r"(?i)^\s*ALTER\s+(STAGE|PIPE|STREAM|TASK)\b",
        r"(?i)^\s*PUT\s+FILE\b",
        r"(?i)^\s*GET\s+@",
        r"(?i)^\s*REMOVE\s+@",
        // GRANT / REVOKE
        r"(?i)^\s*GRANT\b",
        r"(?i)^\s*REVOKE\b",
        // Role management
        r"(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?ROLE\b",
        r"(?i)^\s*DROP\s+ROLE\b",
        // Resource monitor
        r"(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?RESOURCE\s+MONITOR\b",
        // Comment (Snowflake COMMENT ON)
        r"(?i)^\s*COMMENT\s+ON\b",
        // SET / UNSET session variables
        r"(?i)^\s*SET\s+\w+\s*=",
        r"(?i)^\s*UNSET\s+\w+",
        // Database management (SQLite has only one database per connection)
        r"(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?DATABASE\b",
        r"(?i)^\s*DROP\s+DATABASE\b",
        // ANALYZE — Snowflake statistics command; no meaningful SQLite equivalent
        r"(?i)^\s*ANALYZE\s*$",
        // MERGE INTO — SQLite does not support MERGE (fundamental limitation)
        r"(?i)^\s*MERGE\s+INTO\b",
        // CALL with no meaningful local equivalent (can be overridden)
        // We intentionally do NOT add CALL here — stored procedures may be user-defined.
    ];
    patterns
        .iter()
        .map(|p| Regex::new(p).expect("valid noop regex"))
        .collect()
});

/// Returns `true` if `sql` should be silently ignored.
pub fn is_noop(sql: &str) -> bool {
    let trimmed = sql.trim();
    if trimmed.is_empty() || trimmed == ";" {
        return true;
    }
    NOOP_PATTERNS.iter().any(|re| re.is_match(trimmed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_use_database() {
        assert!(is_noop("USE DATABASE my_db"));
        assert!(is_noop("use schema public"));
        assert!(is_noop("  USE WAREHOUSE compute_wh  "));
    }

    #[test]
    fn noop_alter_session() {
        assert!(is_noop("ALTER SESSION SET QUERY_TAG = 'test'"));
    }

    #[test]
    fn noop_show() {
        assert!(is_noop("SHOW TABLES"));
        assert!(is_noop("SHOW SCHEMAS IN DATABASE mydb"));
    }

    #[test]
    fn not_noop_select() {
        assert!(!is_noop("SELECT 1"));
        assert!(!is_noop("CREATE TABLE foo (id INT)"));
    }
}
