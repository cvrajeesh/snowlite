//! Translates Snowflake data-type names to SQLite type affinities.
//!
//! SQLite uses a flexible type affinity system. We map Snowflake types as follows:
//!
//! | Snowflake type | SQLite affinity | Notes |
//! |---|---|---|
//! | NUMBER / DECIMAL / NUMERIC | INTEGER or REAL | INTEGER when scale=0 or unspecified |
//! | INT / BIGINT / SMALLINT | INTEGER | |
//! | FLOAT / FLOAT4 / FLOAT8 / DOUBLE | REAL | |
//! | REAL | REAL | |
//! | VARCHAR / CHAR / STRING / TEXT | TEXT | |
//! | BOOLEAN | INTEGER | 0/1 |
//! | DATE | TEXT | ISO-8601 YYYY-MM-DD |
//! | TIME | TEXT | HH:MM:SS |
//! | TIMESTAMP_NTZ / _LTZ / _TZ / TIMESTAMP | TEXT | ISO-8601 |
//! | VARIANT / OBJECT / ARRAY | TEXT | JSON stored as text |
//! | BINARY / VARBINARY / BYTES | BLOB | |

use once_cell::sync::Lazy;
use regex::Regex;

/// Rewrite all Snowflake type annotations in a DDL statement to SQLite affinities.
pub fn rewrite_types(sql: &str) -> String {
    let sql = rewrite_number_type(sql);
    rewrite_simple_types(&sql)
}

/// Rewrite `NUMBER(p,s)`, `DECIMAL(p,s)`, `NUMERIC(p,s)`.
fn rewrite_number_type(sql: &str) -> String {
    static RE_NUMBER_WITH_SCALE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b(NUMBER|DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*([1-9]\d*)\s*\)")
            .expect("valid regex")
    });
    static RE_NUMBER_NO_SCALE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b(NUMBER|DECIMAL|NUMERIC)\s*(?:\(\s*\d+\s*(?:,\s*0\s*)?\))?")
            .expect("valid regex")
    });

    // First replace NUMBER(p, s>0) → REAL
    let sql = RE_NUMBER_WITH_SCALE
        .replace_all(sql, "REAL")
        .into_owned();
    // Then replace NUMBER(p) / NUMBER(p,0) / NUMBER → INTEGER
    RE_NUMBER_NO_SCALE
        .replace_all(&sql, "INTEGER")
        .into_owned()
}

/// Rewrite non-parameterised and simply-parameterised types.
fn rewrite_simple_types(sql: &str) -> String {
    // Order matters: longer / more specific patterns first.
    let replacements: &[(&str, &str)] = &[
        // Timestamps — Snowflake has three variants
        (r"(?i)\bTIMESTAMP_NTZ\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bTIMESTAMP_LTZ\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bTIMESTAMP_TZ\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        // \b after TIMESTAMP prevents matching TIMESTAMP_FROM_PARTS function name
        (r"(?i)\bTIMESTAMP\b\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        // Datetime (MySQL-style, occasionally used)
        (r"(?i)\bDATETIME\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        // Date / Time — \b after TIME prevents matching TIME_FROM_PARTS function name
        (r"(?i)\bDATE\b", "TEXT"),
        (r"(?i)\bTIME\b\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        // Semi-structured
        (r"(?i)\bVARIANT\b", "TEXT"),
        (r"(?i)\bOBJECT\b", "TEXT"),
        (r"(?i)\bARRAY\b", "TEXT"),
        // String types
        (r"(?i)\bVARCHAR\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bNVARCHAR\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bCHAR\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bNCHAR\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        (r"(?i)\bSTRING\s*(?:\(\s*\d+\s*\))?", "TEXT"),
        // Boolean
        (r"(?i)\bBOOLEAN\b", "INTEGER"),
        // Float
        (r"(?i)\bFLOAT4\b", "REAL"),
        (r"(?i)\bFLOAT8\b", "REAL"),
        (r"(?i)\bDOUBLE\s+PRECISION\b", "REAL"),
        (r"(?i)\bDOUBLE\b", "REAL"),
        (r"(?i)\bFLOAT\b", "REAL"),
        // Integer aliases
        (r"(?i)\bBIGINT\b", "INTEGER"),
        (r"(?i)\bSMALLINT\b", "INTEGER"),
        (r"(?i)\bTINYINT\b", "INTEGER"),
        (r"(?i)\bBYTEINT\b", "INTEGER"),
        // Binary
        (r"(?i)\bVARBINARY\s*(?:\(\s*\d+\s*\))?", "BLOB"),
        (r"(?i)\bBINARY\s*(?:\(\s*\d+\s*\))?", "BLOB"),
        (r"(?i)\bBYTES\s*(?:\(\s*\d+\s*\))?", "BLOB"),
    ];

    let mut result = sql.to_owned();
    for (pattern, replacement) in replacements {
        let re = Regex::new(pattern).expect("valid type regex");
        result = re.replace_all(&result, *replacement).into_owned();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_integer() {
        assert_eq!(rewrite_types("id NUMBER(18,0)"), "id INTEGER");
        assert_eq!(rewrite_types("id NUMBER"), "id INTEGER");
        assert_eq!(rewrite_types("id NUMBER(10)"), "id INTEGER");
    }

    #[test]
    fn number_real() {
        assert_eq!(rewrite_types("price NUMBER(10,2)"), "price REAL");
        assert_eq!(rewrite_types("rate DECIMAL(8,4)"), "rate REAL");
    }

    #[test]
    fn varchar() {
        assert_eq!(rewrite_types("name VARCHAR(255)"), "name TEXT");
        assert_eq!(rewrite_types("code CHAR(3)"), "code TEXT");
    }

    #[test]
    fn timestamps() {
        assert_eq!(rewrite_types("ts TIMESTAMP_NTZ"), "ts TEXT");
        assert_eq!(rewrite_types("ts TIMESTAMP_NTZ(9)"), "ts TEXT");
        assert_eq!(rewrite_types("ts TIMESTAMP_LTZ"), "ts TEXT");
        assert_eq!(rewrite_types("ts TIMESTAMP_TZ(6)"), "ts TEXT");
    }

    #[test]
    fn variant() {
        assert_eq!(rewrite_types("data VARIANT"), "data TEXT");
        assert_eq!(rewrite_types("tags ARRAY"), "tags TEXT");
        assert_eq!(rewrite_types("obj OBJECT"), "obj TEXT");
    }

    #[test]
    fn boolean() {
        assert_eq!(rewrite_types("active BOOLEAN"), "active INTEGER");
    }
}
