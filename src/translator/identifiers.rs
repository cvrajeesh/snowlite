//! Strips Snowflake fully-qualified identifiers (`db.schema.table`) down to
//! just the table name (or optionally `schema__table` to avoid collisions).
//!
//! Snowflake identifiers can be quoted with double-quotes and are
//! case-insensitive.  SQLite identifiers follow the same quoting rules, so
//! we simply remove the leading components.

use once_cell::sync::Lazy;
use regex::Regex;

/// Rewrite fully-qualified three-part identifiers `db.schema.table` or
/// two-part identifiers `schema.table` to just `table`.
///
/// When `use_schema_prefix` is `true`, two-part identifiers become
/// `schema__table` so that same-named tables in different schemas don't
/// collide.
pub fn strip_qualifiers(sql: &str, use_schema_prefix: bool) -> String {
    // Match: optional "db". optional "schema". table
    // Each part may be quoted with double-quotes.
    static RE_THREE_PART: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r#"(?i)"?[A-Za-z_][A-Za-z0-9_$]*"?\s*\.\s*"?[A-Za-z_][A-Za-z0-9_$]*"?\s*\.\s*("?[A-Za-z_][A-Za-z0-9_$]*"?)"#,
        )
        .expect("valid three-part identifier regex")
    });
    static RE_TWO_PART: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r#"(?i)("?[A-Za-z_][A-Za-z0-9_$]*"?)\s*\.\s*("?[A-Za-z_][A-Za-z0-9_$]*"?)"#,
        )
        .expect("valid two-part identifier regex")
    });

    // Strip three-part qualifiers first
    let sql = RE_THREE_PART.replace_all(sql, "$1").into_owned();

    if use_schema_prefix {
        // Keep schema, join with double-underscore
        RE_TWO_PART
            .replace_all(&sql, |caps: &regex::Captures| {
                let schema = caps[1].trim_matches('"');
                let table = caps[2].trim_matches('"');
                format!("{schema}__{table}")
            })
            .into_owned()
    } else {
        // Drop schema entirely
        RE_TWO_PART.replace_all(&sql, "$2").into_owned()
    }
}
