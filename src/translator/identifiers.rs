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
///
/// String literals (single-quoted) and double-quoted identifiers are preserved
/// verbatim so that paths like `'a.b'` inside function arguments are not
/// corrupted.
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

    // Apply regex substitutions only to segments of SQL that are outside
    // single-quoted string literals, preventing dotted paths like 'a.b' inside
    // function call arguments from being incorrectly stripped.
    apply_outside_literals(sql, |segment| {
        // Strip three-part qualifiers first
        let s = RE_THREE_PART.replace_all(segment, "$1").into_owned();

        if use_schema_prefix {
            RE_TWO_PART
                .replace_all(&s, |caps: &regex::Captures| {
                    let schema = caps[1].trim_matches('"');
                    let table = caps[2].trim_matches('"');
                    let sanitize = |s: &str| -> String {
                        s.chars()
                            .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
                            .collect()
                    };
                    format!("{}__{}", sanitize(schema), sanitize(table))
                })
                .into_owned()
        } else {
            RE_TWO_PART.replace_all(&s, "$2").into_owned()
        }
    })
}

/// Apply a transformation function to all segments of `sql` that are **outside**
/// single-quoted string literals.  Single-quoted segments are copied through
/// unchanged.  Double-quoted identifiers are treated as non-literal SQL and
/// ARE passed through the transform.
fn apply_outside_literals(sql: &str, mut transform: impl FnMut(&str) -> String) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    let mut segment_start = 0;

    while let Some((i, ch)) = chars.next() {
        if ch == '\'' {
            // Flush the non-literal segment accumulated so far
            if i > segment_start {
                result.push_str(&transform(&sql[segment_start..i]));
            }
            // Copy the single-quoted literal verbatim (handle escaped '' too)
            result.push('\'');
            let mut literal_end = i + 1;
            loop {
                match chars.next() {
                    None => break,
                    Some((j, '\'')) => {
                        result.push('\'');
                        literal_end = j + 1;
                        // Check for escaped '' (two consecutive single quotes)
                        if chars.peek().map(|(_, c)| *c) == Some('\'') {
                            chars.next();
                            result.push('\'');
                            literal_end += 1;
                        } else {
                            break;
                        }
                    }
                    Some((_, c)) => {
                        result.push(c);
                        literal_end += c.len_utf8();
                    }
                }
            }
            segment_start = literal_end;
        }
    }

    // Flush any remaining non-literal segment
    if segment_start < sql.len() {
        result.push_str(&transform(&sql[segment_start..]));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_part() {
        assert_eq!(
            strip_qualifiers("SELECT * FROM mydb.public.orders", false),
            "SELECT * FROM orders"
        );
    }

    #[test]
    fn two_part_no_prefix() {
        assert_eq!(
            strip_qualifiers("SELECT * FROM public.orders", false),
            "SELECT * FROM orders"
        );
    }

    #[test]
    fn two_part_with_prefix() {
        assert_eq!(
            strip_qualifiers("SELECT * FROM public.orders", true),
            "SELECT * FROM public__orders"
        );
    }

    #[test]
    fn quoted_identifiers() {
        assert_eq!(
            strip_qualifiers(r#"SELECT * FROM "MY_DB"."PUBLIC"."ORDERS""#, false),
            r#"SELECT * FROM "ORDERS""#
        );
    }
}
