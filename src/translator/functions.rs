//! Translates Snowflake scalar functions to their SQLite equivalents.
//!
//! All transformations are regex-based.  More complex, multi-argument
//! transformations (e.g. `DATEADD`, `DATEDIFF`) use capture groups.

use once_cell::sync::Lazy;
use regex::Regex;

/// A single translation rule: pattern → replacement.
struct Rule {
    re: Regex,
    replacement: &'static str,
}

impl Rule {
    fn new(pattern: &str, replacement: &'static str) -> Self {
        Rule {
            re: Regex::new(pattern).expect("valid function regex"),
            replacement,
        }
    }

    fn apply(&self, sql: &str) -> String {
        self.re.replace_all(sql, self.replacement).into_owned()
    }
}

/// Apply all function translation rules to the SQL string.
pub fn rewrite_functions(sql: &str) -> String {
    let sql = apply_simple_rules(sql);
    let sql = rewrite_iff(&sql);
    let sql = rewrite_decode(&sql);
    let sql = rewrite_nvl2(&sql);
    let sql = rewrite_dateadd(&sql);
    let sql = rewrite_datediff(&sql);
    let sql = rewrite_date_trunc(&sql);
    let sql = rewrite_listagg(&sql);
    let sql = rewrite_semi_structured_paths(&sql);
    let sql = rewrite_ilike(&sql);
    let sql = rewrite_create_or_replace(&sql);
    rewrite_top_n(&sql)
}

// ── Simple one-to-one function mappings ────────────────────────────────────

static SIMPLE_RULES: Lazy<Vec<Rule>> = Lazy::new(|| {
    vec![
        // NULL-handling
        Rule::new(r"(?i)\bNVL\s*\(", "COALESCE("),
        Rule::new(r"(?i)\bZEROIFNULL\s*\(([^)]+)\)", "COALESCE($1, 0)"),
        Rule::new(r"(?i)\bNULLIFZERO\s*\(([^)]+)\)", "NULLIF($1, 0)"),
        Rule::new(r"(?i)\bEMPTYTONULL\s*\(([^)]+)\)", "NULLIF($1, '')"),
        // Boolean logic
        Rule::new(r"(?i)\bBOOLOR\s*\(([^,]+),\s*([^)]+)\)", "($1 OR $2)"),
        Rule::new(r"(?i)\bBOOLAND\s*\(([^,]+),\s*([^)]+)\)", "($1 AND $2)"),
        Rule::new(
            r"(?i)\bBOOLXOR\s*\(([^,]+),\s*([^)]+)\)",
            "(($1 OR $2) AND NOT ($1 AND $2))",
        ),
        // Type conversions
        Rule::new(r"(?i)\bTO_VARCHAR\s*\(([^)]+)\)", "CAST($1 AS TEXT)"),
        Rule::new(r"(?i)\bTO_CHAR\s*\(([^)]+)\)", "CAST($1 AS TEXT)"),
        Rule::new(r"(?i)\bTO_NUMBER\s*\(([^)]+)\)", "CAST($1 AS REAL)"),
        Rule::new(r"(?i)\bTO_NUMERIC\s*\(([^)]+)\)", "CAST($1 AS REAL)"),
        Rule::new(r"(?i)\bTO_DECIMAL\s*\(([^)]+)\)", "CAST($1 AS REAL)"),
        Rule::new(r"(?i)\bTO_DOUBLE\s*\(([^)]+)\)", "CAST($1 AS REAL)"),
        Rule::new(r"(?i)\bTO_BOOLEAN\s*\(([^)]+)\)", "CAST($1 AS INTEGER)"),
        Rule::new(r"(?i)\bTO_BINARY\s*\(([^)]+)\)", "CAST($1 AS BLOB)"),
        Rule::new(r"(?i)\bTO_DATE\s*\(([^)]+)\)", "DATE($1)"),
        Rule::new(r"(?i)\bTO_TIME\s*\(([^)]+)\)", "TIME($1)"),
        Rule::new(r"(?i)\bTO_TIMESTAMP\s*\(([^)]+)\)", "DATETIME($1)"),
        Rule::new(r"(?i)\bTO_TIMESTAMP_NTZ\s*\(([^)]+)\)", "DATETIME($1)"),
        Rule::new(r"(?i)\bTO_TIMESTAMP_LTZ\s*\(([^)]+)\)", "DATETIME($1)"),
        Rule::new(r"(?i)\bTO_TIMESTAMP_TZ\s*\(([^)]+)\)", "DATETIME($1)"),
        // Current date/time
        Rule::new(r"(?i)\bCURRENT_TIMESTAMP\s*\(\s*\)", "DATETIME('now')"),
        Rule::new(r"(?i)\bCURRENT_TIMESTAMP\b", "DATETIME('now')"),
        Rule::new(r"(?i)\bGETDATE\s*\(\s*\)", "DATETIME('now')"),
        Rule::new(r"(?i)\bSYSDATE\s*\(\s*\)", "DATETIME('now')"),
        Rule::new(r"(?i)\bCURRENT_DATE\s*\(\s*\)", "DATE('now')"),
        Rule::new(r"(?i)\bCURRENT_DATE\b", "DATE('now')"),
        Rule::new(r"(?i)\bCURRENT_TIME\s*\(\s*\)", "TIME('now')"),
        Rule::new(r"(?i)\bCURRENT_TIME\b", "TIME('now')"),
        Rule::new(r"(?i)\bLOCALTIMESTAMP\s*\(\s*\)", "DATETIME('now')"),
        Rule::new(r"(?i)\bLOCALTIME\s*\(\s*\)", "TIME('now')"),
        // Date part extraction
        Rule::new(r"(?i)\bYEAR\s*\(([^)]+)\)", "CAST(STRFTIME('%Y', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bMONTH\s*\(([^)]+)\)", "CAST(STRFTIME('%m', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bDAY\s*\(([^)]+)\)", "CAST(STRFTIME('%d', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bHOUR\s*\(([^)]+)\)", "CAST(STRFTIME('%H', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bMINUTE\s*\(([^)]+)\)", "CAST(STRFTIME('%M', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bSECOND\s*\(([^)]+)\)", "CAST(STRFTIME('%S', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bDAYOFWEEK\s*\(([^)]+)\)", "CAST(STRFTIME('%w', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bDAYOFYEAR\s*\(([^)]+)\)", "CAST(STRFTIME('%j', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bWEEKOFYEAR\s*\(([^)]+)\)", "CAST(STRFTIME('%W', $1) AS INTEGER)"),
        Rule::new(r"(?i)\bQUARTER\s*\(([^)]+)\)", "((CAST(STRFTIME('%m', $1) AS INTEGER) + 2) / 3)"),
        // String functions
        // RLIKE is a Snowflake alias for REGEXP; SQLite's REGEXP operator calls regexp(pattern, text)
        Rule::new(r"(?i)\bRLIKE\b", "REGEXP"),
        Rule::new(r"(?i)\bCONTAINS\s*\(([^,]+),\s*([^)]+)\)", "(INSTR($1, $2) > 0)"),
        Rule::new(r"(?i)\bSTARTSWITH\s*\(([^,]+),\s*([^)]+)\)", "($1 LIKE $2 || '%')"),
        Rule::new(r"(?i)\bENDSWITH\s*\(([^,]+),\s*([^)]+)\)", "($1 LIKE '%' || $2)"),
        Rule::new(r"(?i)\bCHARINDEX\s*\(([^,]+),\s*([^)]+)\)", "INSTR($2, $1)"),
        Rule::new(r"(?i)\bINSTRSPACE\s*\(([^)]+)\)", "INSTR($1)"),
        Rule::new(r"(?i)\bSPACE\s*\(([^)]+)\)", "SUBSTR('                                ', 1, $1)"),
        Rule::new(r"(?i)\bSTRPOS\s*\(([^,]+),\s*([^)]+)\)", "INSTR($1, $2)"),
        Rule::new(r"(?i)\bLTRIM\s*\(([^,)]+)\)", "LTRIM($1)"),
        Rule::new(r"(?i)\bRTRIM\s*\(([^,)]+)\)", "RTRIM($1)"),
        Rule::new(r"(?i)\bBITAND\s*\(([^,]+),\s*([^)]+)\)", "($1 & $2)"),
        Rule::new(r"(?i)\bBITOR\s*\(([^,]+),\s*([^)]+)\)", "($1 | $2)"),
        Rule::new(r"(?i)\bBITXOR\s*\(([^,]+),\s*([^)]+)\)", "($1 ^ $2)"),
        Rule::new(r"(?i)\bBITSHIFTLEFT\s*\(([^,]+),\s*([^)]+)\)", "($1 << $2)"),
        Rule::new(r"(?i)\bBITSHIFTRIGHT\s*\(([^,]+),\s*([^)]+)\)", "($1 >> $2)"),
        // Math
        Rule::new(r"(?i)\bDIV0\s*\(([^,]+),\s*([^)]+)\)", "CASE WHEN $2 = 0 THEN 0 ELSE $1 / $2 END"),
        Rule::new(r"(?i)\bDIV0NULL\s*\(([^,]+),\s*([^)]+)\)", "CASE WHEN $2 = 0 THEN NULL ELSE $1 / $2 END"),
        Rule::new(r"(?i)\bSQUARE\s*\(([^)]+)\)", "(($1) * ($1))"),
        Rule::new(r"(?i)\bCBRT\s*\(([^)]+)\)", "POWER($1, 1.0/3.0)"),
        Rule::new(r"(?i)\bLN\s*\(([^)]+)\)", "LOG($1)"),
        // GREATEST / LEAST — SQLite multi-arg MAX/MIN are scalar when called with 2+ args
        Rule::new(r"(?i)\bGREATEST\s*\(", "MAX("),
        Rule::new(r"(?i)\bLEAST\s*\(", "MIN("),
        // Semi-structured — ARRAY_SIZE
        Rule::new(r"(?i)\bARRAY_SIZE\s*\(([^)]+)\)", "JSON_ARRAY_LENGTH($1)"),
        Rule::new(r"(?i)\bARRAY_LENGTH\s*\(([^)]+)\)", "JSON_ARRAY_LENGTH($1)"),
        // PARSE_JSON is a no-op (values are already stored as JSON text)
        Rule::new(r"(?i)\bPARSE_JSON\s*\(([^)]+)\)", "$1"),
        // Snowflake OBJECT_CONSTRUCT — store as JSON
        // (basic two-arg version; complex versions require a custom function)
        Rule::new(
            r"(?i)\bOBJECT_CONSTRUCT\s*\(\s*\)",
            "JSON('{}')",
        ),
    ]
});

fn apply_simple_rules(sql: &str) -> String {
    let mut result = sql.to_owned();
    for rule in SIMPLE_RULES.iter() {
        result = rule.apply(&result);
    }
    result
}

// ── IFF ─────────────────────────────────────────────────────────────────────

/// Translate `IFF(condition, true_val, false_val)` →
/// `CASE WHEN condition THEN true_val ELSE false_val END`.
///
/// This is done character-by-character to handle nested parentheses correctly.
pub fn rewrite_iff(sql: &str) -> String {
    static RE_IFF: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)\bIFF\s*\(").expect("valid IFF regex"));

    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        if let Some(m) = RE_IFF.find(&sql[i..]) {
            if m.start() > 0 {
                result.push_str(&sql[i..i + m.start()]);
            }
            i += m.end();

            // Parse three comma-separated arguments respecting nested parens/quotes
            let args_start = i;
            let args = &sql[args_start..];
            if let Some((cond, t_val, f_val, consumed)) = split_three_args(args) {
                result.push_str("CASE WHEN ");
                result.push_str(cond.trim());
                result.push_str(" THEN ");
                result.push_str(t_val.trim());
                result.push_str(" ELSE ");
                result.push_str(f_val.trim());
                result.push_str(" END");
                i = args_start + consumed + 1; // +1 for closing ')'
            } else {
                // Fallback: keep as-is
                result.push_str("IFF(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── DECODE ───────────────────────────────────────────────────────────────────

/// Translate `DECODE(expr, s1, r1, s2, r2, ...[, default])` →
/// `CASE expr WHEN s1 THEN r1 WHEN s2 THEN r2 ... [ELSE default] END`.
pub fn rewrite_decode(sql: &str) -> String {
    static RE_DECODE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)\bDECODE\s*\(").expect("valid DECODE regex"));

    let mut result = String::with_capacity(sql.len());
    let len = sql.len();
    let mut i = 0;

    while i < len {
        if let Some(m) = RE_DECODE.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();

            let args_start = i;
            if let Some(args) = extract_parenthesized(&sql[args_start..]) {
                let parts = split_args(args);
                if parts.len() >= 3 {
                    let expr = parts[0].trim();
                    let mut case = format!("CASE {expr}");
                    let mut k = 1;
                    while k + 1 < parts.len() {
                        case.push_str(&format!(
                            " WHEN {} THEN {}",
                            parts[k].trim(),
                            parts[k + 1].trim()
                        ));
                        k += 2;
                    }
                    // Odd remaining arg = ELSE default
                    if k < parts.len() {
                        case.push_str(&format!(" ELSE {}", parts[k].trim()));
                    }
                    case.push_str(" END");
                    result.push_str(&case);
                    i = args_start + args.len() + 1; // skip past closing ')'
                } else {
                    result.push_str("DECODE(");
                }
            } else {
                result.push_str("DECODE(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── NVL2 ─────────────────────────────────────────────────────────────────────

pub fn rewrite_nvl2(sql: &str) -> String {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)\bNVL2\s*\(").expect("valid NVL2 regex"));

    let mut result = String::with_capacity(sql.len());
    let len = sql.len();
    let mut i = 0;

    while i < len {
        if let Some(m) = RE.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();
            let args_start = i;
            if let Some((a, b, c, consumed)) = split_three_args(&sql[args_start..]) {
                result.push_str("CASE WHEN ");
                result.push_str(a.trim());
                result.push_str(" IS NOT NULL THEN ");
                result.push_str(b.trim());
                result.push_str(" ELSE ");
                result.push_str(c.trim());
                result.push_str(" END");
                i = args_start + consumed + 1;
            } else {
                result.push_str("NVL2(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── DATEADD ──────────────────────────────────────────────────────────────────

pub fn rewrite_dateadd(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bDATEADD\s*\(").expect("valid DATEADD regex")
    });

    let mut result = String::with_capacity(sql.len());
    let mut i = 0;

    while i < sql.len() {
        if let Some(m) = RE.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();

            // Use paren-aware splitting for the three arguments
            if let Some((unit_str, n, date, consumed)) = split_three_args(&sql[i..]) {
                let unit = unit_str.trim().to_ascii_lowercase();
                let n = n.trim();
                let date = date.trim();
                let replacement = match unit.as_str() {
                    "year" => format!("DATE({date}, ({n}) || ' years')"),
                    "quarter" => format!("DATE({date}, (({n}) * 3) || ' months')"),
                    "month" => format!("DATE({date}, ({n}) || ' months')"),
                    "week" => format!("DATE({date}, (({n}) * 7) || ' days')"),
                    "day" => format!("DATE({date}, ({n}) || ' days')"),
                    "hour" => format!("DATETIME({date}, ({n}) || ' hours')"),
                    "minute" => format!("DATETIME({date}, ({n}) || ' minutes')"),
                    "second" => format!("DATETIME({date}, ({n}) || ' seconds')"),
                    _ => {
                        result.push_str("DATEADD(");
                        continue;
                    }
                };
                result.push_str(&replacement);
                i += consumed + 1; // +1 for closing ')'
            } else {
                result.push_str("DATEADD(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── DATEDIFF ─────────────────────────────────────────────────────────────────

pub fn rewrite_datediff(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bDATEDIFF\s*\(").expect("valid DATEDIFF regex")
    });

    let mut result = String::with_capacity(sql.len());
    let mut i = 0;

    while i < sql.len() {
        if let Some(m) = RE.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();

            if let Some((unit_str, d1, d2, consumed)) = split_three_args(&sql[i..]) {
                let unit = unit_str.trim().to_ascii_lowercase();
                let d1 = d1.trim();
                let d2 = d2.trim();
                let replacement = match unit.as_str() {
                    "day" => format!("(JULIANDAY({d2}) - JULIANDAY({d1}))"),
                    "week" => format!("((JULIANDAY({d2}) - JULIANDAY({d1})) / 7)"),
                    "hour" => format!("((JULIANDAY({d2}) - JULIANDAY({d1})) * 24)"),
                    "minute" => format!("((JULIANDAY({d2}) - JULIANDAY({d1})) * 1440)"),
                    "second" => format!("((JULIANDAY({d2}) - JULIANDAY({d1})) * 86400)"),
                    "month" => format!(
                        "((CAST(STRFTIME('%Y', {d2}) AS INTEGER) - CAST(STRFTIME('%Y', {d1}) AS INTEGER)) * 12 + \
                          CAST(STRFTIME('%m', {d2}) AS INTEGER) - CAST(STRFTIME('%m', {d1}) AS INTEGER))"
                    ),
                    "year" => format!(
                        "(CAST(STRFTIME('%Y', {d2}) AS INTEGER) - CAST(STRFTIME('%Y', {d1}) AS INTEGER))"
                    ),
                    "quarter" => format!(
                        "(((CAST(STRFTIME('%Y', {d2}) AS INTEGER) - CAST(STRFTIME('%Y', {d1}) AS INTEGER)) * 12 + \
                           CAST(STRFTIME('%m', {d2}) AS INTEGER) - CAST(STRFTIME('%m', {d1}) AS INTEGER)) / 3)"
                    ),
                    _ => {
                        result.push_str("DATEDIFF(");
                        continue;
                    }
                };
                result.push_str(&replacement);
                i += consumed + 1;
            } else {
                result.push_str("DATEDIFF(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── DATE_TRUNC ───────────────────────────────────────────────────────────────

pub fn rewrite_date_trunc(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bDATE_TRUNC\s*\(").expect("valid DATE_TRUNC regex")
    });

    let mut result = String::with_capacity(sql.len());
    let mut i = 0;

    while i < sql.len() {
        if let Some(m) = RE.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();

            // DATE_TRUNC has two arguments: unit and date expression
            if let Some(args_content) = extract_parenthesized(&sql[i..]) {
                let parts = split_args(args_content);
                if parts.len() == 2 {
                    // Strip optional quotes around the unit keyword
                    let unit = parts[0].trim().trim_matches('\'').to_ascii_lowercase();
                    let date = parts[1].trim();
                    let replacement = match unit.as_str() {
                        "year" => format!("DATE({date}, 'start of year')"),
                        "month" => format!("DATE({date}, 'start of month')"),
                        "day" => format!("DATE({date})"),
                        "hour" => format!("STRFTIME('%Y-%m-%d %H:00:00', {date})"),
                        "minute" => format!("STRFTIME('%Y-%m-%d %H:%M:00', {date})"),
                        "second" => format!("STRFTIME('%Y-%m-%d %H:%M:%S', {date})"),
                        "quarter" => format!(
                            "DATE({date}, '-' || ((CAST(STRFTIME('%m', {date}) AS INTEGER) - 1) % 3) || ' months', 'start of month')"
                        ),
                        "week" => format!("DATE({date}, 'weekday 1', '-7 days')"),
                        _ => {
                            result.push_str("DATE_TRUNC(");
                            continue;
                        }
                    };
                    result.push_str(&replacement);
                    i += args_content.len() + 1; // +1 for closing ')'
                } else {
                    result.push_str("DATE_TRUNC(");
                }
            } else {
                result.push_str("DATE_TRUNC(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── LISTAGG ──────────────────────────────────────────────────────────────────

/// Translate `LISTAGG(expr [, delimiter]) WITHIN GROUP (ORDER BY ...)` →
/// `GROUP_CONCAT(expr [, delimiter])`.
///
/// The `WITHIN GROUP (ORDER BY ...)` clause is consumed and dropped — SQLite's
/// `GROUP_CONCAT` does not support an internal `ORDER BY`.
pub fn rewrite_listagg(sql: &str) -> String {
    static RE_LISTAGG: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)\bLISTAGG\s*\(").expect("valid LISTAGG regex"));
    static RE_WITHIN: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)^\s*WITHIN\s+GROUP\s*\(").expect("valid WITHIN GROUP regex")
    });

    let mut result = String::with_capacity(sql.len());
    let len = sql.len();
    let mut i = 0;

    while i < len {
        if let Some(m) = RE_LISTAGG.find(&sql[i..]) {
            result.push_str(&sql[i..i + m.start()]);
            i += m.end();

            if let Some(args_content) = extract_parenthesized(&sql[i..]) {
                let parts = split_args(args_content);
                let expr = parts.first().map(|s| s.trim()).unwrap_or("");
                let delim = parts.get(1).map(|s| s.trim());
                i += args_content.len() + 1; // +1 for closing ')'

                // Consume the mandatory WITHIN GROUP (ORDER BY ...) clause
                if let Some(wm) = RE_WITHIN.find(&sql[i..]) {
                    let within_start = i + wm.end();
                    if let Some(within_content) = extract_parenthesized(&sql[within_start..]) {
                        i = within_start + within_content.len() + 1; // +1 for ')'
                    }
                }

                match delim {
                    Some(d) => result.push_str(&format!("GROUP_CONCAT({expr}, {d})")),
                    None => result.push_str(&format!("GROUP_CONCAT({expr})")),
                }
            } else {
                result.push_str("LISTAGG(");
            }
        } else {
            result.push_str(&sql[i..]);
            break;
        }
    }
    result
}

// ── Semi-structured path expressions ────────────────────────────────────────

/// Translate Snowflake semi-structured paths:
/// - `col:field` → `JSON_EXTRACT(col, '$.field')`
/// - `col['field']` → `JSON_EXTRACT(col, '$.field')`
/// - `col[0]` → `JSON_EXTRACT(col, '$[0]')`
pub fn rewrite_semi_structured_paths(sql: &str) -> String {
    // col:path.subpath → JSON_EXTRACT(col, '$.path.subpath')
    static RE_COLON_PATH: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b([A-Za-z_][A-Za-z0-9_]*):((?:[A-Za-z_][A-Za-z0-9_]*)(?:\.[A-Za-z_][A-Za-z0-9_]*)*)")
            .expect("valid colon path regex")
    });
    let sql = RE_COLON_PATH
        .replace_all(sql, |caps: &regex::Captures| {
            format!("JSON_EXTRACT({}, '$.{}')", &caps[1], &caps[2])
        })
        .into_owned();

    // col['field'] → JSON_EXTRACT(col, '$.field')
    static RE_BRACKET_STR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?i)\b([A-Za-z_][A-Za-z0-9_]*)\[(?:'([^']+)'|"([^"]+)")\]"#)
            .expect("valid bracket string path regex")
    });
    let sql = RE_BRACKET_STR
        .replace_all(&sql, |caps: &regex::Captures| {
            let col = &caps[1];
            let field = caps.get(2).or_else(|| caps.get(3)).map(|m| m.as_str()).unwrap_or("");
            // Sanitize field name: escape single quotes to prevent JSON path injection
            let field = field.replace('\'', "");
            format!("JSON_EXTRACT({col}, '$.{field}')")
        })
        .into_owned();

    // col[n] → JSON_EXTRACT(col, '$[n]')
    static RE_BRACKET_INT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b([A-Za-z_][A-Za-z0-9_]*)\[(\d+)\]")
            .expect("valid bracket int path regex")
    });
    RE_BRACKET_INT
        .replace_all(&sql, |caps: &regex::Captures| {
            format!("JSON_EXTRACT({}, '$[{}]')", &caps[1], &caps[2])
        })
        .into_owned()
}

// ── ILIKE ────────────────────────────────────────────────────────────────────

/// Translate `expr ILIKE pattern` → `LOWER(expr) LIKE LOWER(pattern)`.
pub fn rewrite_ilike(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)([^\s(]+)\s+ILIKE\s+([^\s)]+)").expect("valid ILIKE regex")
    });
    RE.replace_all(sql, "LOWER($1) LIKE LOWER($2)")
        .into_owned()
}

// ── CREATE OR REPLACE TABLE ──────────────────────────────────────────────────

/// Translate `CREATE OR REPLACE TABLE name (...)` →
/// `DROP TABLE IF EXISTS name; CREATE TABLE name (...)`.
pub fn rewrite_create_or_replace(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bCREATE\s+OR\s+REPLACE\s+TABLE\s+")
            .expect("valid CREATE OR REPLACE regex")
    });
    RE.replace_all(sql, "CREATE TABLE IF NOT EXISTS ").into_owned()
}

// ── TOP n → LIMIT n ──────────────────────────────────────────────────────────

pub fn rewrite_top_n(sql: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bSELECT\s+TOP\s+(\d+)\b").expect("valid TOP regex")
    });
    // This is a simplistic rewrite — it adds LIMIT at the end.
    // For a full implementation an AST-based approach would be needed.
    if let Some(caps) = RE.captures(sql) {
        let n = &caps[1];
        let replaced = RE.replace(sql, "SELECT").into_owned();
        format!("{replaced} LIMIT {n}")
    } else {
        sql.to_owned()
    }
}

// ── Helper: nested-paren-aware argument splitting ─────────────────────────────

/// Extract the content inside the outermost `(...)`, returning the content
/// without the surrounding parentheses and the number of bytes consumed.
fn extract_parenthesized(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'(') {
        // Already consumed the opening paren by the regex; content starts at 0
    }
    let mut depth = 1u32;
    let mut in_single = false;
    let mut in_double = false;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'(' if !in_single && !in_double => {
                depth = depth.saturating_add(1);
            }
            b')' if !in_single && !in_double => {
                if depth <= 1 {
                    return Some(&s[..i]);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Split a comma-separated argument list respecting nested parens and quotes.
pub fn split_args(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let bytes = s.as_bytes();
    let mut depth = 0u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'(' if !in_single && !in_double => {
                depth = depth.saturating_add(1);
            }
            b')' if !in_single && !in_double => {
                depth = depth.saturating_sub(1);
            }
            b',' if depth == 0 && !in_single && !in_double => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Split exactly three comma-separated arguments from `s` (which begins right
/// after the opening paren).  Returns `(arg1, arg2, arg3, bytes_consumed)`
/// where `bytes_consumed` does NOT include the final `)`.
fn split_three_args(s: &str) -> Option<(&str, &str, &str, usize)> {
    let bytes = s.as_bytes();
    let mut depth = 0u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut commas = Vec::new();
    let mut end = None;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'(' if !in_single && !in_double => {
                depth = depth.saturating_add(1);
            }
            b')' if !in_single && !in_double => {
                if depth == 0 {
                    end = Some(i);
                    break;
                }
                depth -= 1;
            }
            b',' if depth == 0 && !in_single && !in_double => {
                commas.push(i);
            }
            _ => {}
        }
    }

    if commas.len() < 2 {
        return None;
    }
    let end = end?;
    Some((
        &s[..commas[0]],
        &s[commas[0] + 1..commas[1]],
        &s[commas[1] + 1..end],
        end,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iff_basic() {
        assert_eq!(
            rewrite_iff("SELECT IFF(x > 0, 'pos', 'neg') FROM t"),
            "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"
        );
    }

    #[test]
    fn nvl_basic() {
        assert_eq!(
            apply_simple_rules("SELECT NVL(a, 0) FROM t"),
            "SELECT COALESCE(a, 0) FROM t"
        );
    }

    #[test]
    fn to_varchar() {
        assert_eq!(
            apply_simple_rules("SELECT TO_VARCHAR(amount)"),
            "SELECT CAST(amount AS TEXT)"
        );
    }

    #[test]
    fn current_timestamp() {
        assert_eq!(
            apply_simple_rules("SELECT CURRENT_TIMESTAMP()"),
            "SELECT DATETIME('now')"
        );
    }

    #[test]
    fn dateadd() {
        let result = rewrite_dateadd("DATEADD(day, 7, created_at)");
        assert!(result.contains("DATE(created_at"), "got: {result}");
        assert!(result.contains("7"), "got: {result}");
    }

    #[test]
    fn datediff_day() {
        let result = rewrite_datediff("DATEDIFF(day, start_date, end_date)");
        assert!(result.contains("JULIANDAY"), "got: {result}");
    }

    #[test]
    fn semi_structured_colon() {
        assert_eq!(
            rewrite_semi_structured_paths("SELECT metadata:user_id FROM events"),
            "SELECT JSON_EXTRACT(metadata, '$.user_id') FROM events"
        );
    }

    #[test]
    fn ilike_basic() {
        assert_eq!(
            rewrite_ilike("WHERE name ILIKE '%john%'"),
            "WHERE LOWER(name) LIKE LOWER('%john%')"
        );
    }

    #[test]
    fn create_or_replace() {
        let sql = "CREATE OR REPLACE TABLE foo (id INT)";
        let result = rewrite_create_or_replace(sql);
        assert!(result.contains("CREATE TABLE IF NOT EXISTS foo"));
    }

    #[test]
    fn decode_basic() {
        let result = rewrite_decode("DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')");
        assert!(result.contains("CASE status"), "got: {result}");
        assert!(result.contains("WHEN 'A' THEN 'Active'"), "got: {result}");
        assert!(result.contains("ELSE 'Unknown'"), "got: {result}");
    }

    #[test]
    fn listagg_with_delimiter() {
        let result = rewrite_listagg(
            "SELECT LISTAGG(item, ',') WITHIN GROUP (ORDER BY item) FROM t GROUP BY cat",
        );
        assert_eq!(
            result,
            "SELECT GROUP_CONCAT(item, ',') FROM t GROUP BY cat"
        );
    }

    #[test]
    fn listagg_without_delimiter() {
        let result = rewrite_listagg(
            "SELECT LISTAGG(val) WITHIN GROUP (ORDER BY val) FROM t",
        );
        assert_eq!(result, "SELECT GROUP_CONCAT(val) FROM t");
    }

    #[test]
    fn greatest_translates_to_max() {
        assert_eq!(
            apply_simple_rules("SELECT GREATEST(a, b, c) FROM t"),
            "SELECT MAX(a, b, c) FROM t"
        );
    }

    #[test]
    fn least_translates_to_min() {
        assert_eq!(
            apply_simple_rules("SELECT LEAST(a, b) FROM t"),
            "SELECT MIN(a, b) FROM t"
        );
    }

    #[test]
    fn listagg_with_nested_function_arg() {
        let result = rewrite_listagg(
            "LISTAGG(UPPER(name), '|') WITHIN GROUP (ORDER BY name)",
        );
        assert_eq!(result, "GROUP_CONCAT(UPPER(name), '|')");
    }
}
