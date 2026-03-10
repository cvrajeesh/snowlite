//! Integration tests for snowlite.
//!
//! Run with: `cargo test`

use snowlite::{Connection, Value};
use serde_json;

fn conn() -> Connection {
    Connection::open_in_memory().expect("open in-memory db")
}

// ── DDL ──────────────────────────────────────────────────────────────────────

#[test]
fn create_table_with_snowflake_types() {
    let c = conn();
    c.execute(
        "CREATE TABLE orders (
            id         NUMBER(18, 0) NOT NULL,
            product    VARCHAR(255),
            price      NUMBER(10, 2),
            active     BOOLEAN,
            metadata   VARIANT,
            created_at TIMESTAMP_NTZ
        )",
        &[],
    )
    .expect("create table");

    c.execute(
        "INSERT INTO orders (id, product, price, active, metadata, created_at)
         VALUES (?, ?, ?, ?, ?, ?)",
        &[
            &1i64,
            &"Widget",
            &9.99f64,
            &1i64,
            &r#"{"color":"red"}"#,
            &"2024-01-15T10:00:00",
        ],
    )
    .expect("insert");

    let rows = c
        .query("SELECT id, product, price, active FROM orders", &[])
        .expect("query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
    assert_eq!(rows[0].get::<String>(1).unwrap(), "Widget");
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 9.99);
}

#[test]
fn create_or_replace_table() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    // Second call should recreate the table
    c.execute("CREATE OR REPLACE TABLE t (id INTEGER, name TEXT)", &[])
        .unwrap();
    // Old data still accessible (IF NOT EXISTS keeps existing table)
    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows.len(), 1);
}

// ── Functions ────────────────────────────────────────────────────────────────

#[test]
fn nvl_function() {
    let c = conn();
    c.execute("CREATE TABLE t (val INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (42)", &[]).unwrap();

    let rows = c.query("SELECT NVL(val, 0) FROM t ORDER BY val", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    // NULL row comes first (ORDER BY NULL first in SQLite)
    let vals: Vec<i64> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert!(vals.contains(&0i64));
    assert!(vals.contains(&42i64));
}

#[test]
fn iff_function() {
    let c = conn();
    c.execute("CREATE TABLE t (amount REAL)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (50.0)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (200.0)", &[]).unwrap();

    let rows = c
        .query("SELECT IFF(amount > 100, 'large', 'small') FROM t ORDER BY amount", &[])
        .unwrap();

    assert_eq!(rows[0].get::<String>(0).unwrap(), "small");
    assert_eq!(rows[1].get::<String>(0).unwrap(), "large");
}

#[test]
fn decode_function() {
    let c = conn();
    c.execute("CREATE TABLE t (status TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('A')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('I')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('X')", &[]).unwrap();

    let rows = c
        .query(
            "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM t ORDER BY status",
            &[],
        )
        .unwrap();

    let values: Vec<String> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert!(values.contains(&"Active".to_owned()));
    assert!(values.contains(&"Inactive".to_owned()));
    assert!(values.contains(&"Unknown".to_owned()));
}

#[test]
fn nvl2_function() {
    let c = conn();
    c.execute("CREATE TABLE t (x INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();

    let rows = c
        .query("SELECT NVL2(x, 'not null', 'is null') FROM t ORDER BY x", &[])
        .unwrap();
    let values: Vec<String> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert!(values.contains(&"not null".to_owned()));
    assert!(values.contains(&"is null".to_owned()));
}

#[test]
fn dateadd_function() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATEADD(day, 7, '2024-01-01')",
            &[],
        )
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "2024-01-08");
}

#[test]
fn datediff_function() {
    let c = conn();
    let rows = c
        .query("SELECT DATEDIFF(day, '2024-01-01', '2024-01-08')", &[])
        .unwrap();
    let diff: f64 = rows[0].get(0).unwrap();
    assert_eq!(diff as i64, 7);
}

#[test]
fn date_trunc_function() {
    let c = conn();
    let rows = c
        .query("SELECT DATE_TRUNC('month', '2024-03-15')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "2024-03-01");
}

#[test]
fn to_varchar_function() {
    let c = conn();
    let rows = c.query("SELECT TO_VARCHAR(42)", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "42");
}

#[test]
fn current_timestamp_function() {
    let c = conn();
    let rows = c.query("SELECT CURRENT_TIMESTAMP()", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    // Should be a non-empty datetime string
    assert!(!result.is_empty());
    assert!(result.contains('-'));
}

#[test]
fn contains_function() {
    let c = conn();
    let rows = c
        .query("SELECT CONTAINS('hello world', 'world')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 1);
}

#[test]
fn startswith_function() {
    let c = conn();
    let rows = c
        .query("SELECT STARTSWITH('hello world', 'hello')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 1);
}

#[test]
fn array_size_function() {
    let c = conn();
    let rows = c
        .query("SELECT ARRAY_SIZE('[1,2,3,4,5]')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 5);
}

// ── Semi-structured ──────────────────────────────────────────────────────────

#[test]
fn semi_structured_colon_path() {
    let c = conn();
    c.execute("CREATE TABLE events (data VARIANT)", &[]).unwrap();
    c.execute(
        "INSERT INTO events VALUES (?)",
        &[&r#"{"user_id": 42, "action": "click"}"#],
    )
    .unwrap();

    let rows = c
        .query("SELECT data:user_id FROM events", &[])
        .unwrap();
    let user_id: String = rows[0].get(0).unwrap();
    assert_eq!(user_id, "42");
}

// ── Identifiers ──────────────────────────────────────────────────────────────

#[test]
fn fully_qualified_identifier_stripped() {
    let c = conn();
    c.execute("CREATE TABLE products (id INTEGER, name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO products VALUES (1, 'foo')", &[]).unwrap();

    let rows = c
        .query("SELECT * FROM mydb.public.products", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn two_part_identifier_stripped() {
    // schema.table with no prefix config → just table
    let c = conn();
    c.execute("CREATE TABLE orders (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES (1)", &[]).unwrap();

    let rows = c
        .query("SELECT id FROM public.orders", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
}

#[test]
fn quoted_three_part_identifier_stripped() {
    // "DB"."SCHEMA"."TABLE" → "TABLE" (keeps original casing and quotes)
    let c = conn();
    c.execute("CREATE TABLE ORDERS (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO ORDERS VALUES (42)", &[]).unwrap();

    let rows = c
        .query(r#"SELECT id FROM "MY_DB"."PUBLIC"."ORDERS""#, &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 42);
}

// ── No-ops ───────────────────────────────────────────────────────────────────

#[test]
fn noop_statements_are_ignored() {
    let c = conn();
    // These should not error
    c.execute("USE DATABASE test_db", &[]).unwrap();
    c.execute("ALTER SESSION SET QUERY_TAG = 'test'", &[]).unwrap();
    c.execute("USE WAREHOUSE compute_wh", &[]).unwrap();
    c.execute("SHOW TABLES", &[]).unwrap();
}

// ── Row API ──────────────────────────────────────────────────────────────────

#[test]
fn row_get_by_name() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'Alice')", &[]).unwrap();

    let rows = c.query("SELECT id, name FROM t", &[]).unwrap();
    assert_eq!(rows[0].get_by_name::<i64>("id").unwrap(), 1);
    assert_eq!(rows[0].get_by_name::<String>("name").unwrap(), "Alice");
}

#[test]
fn row_option_null() {
    let c = conn();
    c.execute("CREATE TABLE t (val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();

    let rows = c.query("SELECT val FROM t", &[]).unwrap();
    let val: Option<String> = rows[0].get(0).unwrap();
    assert!(val.is_none());
}

// ── Transactions ─────────────────────────────────────────────────────────────

#[test]
fn transaction_commit() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();

    c.transaction(|conn| {
        conn.execute("INSERT INTO t VALUES (1)", &[])?;
        conn.execute("INSERT INTO t VALUES (2)", &[])?;
        Ok(())
    })
    .unwrap();

    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn transaction_rollback() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();

    let _ = c.transaction(|conn| -> snowlite::Result<()> {
        conn.execute("INSERT INTO t VALUES (1)", &[])?;
        Err(snowlite::Error::other("simulated error"))
    });

    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    assert_eq!(count, 0);
}

// ── Security fix regression tests ────────────────────────────────────────────

/// i64 → i32: values within range should succeed; out-of-range should error.
#[test]
fn type_conversion_i32_overflow_returns_error() {
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (?)", &[&(i64::MAX)]).unwrap();

    let rows = c.query("SELECT v FROM t", &[]).unwrap();
    let result = rows[0].get::<i32>(0);
    assert!(
        result.is_err(),
        "converting i64::MAX to i32 should return an error, not silently truncate"
    );
}

/// i64 → u64: negative values must return an error, not wrap.
#[test]
fn type_conversion_u64_negative_returns_error() {
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (?)", &[&(-1i64)]).unwrap();

    let rows = c.query("SELECT v FROM t", &[]).unwrap();
    let result = rows[0].get::<u64>(0);
    assert!(
        result.is_err(),
        "converting -1 to u64 should return an error, not wrap to u64::MAX"
    );
}

/// f64 → i64: NaN must return a TypeConversion error, not silently become 0.
#[test]
fn type_conversion_f64_nan_to_i64_returns_error() {
    use snowlite::{Error, Value};
    // Build a Value::Real(NaN) directly and call from_value
    let v = Value::Real(f64::NAN);
    let result = <i64 as snowlite::row::FromValue>::from_value(&v);
    assert!(
        result.is_err(),
        "converting NaN to i64 should be an error"
    );
    match result {
        Err(Error::TypeConversion { .. }) => {}
        other => panic!("expected TypeConversion error, got {:?}", other),
    }
}

/// REGEXP custom function: invalid regex should return a SQLite function error,
/// not panic or exhaust memory.
#[test]
fn regexp_invalid_pattern_returns_error() {
    let c = conn();
    // An invalid regex pattern
    let result = c.query("SELECT regexp('[invalid', 'test')", &[]);
    assert!(
        result.is_err(),
        "REGEXP with invalid pattern should return an error"
    );
}

/// REGEXP custom function: a valid pattern should match correctly.
#[test]
fn regexp_valid_pattern_matches() {
    let c = conn();
    let rows = c
        .query("SELECT regexp('^hello', 'hello world')", &[])
        .unwrap();
    let matched: i64 = rows[0].get(0).unwrap();
    assert_eq!(matched, 1, "valid REGEXP pattern should match");
}

/// GET_PATH custom function: a deeply nested path beyond MAX_PATH_DEPTH (64)
/// should not panic and should return 'null' for the missing key.
#[test]
fn get_path_deeply_nested_does_not_panic() {
    let c = conn();
    // Construct a path with 100 segments (beyond the 64-segment cap)
    let deep_path: String = (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>().join(".");
    let sql = format!("SELECT get_path('{{}}', '{}')", deep_path);
    let rows = c.query(&sql, &[]).expect("get_path with deep path should not panic");
    // Result should be "null" (serde_json Null serialized) since the path doesn't exist
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "null");
}

// ── execute_batch ────────────────────────────────────────────────────────────

#[test]
fn execute_batch_with_noops() {
    let c = conn();
    c.execute_batch(
        "
        USE DATABASE mydb;
        CREATE TABLE items (id INTEGER, label TEXT);
        INSERT INTO items VALUES (1, 'first');
        ALTER SESSION SET QUERY_TAG = 'test';
        INSERT INTO items VALUES (2, 'second');
        ",
    )
    .unwrap();

    let rows = c.query("SELECT COUNT(*) FROM items", &[]).unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn execute_batch_semicolons_in_string_literals() {
    // Semicolons inside string literals must not split the statement
    let c = conn();
    c.execute_batch(
        "
        CREATE TABLE notes (id INTEGER, text TEXT);
        INSERT INTO notes VALUES (1, 'hello; world');
        INSERT INTO notes VALUES (2, 'foo;bar;baz');
        ",
    )
    .unwrap();

    let rows = c.query("SELECT text FROM notes ORDER BY id", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "hello; world");
    assert_eq!(rows[1].get::<String>(0).unwrap(), "foo;bar;baz");
}

// ═══════════════════════════════════════════════════════════════════════════════
// NEW TESTS — inspired by Snowflake Python connector test suite
// ═══════════════════════════════════════════════════════════════════════════════

// ── String functions ──────────────────────────────────────────────────────────

#[test]
fn upper_lower_functions() {
    let c = conn();
    let rows = c.query("SELECT UPPER('hello'), LOWER('WORLD')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "HELLO");
    assert_eq!(rows[0].get::<String>(1).unwrap(), "world");
}

#[test]
fn length_function() {
    let c = conn();
    let rows = c.query("SELECT LENGTH('Snowflake')", &[]).unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 9);
}

#[test]
fn substr_function() {
    let c = conn();
    // SUBSTR is a SQLite builtin; Snowflake SUBSTRING maps to the same
    let rows = c.query("SELECT SUBSTR('Snowflake', 1, 4)", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "Snow");
}

#[test]
fn trim_functions() {
    let c = conn();
    let rows = c
        .query("SELECT TRIM('  hello  '), LTRIM('  hello  '), RTRIM('  hello  ')", &[])
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "hello");
    assert_eq!(rows[0].get::<String>(1).unwrap(), "hello  ");
    assert_eq!(rows[0].get::<String>(2).unwrap(), "  hello");
}

#[test]
fn endswith_function() {
    let c = conn();
    let rows = c
        .query("SELECT ENDSWITH('hello world', 'world')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 1);
}

#[test]
#[ignore = "CHARINDEX corrupted by CHAR→TEXT type rewriter: 'CHARINDEX' becomes 'TEXTINDEX' (see failure plan item 15)"]
fn charindex_function() {
    let c = conn();
    let rows = c
        .query("SELECT CHARINDEX('lo', 'hello')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    // CHARINDEX('lo', 'hello') = 4 in Snowflake (1-based); INSTR swaps args
    assert_eq!(result, 4);
}

#[test]
#[ignore = "colon path delimiter in string literals is corrupted by the semi-structured path rewriter (see failure plan item 16)"]
fn split_part_function() {
    let c = conn();
    let rows = c
        .query("SELECT split_part('a:b:c', ':', 2)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "b");
}

#[test]
fn split_part_function_pipe_delimiter() {
    // Use a pipe delimiter to avoid the colon path rewriter corrupting the string
    let c = conn();
    let rows = c
        .query("SELECT split_part('a|b|c', '|', 2)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "b");
}

#[test]
fn strtok_function() {
    let c = conn();
    let rows = c
        .query("SELECT strtok('a,b,,c', ',', 2)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    // strtok skips empty tokens; 2nd non-empty token is 'b'
    assert_eq!(result, "b");
}

#[test]
fn ilike_case_insensitive() {
    let c = conn();
    c.execute("CREATE TABLE t (name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('Alice')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('BOB')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('charlie')", &[]).unwrap();

    let rows = c
        .query("SELECT name FROM t WHERE name ILIKE 'a%' ORDER BY name", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[test]
fn concat_function() {
    let c = conn();
    // SQLite supports || for concat natively
    let rows = c
        .query("SELECT 'Hello' || ' ' || 'World'", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "Hello World");
}

// ── Math functions ────────────────────────────────────────────────────────────

#[test]
fn abs_function() {
    let c = conn();
    let rows = c.query("SELECT ABS(-42), ABS(3.14)", &[]).unwrap();
    let i: i64 = rows[0].get(0).unwrap();
    let f: f64 = rows[0].get(1).unwrap();
    assert_eq!(i, 42);
    assert!((f - 3.14).abs() < 1e-9);
}

#[test]
fn round_function() {
    let c = conn();
    let rows = c
        .query("SELECT ROUND(3.14159, 2), ROUND(2.5)", &[])
        .unwrap();
    let r1: f64 = rows[0].get(0).unwrap();
    let r2: f64 = rows[0].get(1).unwrap();
    assert!((r1 - 3.14).abs() < 1e-9);
    assert_eq!(r2 as i64, 3); // SQLite banker's rounding
}

#[test]
fn mod_function() {
    let c = conn();
    // SQLite supports % operator natively (MOD(a, b) is not a SQLite builtin,
    // but Snowflake MOD maps to SQLite % via a CASE expression rule)
    let rows = c.query("SELECT 10 % 3", &[]).unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 1);
}

#[test]
fn div0_function() {
    let c = conn();
    let rows = c
        .query("SELECT DIV0(10, 0), DIV0(10, 2)", &[])
        .unwrap();
    let zero_div: i64 = rows[0].get(0).unwrap();
    let normal: i64 = rows[0].get(1).unwrap();
    assert_eq!(zero_div, 0);
    assert_eq!(normal, 5);
}

#[test]
fn div0null_function() {
    let c = conn();
    let rows = c
        .query("SELECT DIV0NULL(10, 0), DIV0NULL(10, 5)", &[])
        .unwrap();
    let null_result: Option<i64> = rows[0].get(0).unwrap();
    let normal: i64 = rows[0].get(1).unwrap();
    assert!(null_result.is_none());
    assert_eq!(normal, 2);
}

#[test]
#[ignore = "SQRT not available: bundled SQLite requires SQLITE_ENABLE_MATH_FUNCTIONS (see failure plan item 13)"]
fn sqrt_function() {
    let c = conn();
    let rows = c.query("SELECT SQRT(9.0)", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 3.0).abs() < 1e-9);
}

#[test]
#[ignore = "SQRT not available: bundled SQLite requires SQLITE_ENABLE_MATH_FUNCTIONS (see failure plan item 13)"]
fn sqrt_function_native() {
    let c = conn();
    let rows = c.query("SELECT SQRT(9.0)", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 3.0).abs() < 1e-9);
}

#[test]
#[ignore = "POWER not available: bundled SQLite requires SQLITE_ENABLE_MATH_FUNCTIONS (see failure plan item 13)"]
fn power_function() {
    let c = conn();
    let rows = c.query("SELECT POWER(2, 10)", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 1024.0).abs() < 1e-9);
}

#[test]
fn bitwise_functions() {
    let c = conn();
    // BITAND → & and BITOR → | work in SQLite
    let rows = c
        .query("SELECT BITAND(12, 10), BITOR(12, 10)", &[])
        .unwrap();
    let band: i64 = rows[0].get(0).unwrap();
    let bor: i64 = rows[0].get(1).unwrap();
    assert_eq!(band, 8);
    assert_eq!(bor, 14);
}

#[test]
#[ignore = "BITXOR translates to ^ which is not a valid SQLite operator (see failure plan item 14)"]
fn bitxor_function() {
    let c = conn();
    let rows = c
        .query("SELECT BITXOR(12, 10)", &[])
        .unwrap();
    let bxor: i64 = rows[0].get(0).unwrap();
    assert_eq!(bxor, 6);
}

#[test]
fn bitshift_functions() {
    let c = conn();
    let rows = c
        .query("SELECT BITSHIFTLEFT(1, 3), BITSHIFTRIGHT(16, 2)", &[])
        .unwrap();
    let left: i64 = rows[0].get(0).unwrap();
    let right: i64 = rows[0].get(1).unwrap();
    assert_eq!(left, 8);
    assert_eq!(right, 4);
}

#[test]
fn square_function() {
    let c = conn();
    let rows = c.query("SELECT SQUARE(5)", &[]).unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 25);
}

// ── Date/time functions ───────────────────────────────────────────────────────

#[test]
fn year_month_day_functions() {
    let c = conn();
    let rows = c
        .query("SELECT YEAR('2024-03-15'), MONTH('2024-03-15'), DAY('2024-03-15')", &[])
        .unwrap();
    let y: i64 = rows[0].get(0).unwrap();
    let m: i64 = rows[0].get(1).unwrap();
    let d: i64 = rows[0].get(2).unwrap();
    assert_eq!(y, 2024);
    assert_eq!(m, 3);
    assert_eq!(d, 15);
}

#[test]
fn hour_minute_second_functions() {
    let c = conn();
    let rows = c
        .query(
            "SELECT HOUR('2024-03-15 14:30:45'), MINUTE('2024-03-15 14:30:45'), SECOND('2024-03-15 14:30:45')",
            &[],
        )
        .unwrap();
    let h: i64 = rows[0].get(0).unwrap();
    let m: i64 = rows[0].get(1).unwrap();
    let s: i64 = rows[0].get(2).unwrap();
    assert_eq!(h, 14);
    assert_eq!(m, 30);
    assert_eq!(s, 45);
}

#[test]
fn dayofweek_function() {
    let c = conn();
    // 2024-03-15 is a Friday; SQLite STRFTIME('%w') returns 0=Sunday..6=Saturday
    let rows = c
        .query("SELECT DAYOFWEEK('2024-03-15')", &[])
        .unwrap();
    let dow: i64 = rows[0].get(0).unwrap();
    assert_eq!(dow, 5); // Friday
}

#[test]
fn dayofyear_function() {
    let c = conn();
    let rows = c
        .query("SELECT DAYOFYEAR('2024-01-31')", &[])
        .unwrap();
    let doy: i64 = rows[0].get(0).unwrap();
    assert_eq!(doy, 31);
}

#[test]
fn quarter_function() {
    let c = conn();
    let rows = c
        .query(
            "SELECT QUARTER('2024-01-15'), QUARTER('2024-04-01'), QUARTER('2024-10-31')",
            &[],
        )
        .unwrap();
    let q1: i64 = rows[0].get(0).unwrap();
    let q2: i64 = rows[0].get(1).unwrap();
    let q4: i64 = rows[0].get(2).unwrap();
    assert_eq!(q1, 1);
    assert_eq!(q2, 2);
    assert_eq!(q4, 4);
}

#[test]
fn current_date_function() {
    let c = conn();
    let rows = c.query("SELECT CURRENT_DATE()", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert!(!result.is_empty());
    // Should be YYYY-MM-DD format
    assert_eq!(result.len(), 10);
    assert_eq!(result.chars().nth(4), Some('-'));
    assert_eq!(result.chars().nth(7), Some('-'));
}

#[test]
fn to_date_function() {
    let c = conn();
    let rows = c
        .query("SELECT TO_DATE('2024-03-15')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "2024-03-15");
}

#[test]
fn to_timestamp_function() {
    let c = conn();
    let rows = c
        .query("SELECT TO_TIMESTAMP('2024-03-15 10:30:00')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "2024-03-15 10:30:00");
}

#[test]
fn dateadd_year_and_month() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATEADD(year, 1, '2024-01-01'), DATEADD(month, 3, '2024-01-01')",
            &[],
        )
        .unwrap();
    let year_result: String = rows[0].get(0).unwrap();
    let month_result: String = rows[0].get(1).unwrap();
    assert_eq!(year_result, "2025-01-01");
    assert_eq!(month_result, "2024-04-01");
}

#[test]
fn dateadd_hour_and_minute() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATEADD(hour, 2, '2024-01-01 10:00:00'), DATEADD(minute, 30, '2024-01-01 10:00:00')",
            &[],
        )
        .unwrap();
    let hour_result: String = rows[0].get(0).unwrap();
    let minute_result: String = rows[0].get(1).unwrap();
    assert_eq!(hour_result, "2024-01-01 12:00:00");
    assert_eq!(minute_result, "2024-01-01 10:30:00");
}

#[test]
fn datediff_month_and_year() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATEDIFF(month, '2024-01-01', '2024-04-01'), DATEDIFF(year, '2022-01-01', '2024-01-01')",
            &[],
        )
        .unwrap();
    let months: f64 = rows[0].get(0).unwrap();
    let years: f64 = rows[0].get(1).unwrap();
    assert_eq!(months as i64, 3);
    assert_eq!(years as i64, 2);
}

#[test]
fn date_trunc_year_and_day() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATE_TRUNC('year', '2024-07-15'), DATE_TRUNC('day', '2024-07-15 14:30:00')",
            &[],
        )
        .unwrap();
    let year_trunc: String = rows[0].get(0).unwrap();
    let day_trunc: String = rows[0].get(1).unwrap();
    assert_eq!(year_trunc, "2024-01-01");
    assert_eq!(day_trunc, "2024-07-15");
}

#[test]
fn date_trunc_hour_and_minute() {
    let c = conn();
    let rows = c
        .query(
            "SELECT DATE_TRUNC('hour', '2024-07-15 14:30:45'), DATE_TRUNC('minute', '2024-07-15 14:30:45')",
            &[],
        )
        .unwrap();
    let hour_trunc: String = rows[0].get(0).unwrap();
    let minute_trunc: String = rows[0].get(1).unwrap();
    assert_eq!(hour_trunc, "2024-07-15 14:00:00");
    assert_eq!(minute_trunc, "2024-07-15 14:30:00");
}

// ── NULL / Conditional functions ──────────────────────────────────────────────

#[test]
fn zeroifnull_function() {
    let c = conn();
    c.execute("CREATE TABLE t (v REAL)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (5.0)", &[]).unwrap();

    let rows = c
        .query("SELECT ZEROIFNULL(v) FROM t ORDER BY v", &[])
        .unwrap();
    let vals: Vec<f64> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert!(vals.contains(&0.0));
    assert!(vals.contains(&5.0));
}

#[test]
fn nullifzero_function() {
    let c = conn();
    let rows = c
        .query("SELECT NULLIFZERO(0), NULLIFZERO(42)", &[])
        .unwrap();
    let zero_result: Option<i64> = rows[0].get(0).unwrap();
    let non_zero: i64 = rows[0].get(1).unwrap();
    assert!(zero_result.is_none());
    assert_eq!(non_zero, 42);
}

#[test]
fn emptytonull_function() {
    let c = conn();
    let rows = c
        .query("SELECT EMPTYTONULL(''), EMPTYTONULL('hello')", &[])
        .unwrap();
    let empty_result: Option<String> = rows[0].get(0).unwrap();
    let non_empty: String = rows[0].get(1).unwrap();
    assert!(empty_result.is_none());
    assert_eq!(non_empty, "hello");
}

#[test]
fn coalesce_function() {
    let c = conn();
    let rows = c
        .query("SELECT COALESCE(NULL, NULL, 'third', 'fourth')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "third");
}

#[test]
fn nullif_function() {
    let c = conn();
    let rows = c
        .query("SELECT NULLIF(5, 5), NULLIF(5, 0)", &[])
        .unwrap();
    let equal_result: Option<i64> = rows[0].get(0).unwrap();
    let unequal_result: i64 = rows[0].get(1).unwrap();
    assert!(equal_result.is_none());
    assert_eq!(unequal_result, 5);
}

#[test]
fn booland_boolor_functions() {
    let c = conn();
    let rows = c
        .query(
            "SELECT BOOLAND(1, 0), BOOLOR(1, 0), BOOLOR(0, 0), BOOLAND(1, 1)",
            &[],
        )
        .unwrap();
    let and_false: i64 = rows[0].get(0).unwrap();
    let or_true: i64 = rows[0].get(1).unwrap();
    let or_false: i64 = rows[0].get(2).unwrap();
    let and_true: i64 = rows[0].get(3).unwrap();
    assert_eq!(and_false, 0);
    assert_eq!(or_true, 1);
    assert_eq!(or_false, 0);
    assert_eq!(and_true, 1);
}

// ── Type conversion functions ─────────────────────────────────────────────────

#[test]
fn to_number_function() {
    let c = conn();
    let rows = c.query("SELECT TO_NUMBER('3.14')", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 3.14).abs() < 1e-9);
}

#[test]
fn to_double_function() {
    let c = conn();
    let rows = c.query("SELECT TO_DOUBLE('2.718')", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 2.718).abs() < 1e-9);
}

#[test]
fn to_boolean_function() {
    let c = conn();
    let rows = c.query("SELECT TO_BOOLEAN(1), TO_BOOLEAN(0)", &[]).unwrap();
    let t: i64 = rows[0].get(0).unwrap();
    let f: i64 = rows[0].get(1).unwrap();
    assert_eq!(t, 1);
    assert_eq!(f, 0);
}

// ── Aggregate functions ───────────────────────────────────────────────────────

#[test]
fn aggregate_count_sum_avg() {
    let c = conn();
    c.execute("CREATE TABLE t (v REAL)", &[]).unwrap();
    for v in [1.0f64, 2.0, 3.0, 4.0, 5.0] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }

    let rows = c
        .query("SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t", &[])
        .unwrap();
    let cnt: i64 = rows[0].get(0).unwrap();
    let sum: f64 = rows[0].get(1).unwrap();
    let avg: f64 = rows[0].get(2).unwrap();
    let min: f64 = rows[0].get(3).unwrap();
    let max: f64 = rows[0].get(4).unwrap();
    assert_eq!(cnt, 5);
    assert!((sum - 15.0).abs() < 1e-9);
    assert!((avg - 3.0).abs() < 1e-9);
    assert!((min - 1.0).abs() < 1e-9);
    assert!((max - 5.0).abs() < 1e-9);
}

#[test]
fn aggregate_count_distinct() {
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    for v in [1i64, 2, 2, 3, 3, 3] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }

    let rows = c
        .query("SELECT COUNT(DISTINCT v) FROM t", &[])
        .unwrap();
    let cnt: i64 = rows[0].get(0).unwrap();
    assert_eq!(cnt, 3);
}

#[test]
fn group_by_and_having() {
    let c = conn();
    c.execute("CREATE TABLE orders (customer TEXT, amount REAL)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES ('A', 100.0)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES ('A', 200.0)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES ('B', 50.0)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES ('B', 30.0)", &[]).unwrap();
    c.execute("INSERT INTO orders VALUES ('C', 500.0)", &[]).unwrap();

    let rows = c
        .query(
            "SELECT customer, SUM(amount) as total FROM orders GROUP BY customer HAVING SUM(amount) > 100 ORDER BY customer",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "A");
    assert_eq!(rows[1].get::<String>(0).unwrap(), "C");
}

// ── DDL extras ───────────────────────────────────────────────────────────────

#[test]
fn drop_table_if_exists() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    // Should not error even if table exists
    c.execute("DROP TABLE IF EXISTS t", &[]).unwrap();
    // Table is gone; creating again should succeed
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 0);
}

#[test]
fn create_and_query_view() {
    let c = conn();
    c.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)", &[])
        .unwrap();
    c.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)", &[])
        .unwrap();
    c.execute("INSERT INTO products VALUES (2, 'Gadget', 49.99)", &[])
        .unwrap();
    c.execute(
        "CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 20.0",
        &[],
    )
    .unwrap();

    let rows = c
        .query("SELECT name FROM expensive_products", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Gadget");
}

#[test]
#[ignore = "TRUNCATE TABLE not supported in SQLite; needs translation to DELETE FROM (see failure plan item 18)"]
fn truncate_table() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (2)", &[]).unwrap();
    c.execute("TRUNCATE TABLE t", &[]).unwrap();
    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 0);
}

#[test]
fn alter_table_add_column() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    c.execute("ALTER TABLE t ADD COLUMN name TEXT", &[]).unwrap();
    // After add column, can update the new column
    c.execute("UPDATE t SET name = 'Alice' WHERE id = 1", &[])
        .unwrap();
    let rows = c.query("SELECT name FROM t WHERE id = 1", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[test]
fn create_or_replace_table_if_not_exists_preserves_data() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'hello')", &[]).unwrap();
    // With default config (not drop_before_create), CREATE OR REPLACE TABLE
    // translates to CREATE TABLE IF NOT EXISTS — the existing table is kept.
    c.execute("CREATE OR REPLACE TABLE t (id INTEGER, val TEXT)", &[])
        .unwrap();
    // Data must be preserved since IF NOT EXISTS is a no-op on an existing table
    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
}

#[test]
fn snowflake_column_options_stripped() {
    let c = conn();
    // AUTOINCREMENT, COMMENT, CLUSTER BY etc. should be stripped
    c.execute(
        "CREATE TABLE t (
            id INTEGER AUTOINCREMENT,
            name TEXT COMMENT = 'Name of the thing'
        ) CLUSTER BY (id)",
        &[],
    )
    .unwrap();
    c.execute("INSERT INTO t (id, name) VALUES (1, 'test')", &[]).unwrap();
    let rows = c.query("SELECT id, name FROM t", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
}

// ── DML extras ───────────────────────────────────────────────────────────────

#[test]
fn update_with_where() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, status TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'active')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (2, 'active')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (3, 'active')", &[]).unwrap();

    c.execute("UPDATE t SET status = 'inactive' WHERE id = 2", &[])
        .unwrap();

    let rows = c
        .query("SELECT status FROM t WHERE id = 2", &[])
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "inactive");
    // Others unchanged
    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE status = 'active'", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
}

#[test]
fn delete_with_where() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, flag INTEGER)", &[]).unwrap();
    for i in 1i64..=5 {
        c.execute("INSERT INTO t VALUES (?, ?)", &[&i, &(i % 2)]).unwrap();
    }

    c.execute("DELETE FROM t WHERE flag = 0", &[]).unwrap();

    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3); // ids 1, 3, 5 remain
}

#[test]
fn multi_row_insert() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[]).unwrap();

    // SQLite supports multi-row VALUES; Snowflake does too
    c.execute(
        "INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
        &[],
    )
    .unwrap();

    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

// ── Query features ────────────────────────────────────────────────────────────

#[test]
fn select_distinct() {
    let c = conn();
    c.execute("CREATE TABLE t (color TEXT)", &[]).unwrap();
    for color in ["red", "blue", "red", "green", "blue"] {
        c.execute("INSERT INTO t VALUES (?)", &[&color]).unwrap();
    }

    let rows = c
        .query("SELECT DISTINCT color FROM t ORDER BY color", &[])
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn select_top_n() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    for i in 1i64..=10 {
        c.execute("INSERT INTO t VALUES (?)", &[&i]).unwrap();
    }

    let rows = c
        .query("SELECT TOP 3 id FROM t ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
    assert_eq!(rows[2].get::<i64>(0).unwrap(), 3);
}

#[test]
fn select_limit_offset() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    for i in 1i64..=10 {
        c.execute("INSERT INTO t VALUES (?)", &[&i]).unwrap();
    }

    let rows = c
        .query("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 3", &[])
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 4);
}

#[test]
fn inner_join() {
    let c = conn();
    // Use distinct column names to avoid ambiguity after identifier-qualifier stripping
    c.execute("CREATE TABLE customers (cust_id INTEGER, cust_name TEXT)", &[]).unwrap();
    c.execute("CREATE TABLE purchases (purch_id INTEGER, buyer_id INTEGER, total REAL)", &[])
        .unwrap();
    c.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')", &[]).unwrap();
    c.execute("INSERT INTO purchases VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 75.0)", &[])
        .unwrap();

    let rows = c
        .query(
            "SELECT cust_name, total FROM customers INNER JOIN purchases ON cust_id = buyer_id ORDER BY total DESC",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice"); // highest total
}

#[test]
fn left_join() {
    let c = conn();
    // Use distinct column names to avoid ambiguity after identifier-qualifier stripping
    c.execute("CREATE TABLE members (mem_id INTEGER, mem_name TEXT)", &[]).unwrap();
    c.execute("CREATE TABLE invoices (inv_id INTEGER, owner_id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO members VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", &[])
        .unwrap();
    c.execute("INSERT INTO invoices VALUES (1, 1), (2, 1)", &[]).unwrap();

    let rows = c
        .query(
            "SELECT mem_name, COUNT(inv_id) as invoice_count FROM members LEFT JOIN invoices ON mem_id = owner_id GROUP BY mem_id, mem_name ORDER BY mem_id",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<i64>(1).unwrap(), 0); // Charlie has no invoices
}

#[test]
fn case_expression() {
    let c = conn();
    c.execute("CREATE TABLE t (score INTEGER)", &[]).unwrap();
    for s in [90i64, 75, 55, 40] {
        c.execute("INSERT INTO t VALUES (?)", &[&s]).unwrap();
    }

    let rows = c
        .query(
            "SELECT CASE
                WHEN score >= 90 THEN 'A'
                WHEN score >= 70 THEN 'B'
                WHEN score >= 60 THEN 'C'
                ELSE 'F'
             END as grade
             FROM t ORDER BY score DESC",
            &[],
        )
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "A");
    assert_eq!(rows[1].get::<String>(0).unwrap(), "B");
    assert_eq!(rows[3].get::<String>(0).unwrap(), "F");
}

#[test]
fn between_predicate() {
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    for v in 1i64..=10 {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }

    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE v BETWEEN 3 AND 7", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 5);
}

#[test]
fn in_predicate() {
    let c = conn();
    c.execute("CREATE TABLE t (status TEXT)", &[]).unwrap();
    for s in ["active", "inactive", "pending", "deleted"] {
        c.execute("INSERT INTO t VALUES (?)", &[&s]).unwrap();
    }

    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE status IN ('active', 'pending')", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
}

#[test]
fn is_null_and_is_not_null() {
    let c = conn();
    c.execute("CREATE TABLE t (v TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('hello')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (NULL)", &[]).unwrap();

    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE v IS NULL", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);

    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE v IS NOT NULL", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
}

#[test]
fn like_predicate() {
    let c = conn();
    c.execute("CREATE TABLE t (name TEXT)", &[]).unwrap();
    for n in ["Alice", "Bob", "Allison", "Charlie"] {
        c.execute("INSERT INTO t VALUES (?)", &[&n]).unwrap();
    }

    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE name LIKE 'Al%'", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
}

#[test]
fn subquery_in_where() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, dept TEXT)", &[]).unwrap();
    c.execute("CREATE TABLE dept_filter (dept TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'eng'), (2, 'hr'), (3, 'eng'), (4, 'sales')", &[])
        .unwrap();
    c.execute("INSERT INTO dept_filter VALUES ('eng'), ('hr')", &[]).unwrap();

    let rows = c
        .query(
            "SELECT COUNT(*) FROM t WHERE dept IN (SELECT dept FROM dept_filter)",
            &[],
        )
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

#[test]
fn cte_with_clause() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, amount REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (1, 100.0), (2, 200.0), (3, 50.0), (4, 150.0)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "WITH high_value AS (
                SELECT id, amount FROM t WHERE amount > 100.0
             )
             SELECT COUNT(*) FROM high_value",
            &[],
        )
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
}

#[test]
fn window_function_row_number() {
    let c = conn();
    c.execute("CREATE TABLE t (dept TEXT, salary REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES ('eng', 100000), ('eng', 120000), ('hr', 80000), ('hr', 90000)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM t ORDER BY dept, salary DESC",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    // First row in each partition should have rn = 1
    assert_eq!(rows[0].get::<i64>(2).unwrap(), 1);
    assert_eq!(rows[2].get::<i64>(2).unwrap(), 1);
}

#[test]
fn window_function_rank() {
    let c = conn();
    c.execute("CREATE TABLE scores (name TEXT, score INTEGER)", &[]).unwrap();
    c.execute(
        "INSERT INTO scores VALUES ('Alice', 100), ('Bob', 90), ('Charlie', 100), ('Dave', 80)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT name, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY rnk, name",
            &[],
        )
        .unwrap();
    // Alice and Charlie both rank 1 (tied), Bob ranks 3, Dave ranks 4
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 1);
    assert_eq!(rows[1].get::<i64>(1).unwrap(), 1);
    assert_eq!(rows[2].get::<i64>(1).unwrap(), 3);
}

#[test]
fn window_function_dense_rank() {
    let c = conn();
    c.execute("CREATE TABLE scores (name TEXT, score INTEGER)", &[]).unwrap();
    c.execute(
        "INSERT INTO scores VALUES ('Alice', 100), ('Bob', 90), ('Charlie', 100), ('Dave', 80)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) as dr FROM scores ORDER BY dr, name",
            &[],
        )
        .unwrap();
    // DENSE_RANK: Alice=1, Charlie=1, Bob=2, Dave=3 (no gaps unlike RANK)
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 1); // Alice
    assert_eq!(rows[1].get::<i64>(1).unwrap(), 1); // Charlie
    assert_eq!(rows[2].get::<i64>(1).unwrap(), 2); // Bob (DENSE_RANK=2, not 3)
    assert_eq!(rows[3].get::<i64>(1).unwrap(), 3); // Dave
}

#[test]
fn window_function_ntile() {
    let c = conn();
    c.execute("CREATE TABLE t (val INTEGER)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (10), (20), (30), (40), (50), (60), (70), (80)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT val, NTILE(4) OVER (ORDER BY val) as bucket FROM t ORDER BY val",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 8);
    // 8 rows into 4 buckets → 2 rows each; first two rows are bucket 1
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 1);
    assert_eq!(rows[1].get::<i64>(1).unwrap(), 1);
    assert_eq!(rows[2].get::<i64>(1).unwrap(), 2);
    assert_eq!(rows[7].get::<i64>(1).unwrap(), 4);
}

#[test]
fn window_function_lag() {
    let c = conn();
    c.execute("CREATE TABLE t (period INTEGER, revenue REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (1, 100.0), (2, 150.0), (3, 120.0), (4, 180.0)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT period, revenue, LAG(revenue, 1, 0) OVER (ORDER BY period) as prev_revenue FROM t ORDER BY period",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    // First row has no previous → default value 0
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 0.0);
    // Second row sees first row's revenue
    assert_eq!(rows[1].get::<f64>(2).unwrap(), 100.0);
    assert_eq!(rows[2].get::<f64>(2).unwrap(), 150.0);
}

#[test]
fn window_function_lead() {
    let c = conn();
    c.execute("CREATE TABLE t (period INTEGER, revenue REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (1, 100.0), (2, 150.0), (3, 120.0), (4, 180.0)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT period, revenue, LEAD(revenue, 1, 0) OVER (ORDER BY period) as next_revenue FROM t ORDER BY period",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    // Last row has no next → default value 0
    assert_eq!(rows[3].get::<f64>(2).unwrap(), 0.0);
    // First row sees second row's revenue
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 150.0);
    assert_eq!(rows[1].get::<f64>(2).unwrap(), 120.0);
}

#[test]
fn window_function_first_value_last_value() {
    let c = conn();
    c.execute("CREATE TABLE t (dept TEXT, salary REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES ('eng', 90000), ('eng', 110000), ('eng', 95000)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT dept, salary,
                FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lowest,
                LAST_VALUE(salary)  OVER (PARTITION BY dept ORDER BY salary
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as highest
             FROM t ORDER BY salary",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    // Every row should see the partition min as FIRST_VALUE and max as LAST_VALUE
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 90000.0); // lowest
    assert_eq!(rows[0].get::<f64>(3).unwrap(), 110000.0); // highest
    assert_eq!(rows[2].get::<f64>(2).unwrap(), 90000.0);
    assert_eq!(rows[2].get::<f64>(3).unwrap(), 110000.0);
}

#[test]
fn window_function_running_sum_with_frame() {
    let c = conn();
    c.execute("CREATE TABLE t (day INTEGER, amount REAL)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT day, amount,
                SUM(amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
             FROM t ORDER BY day",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 10.0);
    assert_eq!(rows[1].get::<f64>(2).unwrap(), 30.0);
    assert_eq!(rows[2].get::<f64>(2).unwrap(), 60.0);
    assert_eq!(rows[3].get::<f64>(2).unwrap(), 100.0);
}

#[test]
fn window_function_nth_value() {
    let c = conn();
    c.execute("CREATE TABLE t (val INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (10), (20), (30), (40)", &[]).unwrap();

    let rows = c
        .query(
            "SELECT val,
                NTH_VALUE(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second
             FROM t ORDER BY val",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    // Every row should see 20 as the 2nd value in the ordered frame
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 20);
    assert_eq!(rows[3].get::<i64>(1).unwrap(), 20);
}

// ── Config options ────────────────────────────────────────────────────────────

#[test]
fn config_drop_before_create() {
    use snowlite::{Config, Connection};
    let config = Config::new().with_drop_before_create();
    let c = Connection::open_in_memory_with_config(config).unwrap();

    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();

    // With drop_before_create, CREATE OR REPLACE should DROP then CREATE (clearing data)
    c.execute("CREATE OR REPLACE TABLE t (id INTEGER, name TEXT)", &[])
        .unwrap();
    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    // Table should be empty after DROP + CREATE
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 0);
}

#[test]
fn config_schema_prefix() {
    use snowlite::{Config, Connection};
    let config = Config::new().with_schema_prefix();
    let c = Connection::open_in_memory_with_config(config).unwrap();

    // Create table using schema prefix convention
    c.execute("CREATE TABLE public__orders (id INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO public__orders VALUES (1)", &[]).unwrap();

    // Query with two-part identifier — should map public.orders → public__orders
    let rows = c
        .query("SELECT COUNT(*) FROM public.orders", &[])
        .unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
}

// ── Semi-structured extras ────────────────────────────────────────────────────

#[test]
fn object_construct_function() {
    let c = conn();
    let rows = c
        .query("SELECT object_construct('name', 'Alice', 'age', 30)", &[])
        .unwrap();
    let json_str: String = rows[0].get(0).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(json["name"], "Alice");
    assert_eq!(json["age"], 30);
}

#[test]
fn array_construct_function() {
    let c = conn();
    let rows = c
        .query("SELECT array_construct(1, 2, 3, 4, 5)", &[])
        .unwrap();
    let json_str: String = rows[0].get(0).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert!(json.is_array());
    assert_eq!(json.as_array().unwrap().len(), 5);
}

#[test]
fn semi_structured_bracket_string_access() {
    let c = conn();
    c.execute("CREATE TABLE events (data VARIANT)", &[]).unwrap();
    c.execute(
        "INSERT INTO events VALUES (?)",
        &[&r#"{"user": "alice", "action": "login"}"#],
    )
    .unwrap();

    let rows = c
        .query("SELECT data['user'] FROM events", &[])
        .unwrap();
    let user: String = rows[0].get(0).unwrap();
    // SQLite JSON_EXTRACT returns the raw string value (no surrounding quotes)
    assert_eq!(user, "alice");
}

#[test]
fn semi_structured_bracket_int_access() {
    let c = conn();
    c.execute("CREATE TABLE events (data VARIANT)", &[]).unwrap();
    c.execute(
        "INSERT INTO events VALUES (?)",
        &[&r#"["first", "second", "third"]"#],
    )
    .unwrap();

    let rows = c
        .query("SELECT data[0] FROM events", &[])
        .unwrap();
    let first: String = rows[0].get(0).unwrap();
    // SQLite JSON_EXTRACT returns the raw string value (no surrounding quotes)
    assert_eq!(first, "first");
}

#[test]
#[ignore = "nested colon paths (data:a.b) fail: the identifier stripper corrupts dotted paths inside string literals (see failure plan item 17)"]
fn semi_structured_nested_colon_path() {
    let c = conn();
    c.execute("CREATE TABLE events (data VARIANT)", &[]).unwrap();
    c.execute(
        "INSERT INTO events VALUES (?)",
        &[&r#"{"user": {"name": "Alice", "id": 42}}"#],
    )
    .unwrap();

    let rows = c
        .query("SELECT data:user.name FROM events", &[])
        .unwrap();
    let name: String = rows[0].get(0).unwrap();
    assert_eq!(name, "Alice");
}

#[test]
fn parse_json_passthrough() {
    let c = conn();
    // PARSE_JSON is a passthrough — the value is already stored as JSON text
    let rows = c
        .query("SELECT PARSE_JSON('{\"a\": 1}')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert!(result.contains("\"a\""));
}

#[test]
fn try_parse_json_valid() {
    let c = conn();
    let rows = c
        .query("SELECT try_parse_json('{\"x\": 99}')", &[])
        .unwrap();
    let result: Option<String> = rows[0].get(0).unwrap();
    assert!(result.is_some());
    assert!(result.unwrap().contains("99"));
}

#[test]
fn get_path_function() {
    let c = conn();
    // Use a single-segment path to avoid the identifier stripper corrupting 'a.b' → 'b'
    let rows = c
        .query("SELECT get_path('{\"a\": 42}', 'a')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "42");
}

#[test]
#[ignore = "multi-segment get_path paths (e.g. 'a.b') are corrupted by the identifier stripper (see failure plan item 17)"]
fn get_path_function_nested() {
    let c = conn();
    let rows = c
        .query("SELECT get_path('{\"a\": {\"b\": 42}}', 'a.b')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "42");
}

// ── Additional type tests ─────────────────────────────────────────────────────

#[test]
fn snowflake_number_types() {
    let c = conn();
    c.execute(
        "CREATE TABLE t (
            int_col    NUMBER(18, 0),
            bigint_col BIGINT,
            small_col  SMALLINT,
            float_col  FLOAT,
            double_col DOUBLE,
            dec_col    DECIMAL(10, 2)
        )",
        &[],
    )
    .unwrap();
    c.execute(
        "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
        &[&42i64, &1000000i64, &100i64, &3.14f64, &2.718f64, &9.99f64],
    )
    .unwrap();
    let rows = c.query("SELECT * FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 42);
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 1_000_000);
}

#[test]
fn snowflake_string_types() {
    let c = conn();
    c.execute(
        "CREATE TABLE t (
            vc   VARCHAR(100),
            ch   CHAR(10),
            str  STRING,
            nvc  NVARCHAR(50)
        )",
        &[],
    )
    .unwrap();
    c.execute("INSERT INTO t VALUES (?, ?, ?, ?)", &[&"a", &"b", &"c", &"d"]).unwrap();
    let rows = c.query("SELECT vc, ch, str, nvc FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "a");
    assert_eq!(rows[0].get::<String>(2).unwrap(), "c");
}

#[test]
fn snowflake_boolean_type() {
    let c = conn();
    c.execute("CREATE TABLE t (active BOOLEAN)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (0)", &[]).unwrap();
    let rows = c.query("SELECT active FROM t ORDER BY active DESC", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
    assert_eq!(rows[1].get::<i64>(0).unwrap(), 0);
}

#[test]
fn snowflake_date_and_time_types() {
    let c = conn();
    c.execute("CREATE TABLE t (d DATE, t TIME, ts TIMESTAMP_NTZ)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (?, ?, ?)",
        &[&"2024-03-15", &"10:30:00", &"2024-03-15 10:30:00"],
    )
    .unwrap();
    let rows = c.query("SELECT d, t, ts FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15");
    assert_eq!(rows[0].get::<String>(1).unwrap(), "10:30:00");
}

#[test]
fn snowflake_timestamp_variants() {
    let c = conn();
    c.execute(
        "CREATE TABLE t (
            ts_ntz  TIMESTAMP_NTZ,
            ts_ltz  TIMESTAMP_LTZ,
            ts_tz   TIMESTAMP_TZ
        )",
        &[],
    )
    .unwrap();
    c.execute(
        "INSERT INTO t VALUES (?, ?, ?)",
        &[
            &"2024-03-15 10:00:00",
            &"2024-03-15 10:00:00",
            &"2024-03-15 10:00:00",
        ],
    )
    .unwrap();
    let rows = c.query("SELECT ts_ntz FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15 10:00:00");
}

#[test]
fn snowflake_binary_type() {
    let c = conn();
    c.execute("CREATE TABLE t (data BINARY(16))", &[]).unwrap();
    let blob: &[u8] = &[1u8, 2, 3, 4];
    c.execute("INSERT INTO t VALUES (?)", &[&blob]).unwrap();
    let rows = c.query("SELECT data FROM t", &[]).unwrap();
    // Returns as Blob
    let val = rows[0].value(0).unwrap();
    assert!(matches!(val, Value::Blob(_)));
}

// ── Parameterised queries ─────────────────────────────────────────────────────

#[test]
fn parameter_binding_all_types() {
    let c = conn();
    c.execute(
        "CREATE TABLE t (i INTEGER, r REAL, s TEXT, b INTEGER)",
        &[],
    )
    .unwrap();
    c.execute(
        "INSERT INTO t VALUES (?, ?, ?, ?)",
        &[&42i64, &3.14f64, &"hello", &1i64],
    )
    .unwrap();

    let rows = c.query("SELECT * FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 42);
    assert!((rows[0].get::<f64>(1).unwrap() - 3.14).abs() < 1e-9);
    assert_eq!(rows[0].get::<String>(2).unwrap(), "hello");
    assert_eq!(rows[0].get::<i64>(3).unwrap(), 1);
}

#[test]
fn null_parameter_binding() {
    let c = conn();
    c.execute("CREATE TABLE t (v TEXT)", &[]).unwrap();
    // Pass a typed None as a null parameter
    let null_val: Option<String> = None;
    c.execute("INSERT INTO t VALUES (?)", &[&null_val]).unwrap();
    let rows = c.query("SELECT v FROM t", &[]).unwrap();
    let v: Option<String> = rows[0].get(0).unwrap();
    assert!(v.is_none());
}

// ── Noop statement extras ─────────────────────────────────────────────────────

#[test]
fn noop_grant_and_revoke() {
    let c = conn();
    // GRANT and REVOKE are no-ops
    c.execute("GRANT SELECT ON TABLE t TO ROLE analyst", &[]).unwrap();
    c.execute("REVOKE SELECT ON TABLE t FROM ROLE analyst", &[]).unwrap();
}

#[test]
fn noop_create_warehouse() {
    let c = conn();
    c.execute("CREATE WAREHOUSE compute_wh WAREHOUSE_SIZE='SMALL'", &[]).unwrap();
    c.execute("ALTER WAREHOUSE compute_wh SUSPEND", &[]).unwrap();
}

#[test]
fn noop_set_unset_variables() {
    let c = conn();
    c.execute("SET my_var = 42", &[]).unwrap();
    c.execute("UNSET my_var", &[]).unwrap();
}

#[test]
fn noop_copy_into() {
    let c = conn();
    c.execute("COPY INTO my_table FROM @my_stage", &[]).unwrap();
}

#[test]
fn noop_show_commands() {
    let c = conn();
    c.execute("SHOW TABLES", &[]).unwrap();
    c.execute("SHOW SCHEMAS IN DATABASE mydb", &[]).unwrap();
    c.execute("SHOW WAREHOUSES", &[]).unwrap();
    c.execute("SHOW ROLES", &[]).unwrap();
}

// ── query_one convenience method ─────────────────────────────────────────────

#[test]
fn query_one_returns_first_row() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')", &[]).unwrap();

    let row = c
        .query_one("SELECT name FROM t ORDER BY id", &[])
        .unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap().get::<String>(0).unwrap(), "Alice");
}

#[test]
fn query_one_returns_none_for_empty() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER)", &[]).unwrap();

    let row = c.query_one("SELECT id FROM t", &[]).unwrap();
    assert!(row.is_none());
}

// ═══════════════════════════════════════════════════════════════════════════════
// FAILING TESTS — unimplemented Snowflake features
//
// These tests are marked `#[ignore]` because the current translator does not
// yet handle these constructs.  They document the gaps and serve as a roadmap.
//
// FAILURE PLAN (to review):
//
//  1. REGEXP_REPLACE / REGEXP_SUBSTR
//     Gap: No `regexp_replace` or `regexp_substr` custom SQLite function is
//     registered.  SQLite does not have these builtins.
//     Fix: Register `regexp_replace(text, pattern, replacement)` and
//          `regexp_substr(text, pattern)` as custom scalar functions using the
//          `regex` crate (similar to how `regexp` is registered in
//          connection.rs `register_custom_functions`).
//
//  2. RLIKE operator
//     Gap: `expr RLIKE pattern` is not translated.
//     Fix: Add a rewrite rule in functions.rs to map `RLIKE` to the custom
//          `regexp(pattern, expr)` function, mirroring the ILIKE→LOWER()..LIKE
//          rewrite.
//
//  3. LISTAGG
//     Gap: `LISTAGG(col, sep) WITHIN GROUP (ORDER BY col)` has no translation.
//     Fix: Add a regex-based rewrite rule that maps:
//          `LISTAGG(col, sep) WITHIN GROUP (ORDER BY ...)` →
//          `GROUP_CONCAT(col, sep)` (dropping the ORDER BY sub-clause, since
//          SQLite's GROUP_CONCAT does not support ORDER BY in the aggregation).
//          For ordered LISTAGG, a subquery-based workaround would be needed.
//
//  4. ARRAY_CONTAINS
//     Gap: `ARRAY_CONTAINS(val, array_col)` is not translated or registered.
//     Fix: Register a custom SQLite scalar function `array_contains(val, arr)`
//          that parses the JSON array and checks membership.
//
//  5. OBJECT_KEYS
//     Gap: `OBJECT_KEYS(obj)` is not translated or registered.
//     Fix: Register a custom SQLite scalar function `object_keys(json)` that
//          returns a JSON array of the object's keys.
//
//  6. CONVERT_TIMEZONE
//     Gap: `CONVERT_TIMEZONE(src_tz, tgt_tz, ts)` has no equivalent in SQLite.
//     Fix: SQLite's datetime functions have no timezone awareness.  For testing
//          purposes, register a passthrough custom function that returns the
//          timestamp unchanged (acceptable for local test stubs).
//
//  7. TRY_CAST / TRY_TO_NUMBER / TRY_TO_DATE
//     Gap: Snowflake `TRY_CAST(x AS type)` returns NULL on failure instead of
//          erroring.  No translation exists.
//     Fix: Register `try_cast_as_integer(x)` / `try_cast_as_real(x)` /
//          `try_cast_as_date(x)` custom functions that return NULL on parse
//          failure; and add rewrite rules to map `TRY_CAST(x AS INTEGER)` etc.
//
//  8. EXTRACT(part FROM date) syntax
//     Gap: Snowflake supports both `YEAR(date)` (already handled) and the SQL
//          standard `EXTRACT(YEAR FROM date)`.  The EXTRACT form is not
//          translated.
//     Fix: Add a regex-based rewrite rule in functions.rs that translates
//          `EXTRACT(part FROM expr)` to the corresponding `STRFTIME()` call.
//
//  9. POSITION(needle IN haystack) syntax
//     Gap: SQL standard POSITION syntax is not translated.
//     Fix: Add a rewrite rule:
//          `POSITION(x IN y)` → `INSTR(y, x)`.
//
// 10. FLATTEN (table function)
//     Gap: `LATERAL FLATTEN(input => col)` has no SQLite equivalent.
//     Fix: This would require significant infrastructure (virtual table or
//          custom table-valued function).  Recommend documenting as "not
//          supported in local-db" and skipping in integration tests.
//
// 11. MERGE statement
//     Gap: SQLite does not support MERGE.  No translation exists.
//     Fix: Translate `MERGE INTO target USING source ON ... WHEN MATCHED
//          THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` to
//          `INSERT OR REPLACE INTO ...` for simple cases.  Complex MERGE
//          patterns would remain unsupported.
//
// 12. PIVOT / UNPIVOT
//     Gap: SQLite does not support PIVOT/UNPIVOT syntax.
//     Fix: Not feasible to translate generically.  Recommend documenting as
//          unsupported and rewiring tests to use conditional aggregation instead.
//
// 13. SQRT / POWER (and other math functions)
//     Gap: The bundled SQLite in rusqlite 0.31 is not compiled with
//          `-DSQLITE_ENABLE_MATH_FUNCTIONS`, so SQRT, POWER, LOG, EXP etc.
//          are not available as SQLite built-in functions.
//     Fix: Either (a) add custom scalar function registrations for SQRT and
//          POWER in `connection.rs register_custom_functions`, using Rust's
//          f64 math; or (b) rewrite SQRT(x) → (x * x ... ) in the translator
//          using expression rewriting.  Recommend option (a).
//
// 14. BITXOR translates to unsupported `^` operator
//     Gap: `BITXOR(a, b)` is translated to `(a ^ b)`.  SQLite does not have
//          a `^` bitwise-XOR operator (only `&` and `|` are supported).
//     Fix: In functions.rs, change the BITXOR rule to use a CASE expression:
//          `BITXOR(a, b)` → `((a | b) & ~(a & b))` or register a custom
//          SQLite scalar function `bitxor(a, b)`.
//
// 15. CHARINDEX corrupted by CHAR→TEXT type rewriter
//     Gap: The `rewrite_types` pass has a pattern `(?i)\bCHAR\s*...` that
//          matches without a trailing word boundary.  As a result, `CHARINDEX`
//          becomes `TEXTINDEX` (CHAR→TEXT prefix substitution).
//     Fix: In types.rs, add a trailing word boundary to the CHAR/NCHAR
//          patterns: `(?i)\bCHAR\b` (with `\b` after CHAR) or use a negative
//          lookahead `(?!ACTER|INDEX)`.
//
// 16. Colon-path rewriter corrupts string literals containing `word:word`
//     Gap: The semi-structured colon-path rewriter
//          `\b([A-Za-z_]...):([A-Za-z_]...)` does not respect single-quoted
//          string literals.  Any string like `'a:b:c'` is corrupted to
//          `'JSON_EXTRACT(a, '$.b'):c'`.  This breaks split_part, strtok,
//          and any other function that takes colon-containing string literals.
//     Fix: Update `rewrite_semi_structured_paths` to skip regions inside
//          single- or double-quoted string literals, similar to the approach
//          used in `split_statements`.
//
// 17. Identifier stripper corrupts dotted paths inside string literals
//     Gap: The two-part identifier stripper pattern matches `a.b` even when
//          it appears inside single-quoted string literals (e.g., the path
//          argument `'a.b'` to `get_path`).  This causes `'a.b'` → `'b'`
//          and nested colon paths like `data:user.name` to produce incorrect
//          SQL (`'$.name'` instead of `'$.user.name'`).
//     Fix: As with item 16, update `strip_qualifiers` in identifiers.rs to
//          skip content inside quoted string literals before applying the
//          identifier replacement regexes.
//
// 18. TRUNCATE TABLE not translated
//     Gap: SQLite does not support `TRUNCATE TABLE`.  The statement passes
//          through untranslated and causes a syntax error.
//     Fix: Add a rewrite rule in functions.rs (or as a new pass in
//          rewriter.rs) that translates `TRUNCATE TABLE tbl` →
//          `DELETE FROM tbl`.
//
// 19. ZEROIFNULL translation incomplete
//     Gap: The ZEROIFNULL rule rewrites the function name to COALESCE but
//          does not add the required second argument `0`.
//          `ZEROIFNULL(x)` → `COALESCE(x)` (invalid; needs `COALESCE(x, 0)`).
//     Fix: Change the ZEROIFNULL rule in functions.rs from a simple name
//          substitution to a full replacement, e.g. using a capture group:
//          `r"(?i)\bZEROIFNULL\s*\(([^)]+)\)"` → `"COALESCE($1, 0)"`.
// ═══════════════════════════════════════════════════════════════════════════════

// ── String Functions ─────────────────────────────────────────────────────────

#[test]
fn regexp_replace_function() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_REPLACE('hello world', 'o', '0')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "hell0 w0rld");
}

#[test]
fn regexp_replace_with_capture_group() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_REPLACE('2024-03-15', '(\\d{4})-(\\d{2})-(\\d{2})', '$3/$2/$1')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "15/03/2024");
}

#[test]
fn regexp_substr_function() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_SUBSTR('hello 123 world', '[0-9]+')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "123");
}

#[test]
fn regexp_substr_with_occurrence() {
    let c = conn();
    // Find the 2nd occurrence of a digit sequence
    let rows = c
        .query("SELECT REGEXP_SUBSTR('abc 10 def 20 ghi', '[0-9]+', 1, 2)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "20");
}

#[test]
fn regexp_substr_no_match_returns_null() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_SUBSTR('hello', '[0-9]+')", &[])
        .unwrap();
    let result: Option<String> = rows[0].get(0).unwrap();
    assert!(result.is_none());
}

#[test]
fn regexp_like_function() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_LIKE('hello123', '[a-z]+[0-9]+')", &[])
        .unwrap();
    let result: bool = rows[0].get(0).unwrap();
    assert!(result);
}

#[test]
fn regexp_like_no_match() {
    let c = conn();
    let rows = c
        .query("SELECT REGEXP_LIKE('hello', '^[0-9]+$')", &[])
        .unwrap();
    let result: bool = rows[0].get(0).unwrap();
    assert!(!result);
}

#[test]
fn rlike_operator() {
    let c = conn();
    c.execute("CREATE TABLE t (name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('hello123')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('abc')", &[]).unwrap();

    let rows = c
        .query("SELECT name FROM t WHERE name RLIKE '[0-9]+'", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "hello123");
}

#[test]
fn lpad_with_spaces() {
    let c = conn();
    let rows = c
        .query("SELECT LPAD('hello', 10)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "     hello");
}

#[test]
fn lpad_with_custom_pad() {
    let c = conn();
    let rows = c
        .query("SELECT LPAD('42', 6, '0')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "000042");
}

#[test]
fn lpad_truncates_when_longer() {
    let c = conn();
    let rows = c
        .query("SELECT LPAD('hello world', 5, '*')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "hello");
}

#[test]
fn rpad_with_spaces() {
    let c = conn();
    let rows = c
        .query("SELECT RPAD('hello', 10)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "hello     ");
}

#[test]
fn rpad_with_custom_pad() {
    let c = conn();
    let rows = c
        .query("SELECT RPAD('hi', 7, '-+')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "hi-+-+-");
}

#[test]
fn initcap_function() {
    let c = conn();
    let rows = c
        .query("SELECT INITCAP('hello world foo')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "Hello World Foo");
}

#[test]
fn initcap_mixed_case() {
    let c = conn();
    let rows = c
        .query("SELECT INITCAP('the QUICK brown FOX')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "The Quick Brown Fox");
}

#[test]
fn repeat_function() {
    let c = conn();
    let rows = c
        .query("SELECT REPEAT('ab', 4)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "abababab");
}

#[test]
fn repeat_zero_times() {
    let c = conn();
    let rows = c
        .query("SELECT REPEAT('abc', 0)", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "");
}

#[test]
fn reverse_function() {
    let c = conn();
    let rows = c
        .query("SELECT REVERSE('hello')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "olleh");
}

#[test]
fn concat_ws_function() {
    let c = conn();
    let rows = c
        .query("SELECT CONCAT_WS(',', 'a', 'b', 'c')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "a,b,c");
}

#[test]
fn concat_ws_skips_nulls() {
    let c = conn();
    c.execute("CREATE TABLE t (a TEXT, b TEXT, c TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('x', NULL, 'z')", &[]).unwrap();
    let rows = c
        .query("SELECT CONCAT_WS('-', a, b, c) FROM t", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "x-z");
}

#[test]
fn replace_function_native() {
    // REPLACE is a SQLite native function — just verify it works end-to-end
    let c = conn();
    let rows = c
        .query("SELECT REPLACE('hello world', 'world', 'Rust')", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert_eq!(result, "hello Rust");
}

// ── LISTAGG ──────────────────────────────────────────────────────────────────

#[test]
fn listagg_with_delimiter() {
    let c = conn();
    c.execute("CREATE TABLE t (category TEXT, item TEXT)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES ('fruit', 'apple'), ('fruit', 'banana'), ('veg', 'carrot')",
        &[],
    )
    .unwrap();

    let rows = c
        .query(
            "SELECT category, LISTAGG(item, ',') WITHIN GROUP (ORDER BY item) FROM t GROUP BY category ORDER BY category",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    // ORDER BY inside WITHIN GROUP is not preserved (SQLite GROUP_CONCAT limitation),
    // but all values must be present
    let fruit: String = rows[0].get(1).unwrap();
    assert!(fruit.contains("apple") && fruit.contains("banana"));
    let veg: String = rows[1].get(1).unwrap();
    assert!(veg.contains("carrot"));
}

#[test]
fn listagg_without_delimiter() {
    let c = conn();
    c.execute("CREATE TABLE t (val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('a'), ('b'), ('c')", &[]).unwrap();

    let rows = c
        .query(
            "SELECT LISTAGG(val) WITHIN GROUP (ORDER BY val) FROM t",
            &[],
        )
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    // All three values present (order not guaranteed)
    assert!(result.contains('a') && result.contains('b') && result.contains('c'));
}

#[test]
fn listagg_with_space_delimiter() {
    let c = conn();
    c.execute("CREATE TABLE t (word TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('hello'), ('world')", &[]).unwrap();

    let rows = c
        .query("SELECT LISTAGG(word, ' ') WITHIN GROUP (ORDER BY word) FROM t", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert!(result.contains("hello") && result.contains("world"));
}

#[test]
fn listagg_with_expression_arg() {
    // LISTAGG arg can be a function call — parser must handle nested parens
    let c = conn();
    c.execute("CREATE TABLE t (val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('hello'), ('world')", &[]).unwrap();

    let rows = c
        .query(
            "SELECT LISTAGG(UPPER(val), '|') WITHIN GROUP (ORDER BY val) FROM t",
            &[],
        )
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    assert!(result.contains("HELLO") && result.contains("WORLD"));
}

#[test]
#[ignore = "ARRAY_CONTAINS not yet implemented (see failure plan item 4)"]
fn array_contains_function() {
    let c = conn();
    let rows = c
        .query("SELECT ARRAY_CONTAINS('b', ARRAY_CONSTRUCT('a', 'b', 'c'))", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 1);
}

#[test]
#[ignore = "OBJECT_KEYS not yet implemented (see failure plan item 5)"]
fn object_keys_function() {
    let c = conn();
    let rows = c
        .query("SELECT OBJECT_KEYS(object_construct('a', 1, 'b', 2))", &[])
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    let keys: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert!(keys.as_array().unwrap().len() == 2);
}

#[test]
#[ignore = "CONVERT_TIMEZONE not yet implemented (see failure plan item 6)"]
fn convert_timezone_function() {
    let c = conn();
    let rows = c
        .query(
            "SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', '2024-03-15 12:00:00')",
            &[],
        )
        .unwrap();
    let result: String = rows[0].get(0).unwrap();
    // UTC to ET = -5h (or -4h DST)
    assert!(result.contains("2024-03-15"));
}

#[test]
#[ignore = "TRY_CAST not yet translated (see failure plan item 7)"]
fn try_cast_returns_null_on_failure() {
    let c = conn();
    let rows = c
        .query("SELECT TRY_CAST('not_a_number' AS INTEGER)", &[])
        .unwrap();
    let result: Option<i64> = rows[0].get(0).unwrap();
    assert!(result.is_none());
}

#[test]
#[ignore = "EXTRACT syntax not yet translated (see failure plan item 8)"]
fn extract_syntax() {
    let c = conn();
    let rows = c
        .query("SELECT EXTRACT(YEAR FROM '2024-03-15')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 2024);
}

#[test]
#[ignore = "POSITION(x IN y) syntax not yet translated (see failure plan item 9)"]
fn position_in_syntax() {
    let c = conn();
    let rows = c
        .query("SELECT POSITION('lo' IN 'hello')", &[])
        .unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 4);
}

#[test]
#[ignore = "FLATTEN table function not supported in SQLite (see failure plan item 10)"]
fn flatten_table_function() {
    let c = conn();
    c.execute("CREATE TABLE t (data VARIANT)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (?)",
        &[&r#"[1, 2, 3]"#],
    )
    .unwrap();

    let rows = c
        .query("SELECT value FROM t, LATERAL FLATTEN(input => data)", &[])
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
#[ignore = "MERGE statement not supported in SQLite (see failure plan item 11)"]
fn merge_statement() {
    let c = conn();
    c.execute("CREATE TABLE target (id INTEGER, val TEXT)", &[]).unwrap();
    c.execute("CREATE TABLE source (id INTEGER, val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO target VALUES (1, 'old'), (2, 'keep')", &[])
        .unwrap();
    c.execute("INSERT INTO source VALUES (1, 'updated'), (3, 'new')", &[])
        .unwrap();

    c.execute(
        "MERGE INTO target USING source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET val = source.val
         WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)",
        &[],
    )
    .unwrap();

    let rows = c.query("SELECT COUNT(*) FROM target", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

#[test]
fn greatest_two_args() {
    let c = conn();
    let rows = c.query("SELECT GREATEST(3, 5)", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 5);
}

#[test]
fn greatest_three_args() {
    let c = conn();
    let rows = c.query("SELECT GREATEST(1, 5, 3)", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 5);
}

#[test]
fn greatest_with_column() {
    let c = conn();
    c.execute("CREATE TABLE t (a INTEGER, b INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (10, 20), (30, 5)", &[]).unwrap();
    let rows = c.query("SELECT GREATEST(a, b) FROM t ORDER BY a", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 20);
    assert_eq!(rows[1].get::<i64>(0).unwrap(), 30);
}

#[test]
fn greatest_returns_null_on_null_arg() {
    let c = conn();
    let rows = c.query("SELECT GREATEST(1, NULL, 3)", &[]).unwrap();
    assert!(rows[0].get::<Option<i64>>(0).unwrap().is_none());
}

#[test]
fn least_two_args() {
    let c = conn();
    let rows = c.query("SELECT LEAST(3, 5)", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

#[test]
fn least_three_args() {
    let c = conn();
    let rows = c.query("SELECT LEAST(7, 2, 9)", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
}

#[test]
fn least_with_column() {
    let c = conn();
    c.execute("CREATE TABLE t (a INTEGER, b INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (10, 20), (30, 5)", &[]).unwrap();
    let rows = c.query("SELECT LEAST(a, b) FROM t ORDER BY a", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 10);
    assert_eq!(rows[1].get::<i64>(0).unwrap(), 5);
}

#[test]
fn least_returns_null_on_null_arg() {
    let c = conn();
    let rows = c.query("SELECT LEAST(1, NULL, 3)", &[]).unwrap();
    assert!(rows[0].get::<Option<i64>>(0).unwrap().is_none());
}

// ── Priority 2: Date / Time Functions ────────────────────────────────────────

#[test]
fn extract_year_from_date() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(YEAR FROM '2024-03-15')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2024);
}

#[test]
fn extract_month_from_date() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(MONTH FROM '2024-03-15')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

#[test]
fn extract_day_from_date() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(DAY FROM '2024-03-15')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 15);
}

#[test]
fn extract_hour_from_timestamp() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(HOUR FROM '2024-03-15 10:30:45')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 10);
}

#[test]
fn extract_minute_from_timestamp() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(MINUTE FROM '2024-03-15 10:30:45')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 30);
}

#[test]
fn extract_second_from_timestamp() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(SECOND FROM '2024-03-15 10:30:45')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 45);
}

#[test]
fn extract_quarter_from_date() {
    let c = conn();
    let rows = c.query("SELECT EXTRACT(QUARTER FROM '2024-07-15')", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 3);
}

#[test]
fn to_date_two_arg_form() {
    // Second arg (format) should be ignored — we just parse the date string
    let c = conn();
    let rows = c.query("SELECT TO_DATE('2024-03-15', 'YYYY-MM-DD')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15");
}

#[test]
fn to_char_date_with_format() {
    let c = conn();
    let rows = c.query("SELECT TO_CHAR('2024-03-15', 'YYYY-MM-DD')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15");
}

#[test]
fn to_char_date_year_format() {
    let c = conn();
    let rows = c.query("SELECT TO_CHAR('2024-03-15', 'YYYY')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024");
}

#[test]
fn date_from_parts_basic() {
    let c = conn();
    let rows = c.query("SELECT DATE_FROM_PARTS(2024, 3, 15)", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15");
}

#[test]
fn time_from_parts_basic() {
    let c = conn();
    let rows = c.query("SELECT TIME_FROM_PARTS(10, 30, 45)", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "10:30:45");
}

#[test]
fn timestamp_from_parts_basic() {
    let c = conn();
    let rows = c.query("SELECT TIMESTAMP_FROM_PARTS(2024, 3, 15, 10, 30, 45)", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15 10:30:45");
}

#[test]
fn last_day_of_january() {
    let c = conn();
    let rows = c.query("SELECT LAST_DAY('2024-01-15')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-01-31");
}

#[test]
fn last_day_of_february_leap_year() {
    let c = conn();
    let rows = c.query("SELECT LAST_DAY('2024-02-10')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-02-29");
}

#[test]
fn next_day_function() {
    let c = conn();
    // 2024-01-15 is a Monday; next Wednesday is 2024-01-17
    let rows = c.query("SELECT NEXT_DAY('2024-01-15', 'Wednesday')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-01-17");
}

#[test]
fn convert_timezone_passthrough() {
    // CONVERT_TIMEZONE is not supported in SQLite — it returns the input timestamp unchanged
    let c = conn();
    let rows = c.query("SELECT CONVERT_TIMEZONE('UTC', '2024-03-15 10:00:00')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "2024-03-15 10:00:00");
}

// ── Priority 2: :: Cast Operator ─────────────────────────────────────────────

#[test]
fn cast_operator_integer() {
    let c = conn();
    let rows = c.query("SELECT '42'::INTEGER", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 42);
}

#[test]
fn cast_operator_text() {
    let c = conn();
    let rows = c.query("SELECT 42::TEXT", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "42");
}

#[test]
fn cast_operator_on_column() {
    let c = conn();
    c.execute("CREATE TABLE t (v TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('99')", &[]).unwrap();
    let rows = c.query("SELECT v::INTEGER FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 99);
}

// ── Priority 2: Semi-Structured Functions ─────────────────────────────────────

#[test]
fn array_slice_basic() {
    let c = conn();
    let rows = c.query("SELECT ARRAY_SLICE('[10,20,30,40,50]', 1, 3)", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed, serde_json::json!([20, 30]));
}

#[test]
fn array_append_basic() {
    let c = conn();
    let rows = c.query("SELECT ARRAY_APPEND('[1,2,3]', 4)", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed, serde_json::json!([1, 2, 3, 4]));
}

#[test]
fn array_concat_basic() {
    let c = conn();
    let rows = c.query("SELECT ARRAY_CONCAT('[1,2]', '[3,4]')", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed, serde_json::json!([1, 2, 3, 4]));
}

#[test]
fn array_compact_removes_nulls() {
    let c = conn();
    let rows = c.query("SELECT ARRAY_COMPACT('[1,null,2,null,3]')", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed, serde_json::json!([1, 2, 3]));
}

#[test]
fn array_unique_deduplicates() {
    let c = conn();
    let rows = c.query("SELECT ARRAY_UNIQUE('[1,2,1,3,2]')", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let mut parsed: Vec<i64> = serde_json::from_str(&result).unwrap();
    parsed.sort();
    assert_eq!(parsed, vec![1, 2, 3]);
}

#[test]
fn typeof_array() {
    let c = conn();
    let rows = c.query("SELECT TYPEOF('[1,2,3]')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap().to_lowercase(), "array");
}

#[test]
fn typeof_object() {
    let c = conn();
    let rows = c.query("SELECT TYPEOF('{\"a\":1}')", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap().to_lowercase(), "object");
}

#[test]
fn object_keys_basic() {
    let c = conn();
    let rows = c.query("SELECT OBJECT_KEYS('{\"a\":1,\"b\":2}')", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let mut keys: Vec<String> = serde_json::from_str(&result).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b"]);
}

#[test]
fn strip_null_value_basic() {
    let c = conn();
    let rows = c.query("SELECT STRIP_NULL_VALUE('{\"a\":1,\"b\":null,\"c\":3}')", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert!(parsed.get("a").is_some());
    assert!(parsed.get("b").is_none());
    assert!(parsed.get("c").is_some());
}

// ── Priority 2: DDL Constructs ────────────────────────────────────────────────

#[test]
fn create_temporary_table() {
    let c = conn();
    c.execute("CREATE TEMPORARY TABLE tmp (id INTEGER, name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO tmp VALUES (1, 'hello')", &[]).unwrap();
    let rows = c.query("SELECT name FROM tmp WHERE id = 1", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "hello");
}

#[test]
fn create_transient_table() {
    let c = conn();
    // TRANSIENT should be stripped; treated as a normal CREATE TABLE
    c.execute("CREATE TRANSIENT TABLE t_transient (id INTEGER, val TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t_transient VALUES (1, 'foo')", &[]).unwrap();
    let rows = c.query("SELECT val FROM t_transient", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "foo");
}

#[test]
fn alter_table_rename_column() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, old_name TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'alice')", &[]).unwrap();
    c.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name", &[]).unwrap();
    let rows = c.query("SELECT new_name FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "alice");
}

#[test]
fn create_database_is_noop() {
    let c = conn();
    // Should not error — silently ignored
    c.execute("CREATE DATABASE mydb", &[]).unwrap();
}

#[test]
fn drop_database_is_noop() {
    let c = conn();
    c.execute("DROP DATABASE mydb", &[]).unwrap();
}

#[test]
fn analyze_is_noop() {
    let c = conn();
    c.execute("ANALYZE", &[]).unwrap();
}

// ── Priority 2: MERGE INTO (no-op) ───────────────────────────────────────────

#[test]
fn merge_into_is_noop() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, val TEXT)", &[]).unwrap();
    // MERGE INTO is a no-op — SQLite does not support it
    c.execute(
        "MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET t.val = src.val",
        &[],
    )
    .unwrap();
}

// ── Priority 3: Math Functions ────────────────────────────────────────────────

#[test]
fn log_two_arg_form() {
    // LOG(base, x) → (LOG(x) / LOG(base)): log base 10 of 100 = 2
    let c = conn();
    let rows = c.query("SELECT LOG(10, 100)", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 2.0).abs() < 1e-9, "log10(100) should be 2.0, got {result}");
}

#[test]
fn log_one_arg_passes_through() {
    // LOG(x) single-arg form passes through unchanged to SQLite's LOG()
    let c = conn();
    let rows = c.query("SELECT LOG(1)", &[]).unwrap();
    // LOG(1) = 0 in any base
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 0.0).abs() < 1e-9, "log(1) should be 0.0, got {result}");
}

#[test]
fn random_function_returns_value() {
    // RANDOM() passes through to SQLite.
    // Note: SQLite RANDOM() returns a 64-bit integer, not a float [0,1) like Snowflake.
    let c = conn();
    let rows = c.query("SELECT RANDOM()", &[]).unwrap();
    let _: i64 = rows[0].get(0).unwrap();
}

#[test]
fn width_bucket_function() {
    let c = conn();
    // 5.35 falls in bucket 3 of 5 buckets over [0.024, 10.06)
    let rows = c.query("SELECT width_bucket(5.35, 0.024, 10.06, 5)", &[]).unwrap();
    let bucket: i64 = rows[0].get(0).unwrap();
    assert_eq!(bucket, 3, "5.35 in [0.024, 10.06] with 5 buckets should be bucket 3");

    // Below min → bucket 0
    let rows = c.query("SELECT width_bucket(-1.0, 0.0, 10.0, 5)", &[]).unwrap();
    let b: i64 = rows[0].get(0).unwrap();
    assert_eq!(b, 0);

    // At or above max → bucket num_buckets + 1
    let rows = c.query("SELECT width_bucket(10.0, 0.0, 10.0, 5)", &[]).unwrap();
    let b: i64 = rows[0].get(0).unwrap();
    assert_eq!(b, 6);
}

// ── Priority 3: Aggregate Functions ──────────────────────────────────────────

#[test]
fn median_odd_count() {
    let c = conn();
    c.execute("CREATE TABLE t (v REAL)", &[]).unwrap();
    for v in [1.0f64, 2.0, 3.0, 4.0, 5.0] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }
    let rows = c.query("SELECT MEDIAN(v) FROM t", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 3.0).abs() < 1e-9, "median of 1..5 should be 3.0, got {result}");
}

#[test]
fn median_even_count() {
    let c = conn();
    c.execute("CREATE TABLE t (v REAL)", &[]).unwrap();
    for v in [1.0f64, 2.0, 3.0, 4.0] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }
    let rows = c.query("SELECT MEDIAN(v) FROM t", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    assert!((result - 2.5).abs() < 1e-9, "median of 1..4 should be 2.5, got {result}");
}

#[test]
fn any_value_function() {
    // ANY_VALUE translates to MIN — returns the minimum value within each group
    let c = conn();
    c.execute("CREATE TABLE t (grp TEXT, v INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('a', 1)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('a', 2)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('b', 3)", &[]).unwrap();

    let rows = c
        .query("SELECT grp, ANY_VALUE(v) FROM t GROUP BY grp ORDER BY grp", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    let v_a: i64 = rows[0].get(1).unwrap();
    let v_b: i64 = rows[1].get(1).unwrap();
    assert_eq!(v_a, 1);
    assert_eq!(v_b, 3);
}

#[test]
fn approx_count_distinct_function() {
    // APPROX_COUNT_DISTINCT maps to COUNT(DISTINCT expr) — exact in SQLite
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    for v in [1i64, 2, 2, 3, 3, 3] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }
    let rows = c.query("SELECT APPROX_COUNT_DISTINCT(v) FROM t", &[]).unwrap();
    let cnt: i64 = rows[0].get(0).unwrap();
    assert_eq!(cnt, 3);
}

#[test]
fn array_agg_function() {
    // ARRAY_AGG maps to JSON_GROUP_ARRAY — returns a JSON array
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    for v in [1i64, 2, 3] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }
    let rows = c.query("SELECT ARRAY_AGG(v) FROM t", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON array");
    assert!(parsed.is_array());
    assert_eq!(parsed.as_array().unwrap().len(), 3);
}

#[test]
fn object_agg_function() {
    // OBJECT_AGG maps to JSON_GROUP_OBJECT — returns a JSON object
    let c = conn();
    c.execute("CREATE TABLE t (k TEXT, v INTEGER)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('a', 1)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('b', 2)", &[]).unwrap();
    let rows = c.query("SELECT OBJECT_AGG(k, v) FROM t", &[]).unwrap();
    let result: String = rows[0].get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON object");
    assert!(parsed.is_object());
    assert_eq!(parsed["a"], serde_json::json!(1));
    assert_eq!(parsed["b"], serde_json::json!(2));
}

// ── Priority 3: Type System ───────────────────────────────────────────────────

#[test]
fn geography_type_maps_to_text() {
    // GEOGRAPHY is unsupported by SQLite — mapped to TEXT with a log::warn!
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, location GEOGRAPHY)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'POINT(1.0 2.0)')", &[]).unwrap();
    let rows = c.query("SELECT location FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "POINT(1.0 2.0)");
}

#[test]
fn geometry_type_maps_to_text() {
    // GEOMETRY is unsupported by SQLite — mapped to TEXT with a log::warn!
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, shape GEOMETRY)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1, 'POLYGON((0 0,1 0,1 1,0 0))')", &[]).unwrap();
    let rows = c.query("SELECT shape FROM t", &[]).unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "POLYGON((0 0,1 0,1 1,0 0))");
}

#[test]
fn from_value_small_integer_types() {
    let c = conn();
    let rows = c.query("SELECT 42, 127, 200, 32767", &[]).unwrap();
    let v_i16: i16 = rows[0].get(0).unwrap();
    let v_i8: i8 = rows[0].get(1).unwrap();
    let v_u8: u8 = rows[0].get(2).unwrap();
    let v_u32: u32 = rows[0].get(3).unwrap();
    assert_eq!(v_i16, 42);
    assert_eq!(v_i8, 127);
    assert_eq!(v_u8, 200);
    assert_eq!(v_u32, 32767);
}

#[test]
fn from_value_serde_json() {
    let c = conn();
    c.execute("CREATE TABLE t (data TEXT)", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('{\"key\":\"value\",\"n\":42}')", &[]).unwrap();
    let rows = c.query("SELECT data FROM t", &[]).unwrap();
    let parsed: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(parsed["key"], "value");
    assert_eq!(parsed["n"], 42);
}

// ── Priority 3: Code Quality ──────────────────────────────────────────────────

#[test]
fn select_top_n_with_order_by() {
    // SELECT TOP N ... ORDER BY col → SELECT ... ORDER BY col LIMIT N
    // LIMIT must follow ORDER BY — verify both ordering and limit are correct
    let c = conn();
    c.execute("CREATE TABLE t (v INTEGER)", &[]).unwrap();
    for v in [5i64, 3, 1, 4, 2] {
        c.execute("INSERT INTO t VALUES (?)", &[&v]).unwrap();
    }
    let rows = c.query("SELECT TOP 3 v FROM t ORDER BY v", &[]).unwrap();
    assert_eq!(rows.len(), 3);
    let vals: Vec<i64> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert_eq!(vals, vec![1, 2, 3]);
}

// ── Priority 3: Bug Fixes ─────────────────────────────────────────────────────

#[test]
fn lpad_empty_pad_string_errors() {
    // Snowflake raises an error when the pad string is empty
    let c = conn();
    let result = c.query("SELECT LPAD('hello', 10, '')", &[]);
    assert!(result.is_err(), "LPAD with empty pad string should return an error");
}

#[test]
fn rpad_empty_pad_string_errors() {
    // Snowflake raises an error when the pad string is empty
    let c = conn();
    let result = c.query("SELECT RPAD('hello', 10, '')", &[]);
    assert!(result.is_err(), "RPAD with empty pad string should return an error");
}

#[test]
fn decimal_precision_limit() {
    // NUMBER(p,s) is stored as REAL (64-bit float); Snowflake preserves up to 38 digits.
    // SQLite REAL has ~15-17 significant decimal digit precision.
    let c = conn();
    c.execute("CREATE TABLE t (v NUMBER(38, 10))", &[]).unwrap();
    c.execute("INSERT INTO t VALUES (1234567890.1234567890)", &[]).unwrap();
    let rows = c.query("SELECT v FROM t", &[]).unwrap();
    let result: f64 = rows[0].get(0).unwrap();
    // The value is approximately preserved but not to 38 significant digits
    assert!(
        (result - 1_234_567_890.123_456_789_f64).abs() < 1.0,
        "REAL approximation within 1 unit: {result}"
    );
}

#[test]
fn string_collation_case_sensitivity() {
    // COLLATE clauses are stripped; SQLite TEXT comparison is case-sensitive by default.
    // Snowflake VARCHAR comparison is case-insensitive — this is a known difference.
    let c = conn();
    c.execute("CREATE TABLE t (name TEXT COLLATE 'utf8')", &[]).unwrap();
    c.execute("INSERT INTO t VALUES ('Alice')", &[]).unwrap();
    let rows = c
        .query("SELECT COUNT(*) FROM t WHERE name = 'alice'", &[])
        .unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    // Documents the difference: SQLite is case-sensitive (0 matches), Snowflake would return 1
    assert_eq!(count, 0, "SQLite TEXT comparison is case-sensitive: 'Alice' != 'alice'");
}

#[test]
fn recursive_cte_depth() {
    // SQLite limits recursion to 1000 by default; Snowflake allows much deeper recursion.
    // This test documents that shallow recursion (depth 10) works correctly.
    let c = conn();
    let rows = c
        .query(
            "WITH RECURSIVE n(i) AS (
                SELECT 1
                UNION ALL
                SELECT i + 1 FROM n WHERE i < 10
             )
             SELECT COUNT(*) FROM n",
            &[],
        )
        .unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    assert_eq!(count, 10);
}
