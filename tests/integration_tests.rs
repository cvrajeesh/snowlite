//! Integration tests for local-db.
//!
//! Run with: `cargo test`

use local_db::{Connection, Value};

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

    let rows = c
        .query("SELECT NVL(val, 0) FROM t ORDER BY val", &[])
        .unwrap();
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
        .query(
            "SELECT IFF(amount > 100, 'large', 'small') FROM t ORDER BY amount",
            &[],
        )
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
        .query(
            "SELECT NVL2(x, 'not null', 'is null') FROM t ORDER BY x",
            &[],
        )
        .unwrap();
    let values: Vec<String> = rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert!(values.contains(&"not null".to_owned()));
    assert!(values.contains(&"is null".to_owned()));
}

#[test]
fn dateadd_function() {
    let c = conn();
    let rows = c
        .query("SELECT DATEADD(day, 7, '2024-01-01')", &[])
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
    let rows = c.query("SELECT ARRAY_SIZE('[1,2,3,4,5]')", &[]).unwrap();
    let result: i64 = rows[0].get(0).unwrap();
    assert_eq!(result, 5);
}

// ── Semi-structured ──────────────────────────────────────────────────────────

#[test]
fn semi_structured_colon_path() {
    let c = conn();
    c.execute("CREATE TABLE events (data VARIANT)", &[])
        .unwrap();
    c.execute(
        "INSERT INTO events VALUES (?)",
        &[&r#"{"user_id": 42, "action": "click"}"#],
    )
    .unwrap();

    let rows = c.query("SELECT data:user_id FROM events", &[]).unwrap();
    let user_id: String = rows[0].get(0).unwrap();
    assert_eq!(user_id, "42");
}

// ── Identifiers ──────────────────────────────────────────────────────────────

#[test]
fn fully_qualified_identifier_stripped() {
    let c = conn();
    c.execute("CREATE TABLE products (id INTEGER, name TEXT)", &[])
        .unwrap();
    c.execute("INSERT INTO products VALUES (1, 'foo')", &[])
        .unwrap();

    let rows = c.query("SELECT * FROM mydb.public.products", &[]).unwrap();
    assert_eq!(rows.len(), 1);
}

// ── No-ops ───────────────────────────────────────────────────────────────────

#[test]
fn noop_statements_are_ignored() {
    let c = conn();
    // These should not error
    c.execute("USE DATABASE test_db", &[]).unwrap();
    c.execute("ALTER SESSION SET QUERY_TAG = 'test'", &[])
        .unwrap();
    c.execute("USE WAREHOUSE compute_wh", &[]).unwrap();
    c.execute("SHOW TABLES", &[]).unwrap();
}

// ── Row API ──────────────────────────────────────────────────────────────────

#[test]
fn row_get_by_name() {
    let c = conn();
    c.execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])
        .unwrap();
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

    let _ = c.transaction(|conn| -> local_db::Result<()> {
        conn.execute("INSERT INTO t VALUES (1)", &[])?;
        Err(local_db::Error::other("simulated error"))
    });

    let rows = c.query("SELECT COUNT(*) FROM t", &[]).unwrap();
    let count: i64 = rows[0].get(0).unwrap();
    assert_eq!(count, 0);
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
