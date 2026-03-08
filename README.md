# local-db

[![codecov](https://codecov.io/gh/cvrajeesh/local-db/branch/main/graph/badge.svg)](https://codecov.io/gh/cvrajeesh/local-db)

A local SQLite-backed database driver that acts as a **drop-in replacement for Snowflake** in integration tests.

Write your application code against the Snowflake SQL dialect. In CI or on a developer laptop, swap the real Snowflake connection for `local-db` — no data warehouse required.

---

## How it works

```
Your Snowflake SQL  ──►  Translator  ──►  SQLite SQL  ──►  rusqlite
```

The translator rewrites Snowflake SQL statements to equivalent SQLite SQL in several passes:

| Pass | What it does |
|---|---|
| **No-op detection** | `USE DATABASE`, `ALTER SESSION`, `SHOW TABLES`, `COPY INTO`, `GRANT`, etc. are silently ignored |
| **DDL rewriting** | `CREATE OR REPLACE TABLE` → `CREATE TABLE IF NOT EXISTS` |
| **Type mapping** | `NUMBER`, `VARIANT`, `TIMESTAMP_NTZ`, `BOOLEAN`, … → SQLite affinities |
| **Identifier stripping** | `db.schema.table` → `table` |
| **Function rewriting** | `IFF`, `NVL`, `DECODE`, `DATEADD`, `DATE_TRUNC`, `TO_VARCHAR`, `CURRENT_TIMESTAMP()`, … → SQLite equivalents |
| **Operator rewriting** | `ILIKE`, `col:path`, `col['field']` semi-structured paths, `TOP n` → SQLite equivalents |

---

## Quick start

Add to `Cargo.toml`:

```toml
[dev-dependencies]
local-db = { git = "https://github.com/cvrajeesh/local-db" }
```

In your integration test:

```rust
use local_db::{Connection, Config};

#[test]
fn my_integration_test() -> local_db::Result<()> {
    let conn = Connection::open_in_memory()?;

    // DDL — Snowflake syntax works transparently
    conn.execute(
        "CREATE OR REPLACE TABLE orders (
            id         NUMBER(18, 0) NOT NULL,
            customer   VARCHAR(255),
            amount     NUMBER(10, 2),
            metadata   VARIANT,
            created_at TIMESTAMP_NTZ
        )",
        &[],
    )?;

    // DML
    conn.execute(
        "INSERT INTO orders (id, customer, amount, metadata, created_at)
         VALUES (?, ?, ?, ?, ?)",
        &[&1i64, &"Acme Corp", &199.99f64, &r#"{"source":"web"}"#, &"2024-01-15T09:00:00"],
    )?;

    // Query using Snowflake functions
    let rows = conn.query(
        "SELECT id,
                NVL(customer, 'unknown')          AS customer,
                IFF(amount > 100, 'large', 'small') AS size,
                metadata:source                    AS source
         FROM orders",
        &[],
    )?;

    for row in &rows {
        let id: i64         = row.get(0)?;
        let customer: String = row.get_by_name("customer")?;
        let size: String     = row.get_by_name("size")?;
        let source: String   = row.get_by_name("source")?;
        println!("{id} | {customer} | {size} | {source}");
    }

    Ok(())
}
```

---

## Supported Snowflake features

### Data types

| Snowflake | SQLite | Notes |
|---|---|---|
| `NUMBER(p, 0)` / `NUMBER` | `INTEGER` | |
| `NUMBER(p, s>0)` / `DECIMAL` | `REAL` | |
| `FLOAT` / `DOUBLE` / `REAL` | `REAL` | |
| `VARCHAR(n)` / `STRING` / `TEXT` / `CHAR` | `TEXT` | |
| `BOOLEAN` | `INTEGER` | `0` / `1` |
| `DATE` | `TEXT` | ISO-8601 `YYYY-MM-DD` |
| `TIME` | `TEXT` | `HH:MM:SS` |
| `TIMESTAMP_NTZ` / `_LTZ` / `_TZ` | `TEXT` | ISO-8601 datetime |
| `VARIANT` / `OBJECT` / `ARRAY` | `TEXT` | JSON stored as text |
| `BINARY` / `VARBINARY` | `BLOB` | |

### DDL

| Snowflake | SQLite translation |
|---|---|
| `CREATE OR REPLACE TABLE t (…)` | `CREATE TABLE IF NOT EXISTS t (…)` |
| `CREATE TABLE … CLONE src` | *(not supported — use `INSERT INTO … SELECT`)* |
| `AUTOINCREMENT` column option | stripped |
| `CLUSTER BY (…)` | stripped |
| `COMMENT = '…'` | stripped |
| `DEFAULT seq.NEXTVAL` | stripped |

### Functions

| Snowflake | SQLite |
|---|---|
| `IFF(cond, t, f)` | `CASE WHEN cond THEN t ELSE f END` |
| `NVL(a, b)` | `COALESCE(a, b)` |
| `NVL2(a, b, c)` | `CASE WHEN a IS NOT NULL THEN b ELSE c END` |
| `ZEROIFNULL(a)` | `COALESCE(a, 0)` |
| `NULLIFZERO(a)` | `NULLIF(a, 0)` |
| `DECODE(expr, s1,r1,…,default)` | `CASE expr WHEN s1 THEN r1 … ELSE default END` |
| `TO_VARCHAR(a)` / `TO_CHAR(a)` | `CAST(a AS TEXT)` |
| `TO_NUMBER(a)` / `TO_DECIMAL(a)` | `CAST(a AS REAL)` |
| `TO_BOOLEAN(a)` | `CAST(a AS INTEGER)` |
| `TO_DATE(a)` | `DATE(a)` |
| `TO_TIMESTAMP(a)` | `DATETIME(a)` |
| `CURRENT_TIMESTAMP()` | `DATETIME('now')` |
| `CURRENT_DATE()` | `DATE('now')` |
| `GETDATE()` / `SYSDATE()` | `DATETIME('now')` |
| `DATEADD(unit, n, d)` | `DATE(d, n \|\| ' days')` etc. |
| `DATEDIFF(unit, d1, d2)` | `JULIANDAY(d2) - JULIANDAY(d1)` etc. |
| `DATE_TRUNC('month', d)` | `DATE(d, 'start of month')` etc. |
| `YEAR(d)` / `MONTH(d)` / `DAY(d)` / … | `STRFTIME('%Y', d)` etc. |
| `CONTAINS(a, b)` | `INSTR(a, b) > 0` |
| `STARTSWITH(a, b)` | `a LIKE b \|\| '%'` |
| `ENDSWITH(a, b)` | `a LIKE '%' \|\| b` |
| `ARRAY_SIZE(arr)` | `JSON_ARRAY_LENGTH(arr)` |
| `PARSE_JSON(s)` | `s` *(passthrough)* |
| `SPLIT_PART(s, d, n)` | custom SQLite function |
| `REGEXP_LIKE(s, p)` | `s REGEXP p` *(custom function)* |
| `DIV0(a, b)` | `CASE WHEN b = 0 THEN 0 ELSE a/b END` |

### Operators / syntax

| Snowflake | SQLite |
|---|---|
| `col ILIKE pattern` | `LOWER(col) LIKE LOWER(pattern)` |
| `col:path` (semi-structured) | `JSON_EXTRACT(col, '$.path')` |
| `col['field']` | `JSON_EXTRACT(col, '$.field')` |
| `col[n]` (array index) | `JSON_EXTRACT(col, '$[n]')` |
| `SELECT TOP n …` | `SELECT … LIMIT n` |

### No-op statements (silently ignored)

- `USE DATABASE / SCHEMA / WAREHOUSE / ROLE`
- `ALTER SESSION …`
- `ALTER / CREATE / DROP WAREHOUSE`
- `SHOW TABLES / SCHEMAS / …`
- `COPY INTO …`
- `CREATE / DROP STAGE / PIPE / STREAM / TASK`
- `GRANT / REVOKE`
- `SET var = value` / `UNSET var`
- `COMMENT ON …`

---

## Configuration

```rust
use local_db::{Connection, Config};

let conn = Connection::open_in_memory_with_config(
    Config::new()
        .with_schema_prefix()       // public.orders → public__orders
        .with_drop_before_create()  // CREATE OR REPLACE → DROP + CREATE
)?;
```

---

## Running tests

```bash
cargo test
```

---

## License

MIT
