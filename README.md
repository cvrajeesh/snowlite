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

## HTTP server (multi-language access)

`local-db` ships an optional HTTP server that implements the **Snowflake wire protocol**. Any Snowflake connector — Python, Node.js, Go, Java, etc. — can point at it instead of a real Snowflake account.

### Enable and run

```bash
# Build and start (default port 8765)
cargo run --features server --bin local-db-server

# Custom port
cargo run --features server --bin local-db-server -- --port 9000
```

### Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/session/v1/login-request` | POST | Authentication — always succeeds, returns a session token |
| `/queries/v1/query-request` | POST | Execute SQL and return results in Snowflake JSON format |
| `/session?delete=true` | POST | Close the session |
| `/health` | GET | Health check |

Each login creates an isolated in-memory SQLite database. Sessions are independent.

### Connect with Python

Install the connector:

```bash
pip install snowflake-connector-python
```

#### Basic connection

```python
import snowflake.connector

conn = snowflake.connector.connect(
    host="localhost",
    port=8765,
    protocol="http",
    user="test",
    password="test",
    account="test",
)
cur = conn.cursor()

# Session-scoping statements are silently ignored
cur.execute("USE DATABASE mydb")
cur.execute("USE WAREHOUSE compute_wh")
cur.execute("ALTER SESSION SET QUERY_TAG = 'etl'")
```

#### DDL and DML

```python
# CREATE OR REPLACE is translated to CREATE TABLE IF NOT EXISTS
cur.execute("""
    CREATE OR REPLACE TABLE orders (
        id          NUMBER(18, 0)  NOT NULL,
        customer    VARCHAR(255),
        amount      NUMBER(10, 2),
        region      VARCHAR(50),
        metadata    VARIANT,
        created_at  TIMESTAMP_NTZ
    )
""")

# Parameterised INSERT (%s placeholders)
rows = [
    (1, "Acme Corp",   199.99, "WEST", '{"source":"web","tier":"gold"}',  "2024-01-15T09:00:00"),
    (2, "Globex",       49.00, "EAST", '{"source":"api","tier":"silver"}', "2024-02-20T14:30:00"),
    (3, "Initech",    1250.00, "WEST", '{"source":"web","tier":"gold"}',   "2024-03-05T08:15:00"),
    (4, "Umbrella",      0.00,  None,  '{}',                              "2024-03-10T11:00:00"),
]
cur.executemany(
    "INSERT INTO orders (id, customer, amount, region, metadata, created_at) "
    "VALUES (%s, %s, %s, %s, %s, %s)",
    rows,
)
```

#### Conditional functions: IFF, NVL, NVL2, DECODE

```python
cur.execute("""
    SELECT
        id,
        NVL(region, 'UNKNOWN')                              AS region,
        IFF(amount > 100, 'large', 'small')                 AS bucket,
        NVL2(region, 'has-region', 'no-region')             AS region_flag,
        DECODE(region, 'WEST', 'Pacific', 'EAST', 'Atlantic', 'Other') AS coast,
        ZEROIFNULL(amount)                                  AS safe_amount
    FROM orders
    ORDER BY id
""")

for row in cur.fetchall():
    print(row)
# (1, 'WEST', 'large',  'has-region', 'Pacific',  199.99)
# (2, 'EAST', 'small',  'has-region', 'Atlantic',  49.0)
# (3, 'WEST', 'large',  'has-region', 'Pacific', 1250.0)
# (4, 'UNKNOWN', 'small', 'no-region', 'Other',    0.0)
```

#### Semi-structured data (VARIANT / JSON)

```python
cur.execute("""
    SELECT
        id,
        metadata:source          AS acquisition_channel,
        metadata:tier            AS customer_tier,
        metadata['source']       AS channel_bracket_syntax
    FROM orders
    WHERE metadata:tier = 'gold'
""")

# Use DictCursor to access columns by name
cur_dict = conn.cursor(snowflake.connector.DictCursor)
cur_dict.execute("""
    SELECT id, metadata:source AS source, metadata:tier AS tier
    FROM orders
    WHERE metadata:tier ILIKE 'gold'
""")

for row in cur_dict.fetchall():
    # Columns returned as uppercase keys: {'ID': '1', 'SOURCE': 'web', 'TIER': 'gold'}
    print(row["ID"], row["SOURCE"], row["TIER"])
```

#### Date and time functions

```python
cur.execute("""
    SELECT
        id,
        created_at,
        DATE_TRUNC('month', created_at)         AS month_start,
        DATEADD('day', 30, created_at)          AS due_date,
        DATEDIFF('day', created_at, '2024-04-01T00:00:00') AS days_old,
        YEAR(created_at)                        AS yr,
        TO_VARCHAR(created_at)                  AS ts_text
    FROM orders
    ORDER BY created_at
""")

for row in cur.fetchall():
    print(row)
```

#### String functions and pattern matching

```python
cur.execute("""
    SELECT id, customer
    FROM orders
    WHERE customer ILIKE '%corp%'
       OR STARTSWITH(customer, 'Glob')
       OR CONTAINS(customer, 'tech')
""")

# SPLIT_PART — split an email domain
cur.execute("""
    SELECT
        SPLIT_PART('user@example.com', '@', 1) AS username,
        SPLIT_PART('user@example.com', '@', 2) AS domain
""")
print(cur.fetchone())   # ('user', 'example.com')
```

#### Transactions

```python
# Autocommit is on by default; use begin/commit for explicit transactions
conn2 = snowflake.connector.connect(
    host="localhost", port=8765, protocol="http",
    user="test", password="test", account="test",
    autocommit=False,
)
cur2 = conn2.cursor()
try:
    cur2.execute("CREATE OR REPLACE TABLE audit_log (event VARCHAR)")
    cur2.execute("INSERT INTO audit_log VALUES (%s)", ("order_created",))
    conn2.commit()
except Exception:
    conn2.rollback()
    raise
finally:
    conn2.close()
```

#### pytest fixture pattern

Wrap the server connection in a pytest fixture for clean per-test isolation:

```python
# conftest.py
import pytest
import snowflake.connector

@pytest.fixture
def sf():
    """Fresh local-db session for each test (requires local-db-server running)."""
    conn = snowflake.connector.connect(
        host="localhost", port=8765, protocol="http",
        user="test", password="test", account="test",
    )
    yield conn.cursor(snowflake.connector.DictCursor)
    conn.close()


# test_orders.py
def test_large_order_flag(sf):
    sf.execute("""
        CREATE OR REPLACE TABLE orders (id NUMBER, amount NUMBER(10,2))
    """)
    sf.execute("INSERT INTO orders VALUES (%s, %s)", (1, 500.0))

    sf.execute("""
        SELECT IFF(amount > 100, 'large', 'small') AS bucket FROM orders
    """)
    row = sf.fetchone()
    assert row["BUCKET"] == "large"
```

#### Cleanup

```python
cur.close()
conn.close()
```

### Connect with Node.js

```javascript
const snowflake = require('snowflake-sdk');

const connection = snowflake.createConnection({
  account:  'test',
  username: 'test',
  password: 'test',
  accessUrl: 'http://localhost:8765',
});

connection.connect((err, conn) => {
  if (err) throw err;

  conn.execute({
    sqlText: `CREATE OR REPLACE TABLE events (
                id    NUMBER,
                name  VARCHAR,
                ts    TIMESTAMP_NTZ
              )`,
    complete: (err) => {
      if (err) throw err;

      conn.execute({
        sqlText: 'INSERT INTO events (id, name, ts) VALUES (?, ?, ?)',
        binds: [1, 'page_view', '2024-01-15T09:00:00'],
        complete: (err) => {
          if (err) throw err;

          conn.execute({
            sqlText: "SELECT id, name, DATE_TRUNC('day', ts) AS day FROM events",
            complete: (err, stmt, rows) => {
              if (err) throw err;
              console.log(rows);
              connection.destroy(() => {});
            },
          });
        },
      });
    },
  });
});
```

### Add to Cargo.toml

If you want to embed the server binary in your own project:

```toml
[dev-dependencies]
local-db = { git = "https://github.com/cvrajeesh/local-db", features = ["server"] }
```

---

## Running tests

```bash
cargo test                            # unit + integration tests
cargo test --features server          # include server tests
```

---

## License

MIT
