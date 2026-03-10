# HTTP Server

`snowlite` ships an optional HTTP server that implements the **Snowflake wire protocol**.
Any Snowflake connector — Python, Node.js, Go, Java, etc. — can point at it instead of a
real Snowflake account.

Each login creates a fresh, isolated in-memory SQLite database. Sessions are completely
independent of each other.

---

## Installation

### Pre-built binary (no Rust required)

Pre-built binaries are attached to every [GitHub Release](https://github.com/cvrajeesh/snowlite/releases/latest):

| Platform | File |
|---|---|
| macOS (Intel + Apple Silicon) | `snowlite-server-macos-universal` |
| Linux x86_64 | `snowlite-server-linux-x86_64` |
| Linux aarch64 | `snowlite-server-linux-aarch64` |
| Windows x86_64 | `snowlite-server-windows-x86_64.exe` |

**macOS / Linux — one-liner:**

```bash
curl -fsSL https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.sh | sh
./snowlite-server
```

**Windows — PowerShell:**

```powershell
irm https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.ps1 | iex
.\snowlite-server.exe
```

### Build from source (requires Rust)

```bash
# Build and start on default port 8765
cargo run --features server --bin snowlite-server

# Custom port
cargo run --features server --bin snowlite-server -- --port 9000
```

---

## Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/session/v1/login-request` | POST | Authenticate — always succeeds; returns a session token |
| `/queries/v1/query-request` | POST | Execute SQL; returns results in Snowflake JSON format |
| `/session?delete=true` | POST | Close the session |
| `/health` | GET | Health check |

---

## Python

Install the connector:

```bash
pip install snowflake-connector-python
```

### Basic connection

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

### DDL and DML

```python
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

### Conditional functions: IFF, NVL, NVL2, DECODE

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

### Semi-structured data (VARIANT / JSON)

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

# Use DictCursor for column-name access
cur_dict = conn.cursor(snowflake.connector.DictCursor)
cur_dict.execute("""
    SELECT id, metadata:source AS source, metadata:tier AS tier
    FROM orders
    WHERE metadata:tier ILIKE 'gold'
""")
for row in cur_dict.fetchall():
    # Keys are uppercased: {'ID': '1', 'SOURCE': 'web', 'TIER': 'gold'}
    print(row["ID"], row["SOURCE"], row["TIER"])
```

### Date and time functions

```python
cur.execute("""
    SELECT
        id,
        created_at,
        DATE_TRUNC('month', created_at)                        AS month_start,
        DATEADD('day', 30, created_at)                         AS due_date,
        DATEDIFF('day', created_at, '2024-04-01T00:00:00')     AS days_old,
        YEAR(created_at)                                       AS yr,
        TO_VARCHAR(created_at)                                 AS ts_text
    FROM orders
    ORDER BY created_at
""")
```

### String functions

```python
# Pattern matching
cur.execute("""
    SELECT id, customer
    FROM orders
    WHERE customer ILIKE '%corp%'
       OR STARTSWITH(customer, 'Glob')
       OR CONTAINS(customer, 'tech')
""")

# Regex
cur.execute("""
    SELECT
        REGEXP_REPLACE(customer, '\\s+', '_')  AS slug,
        REGEXP_SUBSTR(customer, '[A-Z][a-z]+') AS first_word
    FROM orders
""")

# Padding and formatting
cur.execute("""
    SELECT
        LPAD(CAST(id AS TEXT), 6, '0')  AS padded_id,
        RPAD(customer, 20, '.')         AS padded_name,
        INITCAP(LOWER(customer))        AS title_case,
        REPEAT('*', 5)                  AS stars,
        REVERSE(customer)               AS backwards
    FROM orders
""")

# SPLIT_PART
cur.execute("""
    SELECT
        SPLIT_PART('user@example.com', '@', 1) AS username,
        SPLIT_PART('user@example.com', '@', 2) AS domain
""")
print(cur.fetchone())   # ('user', 'example.com')
```

### Window / Analytic functions

```python
cur.execute("""
    SELECT
        id,
        region,
        amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
        RANK()       OVER (ORDER BY amount DESC)                     AS overall_rank,
        SUM(amount)  OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
        LAG(amount, 1, 0) OVER (ORDER BY id)                        AS prev_amount
    FROM orders
    ORDER BY id
""")
for row in cur.fetchall():
    print(row)
```

### Transactions

```python
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

### pytest fixture pattern

```python
# conftest.py
import pytest
import snowflake.connector

@pytest.fixture
def sf():
    """Fresh snowlite session per test (requires snowlite-server running)."""
    conn = snowflake.connector.connect(
        host="localhost", port=8765, protocol="http",
        user="test", password="test", account="test",
    )
    yield conn.cursor(snowflake.connector.DictCursor)
    conn.close()


# test_orders.py
def test_large_order_flag(sf):
    sf.execute("CREATE OR REPLACE TABLE orders (id NUMBER, amount NUMBER(10,2))")
    sf.execute("INSERT INTO orders VALUES (%s, %s)", (1, 500.0))
    sf.execute("SELECT IFF(amount > 100, 'large', 'small') AS bucket FROM orders")
    assert sf.fetchone()["BUCKET"] == "large"
```

---

## Node.js

```javascript
const snowflake = require('snowflake-sdk');

const connection = snowflake.createConnection({
  account:   'test',
  username:  'test',
  password:  'test',
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

---

## Embed in your Cargo project

```toml
[dev-dependencies]
snowlite = { git = "https://github.com/cvrajeesh/snowlite", features = ["server"] }
```
