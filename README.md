# snowlite

[![codecov](https://codecov.io/gh/cvrajeesh/snowlite/branch/main/graph/badge.svg)](https://codecov.io/gh/cvrajeesh/snowlite)

> **⚠️ Experimental — learning project**
> This repository is an experiment in *vibe coding* — building software iteratively with AI assistance. It is not production-ready, not actively maintained, and makes no guarantees of correctness or stability. Use it to learn, hack, or get inspired, but don't depend on it in production systems.

A local SQLite-backed database driver that acts as a **drop-in replacement for Snowflake** in integration tests.

Write your application code against the Snowflake SQL dialect. In CI or on a developer laptop, swap the real Snowflake connection for `snowlite` — no data warehouse required.

---

## How it works

```
Your Snowflake SQL  ──►  Translator  ──►  SQLite SQL  ──►  rusqlite
```

The translator rewrites Snowflake SQL statements to equivalent SQLite SQL in several passes:

| Pass | What it does |
|---|---|
| **No-op detection** | `USE DATABASE`, `ALTER SESSION`, `SHOW`, `COPY INTO`, `GRANT`, etc. are silently ignored |
| **DDL rewriting** | `CREATE OR REPLACE TABLE` → `CREATE TABLE IF NOT EXISTS` |
| **Type mapping** | `NUMBER`, `VARIANT`, `TIMESTAMP_NTZ`, `BOOLEAN`, … → SQLite affinities |
| **Identifier stripping** | `db.schema.table` → `table` |
| **Function rewriting** | 60+ Snowflake functions and operators → SQLite equivalents or custom functions |

See [docs/architecture.md](./docs/architecture.md) for a full pipeline walkthrough.

---

## Quick start

Add to `Cargo.toml`:

```toml
[dev-dependencies]
snowlite = { git = "https://github.com/cvrajeesh/snowlite" }
```

```rust
use snowlite::{Connection, Value};

#[test]
fn my_integration_test() -> snowlite::Result<()> {
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
                NVL(customer, 'unknown')            AS customer,
                IFF(amount > 100, 'large', 'small') AS size,
                metadata:source                     AS source
         FROM orders",
        &[],
    )?;

    for row in &rows {
        let id: i64          = row.get(0)?;
        let customer: String = row.get_by_name("customer")?;
        let size: String     = row.get_by_name("size")?;
        let source: String   = row.get_by_name("source")?;
        println!("{id} | {customer} | {size} | {source}");
    }

    Ok(())
}
```

---

## Configuration

```rust
use snowlite::{Connection, Config};

let conn = Connection::open_in_memory_with_config(
    Config::new()
        .with_schema_prefix()       // public.orders → public__orders
        .with_drop_before_create()  // CREATE OR REPLACE → DROP + CREATE
)?;
```

See [docs/configuration.md](./docs/configuration.md) for full details.

---

## HTTP server (multi-language access)

`snowlite` ships an optional HTTP server that implements the Snowflake wire protocol.
Any Snowflake connector — Python, Node.js, Go, Java, etc. — can point at it instead of
a real Snowflake account.

**macOS / Linux — one-liner install:**

```bash
curl -fsSL https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.sh | sh
./snowlite-server
```

**Windows — PowerShell:**

```powershell
irm https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.ps1 | iex
.\snowlite-server.exe
```

See [docs/http-server.md](./docs/http-server.md) for connection examples in Python and Node.js.

---

## Documentation

| Document | Contents |
|---|---|
| [docs/supported-sql.md](./docs/supported-sql.md) | Complete SQL reference: types, DDL, all functions, window functions, operators, no-ops |
| [docs/http-server.md](./docs/http-server.md) | HTTP server install, endpoints, Python and Node.js examples |
| [docs/configuration.md](./docs/configuration.md) | Config options with code examples |
| [docs/architecture.md](./docs/architecture.md) | Translation pipeline internals, source layout, design decisions |
| [docs/limitations.md](./docs/limitations.md) | Known gaps and unsupported constructs |
| [TODO.md](./TODO.md) | Prioritised backlog of missing features |

---

## Running tests

```bash
cargo test                    # unit + integration tests
cargo test --features server  # include server tests
```

---

## License

MIT
