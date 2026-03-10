# Configuration

`Connection` accepts an optional `Config` struct that controls translator behaviour.

```rust
use snowlite::{Connection, Config};

let conn = Connection::open_in_memory_with_config(
    Config::new()
        .with_schema_prefix()
        .with_drop_before_create()
)?;
```

---

## Options

### `with_schema_prefix()`

When enabled, two-part identifiers (`schema.table`) are preserved as `schema__table`
instead of being stripped to just `table`.

Use this when your test database has multiple schemas and you need to distinguish
`public.orders` from `staging.orders`.

```rust
let conn = Connection::open_in_memory_with_config(
    Config::new().with_schema_prefix()
)?;

// Create tables using the double-underscore convention
conn.execute("CREATE TABLE public__orders (id INTEGER)", &[])?;
conn.execute("INSERT INTO public__orders VALUES (1)", &[])?;

// Query using the Snowflake two-part identifier — translated automatically
let rows = conn.query("SELECT COUNT(*) FROM public.orders", &[])?;
assert_eq!(rows[0].get::<i64>(0)?, 1);
```

Three-part identifiers (`db.schema.table`) are always stripped to just `table`,
regardless of this setting.

---

### `with_drop_before_create()`

Changes how `CREATE OR REPLACE TABLE` is translated:

| Setting | Translation |
|---|---|
| Default | `CREATE TABLE IF NOT EXISTS t (…)` — preserves existing data |
| `with_drop_before_create()` | `DROP TABLE IF EXISTS t; CREATE TABLE t (…)` — clears data |

Use this when your tests expect `CREATE OR REPLACE TABLE` to reset table contents
(matching actual Snowflake behaviour).

```rust
let conn = Connection::open_in_memory_with_config(
    Config::new().with_drop_before_create()
)?;

conn.execute("CREATE TABLE t (id INTEGER)", &[])?;
conn.execute("INSERT INTO t VALUES (1)", &[])?;

// Re-running CREATE OR REPLACE will drop the old table and data
conn.execute("CREATE OR REPLACE TABLE t (id INTEGER, name TEXT)", &[])?;
let rows = conn.query("SELECT COUNT(*) FROM t", &[])?;
assert_eq!(rows[0].get::<i64>(0)?, 0); // table is empty
```

---

## File-backed database

All examples above use in-memory databases. For a persistent database:

```rust
use snowlite::{Connection, Config};
use std::path::Path;

// Default config
let conn = Connection::open("my_test.db")?;

// Custom config
let conn = Connection::open_with_config(
    "my_test.db",
    Config::new().with_drop_before_create(),
)?;
```

---

## Raw SQLite access

If you need to call a rusqlite API directly (register extensions, set PRAGMAs, etc.):

```rust
let raw: &rusqlite::Connection = conn.raw();
raw.execute_batch("PRAGMA cache_size = -64000;")?;
```
