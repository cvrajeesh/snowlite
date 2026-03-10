//! # snowlite
//!
//! A local SQLite-backed database driver that acts as an in-place replacement for
//! Snowflake in integration tests. The driver translates Snowflake SQL dialect to
//! SQLite-compatible SQL so your existing queries and schema definitions work
//! without modification on developer machines.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use snowlite::{Connection, Config, Row};
//!
//! # fn main() -> Result<(), snowlite::Error> {
//! // Create an in-memory database (great for unit / integration tests)
//! let conn = Connection::open_in_memory()?;
//!
//! // DDL — Snowflake syntax is automatically translated
//! conn.execute(
//!     "CREATE OR REPLACE TABLE orders (
//!         id     NUMBER(18, 0) NOT NULL,
//!         status VARCHAR(64),
//!         amount FLOAT,
//!         meta   VARIANT,
//!         created_at TIMESTAMP_NTZ
//!     )",
//!     &[],
//! )?;
//!
//! // DML
//! conn.execute(
//!     "INSERT INTO orders (id, status, amount) VALUES (?, ?, ?)",
//!     &[&1i64, &"pending", &99.95f64],
//! )?;
//!
//! // Query with Snowflake functions
//! let rows = conn.query(
//!     "SELECT id, NVL(status, 'unknown'), IFF(amount > 100, 'large', 'small')
//!      FROM orders",
//!     &[],
//! )?;
//!
//! for row in rows {
//!     let id: i64 = row.get(0)?;
//!     let status: String = row.get(1)?;
//!     let size: String = row.get(2)?;
//!     println!("{id} | {status} | {size}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Snowflake Features Supported
//!
//! | Feature | Translation |
//! |---|---|
//! | `CREATE OR REPLACE TABLE` | `DROP TABLE IF EXISTS` + `CREATE TABLE` |
//! | `NUMBER / FLOAT / BOOLEAN / VARIANT` | SQLite type affinity |
//! | `TIMESTAMP_NTZ / _LTZ / _TZ` | `TEXT` (ISO-8601) |
//! | `IFF(cond, t, f)` | `CASE WHEN … THEN … ELSE … END` |
//! | `NVL / NVL2 / ZEROIFNULL` | `COALESCE` / `NULLIF` |
//! | `DECODE` | `CASE … WHEN` |
//! | `DATEADD / DATEDIFF / DATE_TRUNC` | SQLite `date()` / `strftime()` |
//! | `CURRENT_TIMESTAMP() / GETDATE()` | `DATETIME('now')` |
//! | `TO_VARCHAR / TO_NUMBER / TO_DATE` | `CAST` / `DATE()` |
//! | `ILIKE` | `LOWER(a) LIKE LOWER(b)` |
//! | `CONTAINS / STARTSWITH / ENDSWITH` | `INSTR` / `LIKE` |
//! | `ARRAY_SIZE` | `JSON_ARRAY_LENGTH` |
//! | `USE DATABASE/SCHEMA/WAREHOUSE` | no-op |
//! | `ALTER SESSION` | no-op |
//! | Semi-structured `col:path` | `JSON_EXTRACT(col, '$.path')` |

pub mod connection;
pub mod error;
pub mod row;
pub mod translator;
pub mod types;

pub use connection::{Config, Connection};
pub use error::Error;
pub use row::Row;
pub use types::Value;

/// Convenience result type used throughout this crate.
pub type Result<T> = std::result::Result<T, Error>;
