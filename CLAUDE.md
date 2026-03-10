# CLAUDE.md

## Build & Test Commands

```bash
cargo build              # Build the library
cargo test               # Run all tests (unit + integration)
cargo test -- --nocapture # Run tests with stdout visible
cargo test <test_name>   # Run a specific test
RUST_LOG=trace cargo test # Run tests with translation trace logging
```

## Project Overview

**snowlite** is a Rust library that provides a local SQLite-backed database driver as a drop-in replacement for Snowflake in integration tests. It translates Snowflake SQL dialect to SQLite-compatible SQL via regex-based rewriting passes.

## Architecture

```
src/
  lib.rs              - Crate root; re-exports Connection, Config, Row, Value, Error
  connection.rs       - Connection struct wrapping rusqlite; executes translated SQL
  row.rs              - Row type with by-index and by-name column access; FromValue trait
  types.rs            - Value enum (Null/Integer/Real/Text/Blob/Boolean) with ToSql impl
  error.rs            - Error enum using thiserror (Sqlite, Translation, TypeConversion, etc.)
  translator/
    mod.rs            - Public translate() and translate_batch() convenience functions
    rewriter.rs       - Translator struct orchestrating all rewriting passes
    noop.rs           - Detects statements to silently ignore (USE, ALTER SESSION, SHOW, etc.)
    types.rs          - Rewrites Snowflake type names to SQLite affinities in DDL
    identifiers.rs    - Strips db.schema.table qualifiers down to just table
    functions.rs      - Rewrites Snowflake functions/operators to SQLite equivalents
tests/
  integration_tests.rs - End-to-end tests exercising DDL, functions, identifiers, transactions
```

## Key Patterns

- **Translation pipeline** (`rewriter.rs`): SQL goes through 5 ordered passes: noop detection -> CREATE OR REPLACE rewrite -> identifier stripping -> type rewriting -> function/operator rewriting -> Snowflake option stripping.
- **Regex-based rewriting**: All translations use `regex::Regex` with `once_cell::sync::Lazy` for compiled-once static patterns. Complex functions (IFF, DECODE, NVL2) use character-by-character parsing to handle nested parentheses.
- **Custom SQLite functions** (`connection.rs`): Functions that can't be rewritten via regex are registered as custom SQLite scalar functions: `regexp`, `split_part`, `strtok`, `object_construct`, `array_construct`, `get_path`, `as_object`, `as_array`, `as_varchar`, `try_parse_json`.
- **Config options**: `TranslatorConfig` has two flags: `use_schema_prefix` (schema.table -> schema__table) and `drop_before_create` (CREATE OR REPLACE -> DROP + CREATE instead of IF NOT EXISTS).
- **Statement splitting** (`rewriter.rs:split_statements`): Splits on `;` while respecting string literals, line comments, and block comments.

## Dependencies

- `rusqlite` (bundled SQLite) - database engine
- `regex` + `once_cell` - SQL rewriting
- `thiserror` - error types
- `serde` + `serde_json` - VARIANT/JSON support
- `chrono` - date/time (declared but not directly used in current code)
- `log` - debug/trace logging of translations
