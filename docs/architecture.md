# Architecture

This document explains how snowlite translates Snowflake SQL to SQLite SQL.

---

## High-level flow

```
Your Snowflake SQL
        │
        ▼
  ┌─────────────┐
  │ Translator  │  (src/translator/)
  │             │
  │  1. No-op?  │──► silently return None
  │  2. DDL     │
  │  3. Idents  │
  │  4. Types   │
  │  5. Funcs   │
  │  6. Options │
  └─────────────┘
        │
        ▼
   SQLite SQL
        │
        ▼
  ┌─────────────┐
  │  rusqlite   │  (bundled SQLite)
  │             │
  │  + custom   │  regexp, split_part, lpad, rpad,
  │  functions  │  initcap, repeat, reverse, …
  └─────────────┘
        │
        ▼
    Vec<Row>
```

---

## Source layout

```
src/
  lib.rs              — crate root; re-exports Connection, Config, Row, Value, Error
  connection.rs       — Connection struct; SQL execution; custom SQLite function registration
  row.rs              — Row type with by-index and by-name access; FromValue trait
  types.rs            — Value enum (Null/Integer/Real/Text/Blob/Boolean) with ToSql impl
  error.rs            — Error enum (Sqlite, Translation, TypeConversion, …)
  translator/
    mod.rs            — public translate() and translate_batch() functions
    rewriter.rs       — Translator struct; orchestrates the 6-pass pipeline; statement splitter
    noop.rs           — detects statements to silently ignore
    types.rs          — rewrites Snowflake type names → SQLite affinities in DDL
    identifiers.rs    — strips db.schema.table qualifiers
    functions.rs      — rewrites Snowflake functions/operators → SQLite equivalents
```

---

## Translation pipeline

Each SQL statement passes through 6 ordered passes inside `Translator::translate()`.

### Pass 1 — No-op detection (`noop.rs`)

Checks whether the statement matches a list of Snowflake-only constructs that have no
SQLite equivalent and should be silently ignored:
`USE`, `ALTER SESSION`, `SHOW`, `COPY INTO`, `GRANT`, `SET`, etc.

Returns `None` if the statement is a no-op (caller skips execution entirely).

### Pass 2 — DDL rewriting (`rewriter.rs`)

Transforms `CREATE OR REPLACE TABLE` into either:
- `CREATE TABLE IF NOT EXISTS` (default), or
- `DROP TABLE IF EXISTS; CREATE TABLE` (`drop_before_create` mode)

Also strips `TRANSIENT` from `CREATE TRANSIENT TABLE`.

### Pass 3 — Identifier stripping (`identifiers.rs`)

Strips multi-part qualifiers from table and column references:
- `db.schema.table` → `table`
- `schema.table` → `table` (default) or `schema__table` (`use_schema_prefix` mode)

Handles both quoted (`"MY_DB"."SCHEMA"."TABLE"`) and unquoted identifiers.

### Pass 4 — Type rewriting (`types.rs`)

Rewrites Snowflake column types in DDL to SQLite affinities:
`NUMBER(18,0)` → `INTEGER`, `VARCHAR(255)` → `TEXT`, `VARIANT` → `TEXT`, etc.

### Pass 5 — Function rewriting (`functions.rs`)

The heaviest pass. Applies ~60 translation rules in two categories:

**Simple regex rules** (`SIMPLE_RULES` static) — one-to-one pattern replacements using
`Regex::replace_all`. Examples: `NVL(` → `COALESCE(`, `RLIKE` → `REGEXP`,
`YEAR(x)` → `CAST(STRFTIME('%Y', x) AS INTEGER)`.

**Complex parsers** — functions that require counting nested parentheses to correctly
split arguments. Implemented as character-by-character parsers:
- `IFF(cond, true_val, false_val)` → `CASE WHEN … THEN … ELSE … END`
- `DECODE(expr, s1, r1, …, default)` → `CASE … WHEN … THEN … END`
- `NVL2(a, b, c)` → `CASE WHEN a IS NOT NULL THEN b ELSE c END`
- `DATEADD(unit, n, date)` → SQLite date arithmetic
- `DATEDIFF(unit, d1, d2)` → `JULIANDAY` subtraction
- `DATE_TRUNC(unit, date)` → SQLite `strftime` modifiers
- Semi-structured path access (`col:field`, `col['key']`, `col[n]`)
- `ILIKE` operator → `LOWER(a) LIKE LOWER(b)`
- `SELECT TOP n` → `SELECT … LIMIT n`

### Pass 6 — Snowflake option stripping (`rewriter.rs`)

Strips column/table options that SQLite doesn't understand:
`AUTOINCREMENT`, `COMMENT = '…'`, `CLUSTER BY (…)`, `COLLATE '…'`,
`DEFAULT seq.NEXTVAL`, `DATA_RETENTION_TIME_IN_DAYS`, etc.

---

## Custom SQLite functions

Functions that cannot be expressed as regex rewrites are registered as custom SQLite
scalar functions in `connection.rs → register_custom_functions()`.

| Function | Arity | Purpose |
|---|---|---|
| `regexp(pattern, text)` | 2 | Powers `col REGEXP pattern` and `RLIKE` |
| `regexp_like(text, pattern)` | 2 | Snowflake `REGEXP_LIKE` (text-first arg order) |
| `regexp_replace(text, pat, repl)` | variadic | Replace all regex matches |
| `regexp_substr(text, pat [, pos [, n]])` | variadic | Extract nth occurrence |
| `split_part(str, delim, n)` | 3 | Split on delimiter, return nth part |
| `strtok(str, delims, n)` | 3 | Split on any character in `delims` |
| `lpad(str, len [, pad])` | variadic | Left-pad to length |
| `rpad(str, len [, pad])` | variadic | Right-pad to length |
| `initcap(str)` | 1 | Capitalise first letter of each word |
| `repeat(str, n)` | 2 | Repeat string n times |
| `reverse(str)` | 1 | Reverse character order (unicode-safe) |
| `concat_ws(sep, …)` | variadic | Join non-NULL args with separator |
| `object_construct(k1,v1,…)` | variadic | Build a JSON object |
| `array_construct(v1,…)` | variadic | Build a JSON array |
| `get_path(json, 'a.b.c')` | 2 | Dot-separated JSON path lookup |
| `as_object(v)` / `as_array(v)` / `as_varchar(v)` | 1 | Passthrough VARIANT casts |
| `try_parse_json(v)` | 1 | Passthrough JSON parse |

All regex-based custom functions use `RegexBuilder` with a 1 MiB compiled-size limit
and 1 MiB DFA-size limit to prevent memory exhaustion from adversarial patterns.

---

## Statement splitting

`split_statements()` in `rewriter.rs` splits a batch of SQL on `;` while correctly
handling:
- Single-quoted string literals (`'it''s fine'`)
- Line comments (`-- comment`)
- Block comments (`/* comment */`)

This is used by `execute_batch()` and the `DROP + CREATE` rewrite.

---

## Design decisions

**Regex-based translation** — Simple and fast. All static `Regex` patterns are compiled
once via `once_cell::sync::Lazy`. The downside is that deeply nested or unusual SQL can
confuse the regex patterns; an AST-based approach would be more robust but far more
complex.

**Custom SQLite functions over rewrites** — Functions with complex semantics (e.g.
`REGEXP_REPLACE` with capture groups, `LPAD` with multi-char pad strings) are
implemented as Rust closures registered with rusqlite rather than attempted as regex
rewrites. This gives full control over edge cases.

**Passthrough for unknown constructs** — If the translator doesn't recognise a
construct, it passes it through unchanged. SQLite may or may not understand it. This is
intentional: it means that SQLite-compatible SQL in a Snowflake codebase works without
any translator rules. Window functions are the clearest example — they pass through
unchanged and SQLite executes them natively.
