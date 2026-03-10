# snowlite: Missing Features TODO

Full audit of the codebase against common Snowflake SQL usage patterns.
The library is mature and well-tested (~140 integration tests, 60+ functions translated),
but the constructs below are either untranslated (will error) or produce wrong output.

Items are grouped by priority. Each checkbox represents one implementable unit of work.
Each item should be accompanied by at least one integration test in `tests/integration_tests.rs`.

---

## Priority 1 — HIGH IMPACT

These are hit in most real Snowflake workloads.

### Window / Analytic Functions
SQLite supports window functions natively (since 3.25.0), so Snowflake syntax may pass through
unchanged — but this is **untested** end-to-end.

- [x] `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` — integration test added
- [x] `RANK()`, `DENSE_RANK()`, `NTILE(n)` — integration tests added
- [x] `LAG(expr, offset, default) OVER (...)` / `LEAD(...)` — integration tests added
- [x] `FIRST_VALUE()` / `LAST_VALUE()` with frame specs — integration tests added
- [x] `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame boundaries — integration test added
- [x] `NTH_VALUE(expr, n)` — integration test added

### String Functions (`src/translator/functions.rs` + `src/connection.rs`)
- [x] `REGEXP_REPLACE(str, pattern, replacement)` — custom SQLite function added
- [x] `REGEXP_SUBSTR(str, pattern [, pos, occurrence])` — custom SQLite function added
- [x] `REGEXP_LIKE(str, pattern)` — custom SQLite function added
- [x] `RLIKE` infix operator — translator rewrites to `REGEXP` (SQLite operator)
- [x] `LPAD(str, len [, pad])` — custom SQLite function added
- [x] `RPAD(str, len [, pad])` — custom SQLite function added
- [x] `INITCAP(str)` — custom SQLite function added
- [x] `REPEAT(str, n)` — custom SQLite function added
- [x] `REVERSE(str)` — custom SQLite function added
- [x] `CONCAT_WS(sep, s1, s2, ...)` — custom SQLite function added (skips NULLs)
- [x] `REPLACE(str, from, to)` — SQLite native, integration test added

### LISTAGG (`src/translator/functions.rs`)
- [x] `LISTAGG(expr, delim) WITHIN GROUP (ORDER BY ...)` — rewrites to `GROUP_CONCAT(expr, delim)`; `WITHIN GROUP` clause consumed and dropped (SQLite limitation: ORDER BY inside GROUP_CONCAT not supported)

### GREATEST / LEAST (`src/translator/functions.rs`)
- [x] `GREATEST(v1, v2, ...)` — SQLite scalar `MAX(v1, v2, ...)` (multi-arg form); integration tests added
- [x] `LEAST(v1, v2, ...)` — SQLite scalar `MIN(v1, v2, ...)`; integration tests added

---

## Priority 2 — MEDIUM IMPACT

### Date / Time Functions (`src/translator/functions.rs`)
- [x] `TO_DATE(str, format)` — custom SQLite function; format arg silently ignored; integration test added
- [x] `TO_CHAR(date, format)` — custom SQLite function; maps common Snowflake tokens to strftime; integration tests added
- [x] `TIMESTAMP_FROM_PARTS(y, m, d, hh, mm, ss)` — translator rewrite to `DATETIME(PRINTF(...))`; integration test added
- [x] `DATE_FROM_PARTS(y, m, d)` — translator rewrite to `DATE(PRINTF('%04d-%02d-%02d', ...))`; integration test added
- [x] `TIME_FROM_PARTS(h, m, s)` — translator rewrite to `TIME(PRINTF(...))`; integration test added
- [x] `LAST_DAY(date)` — simple rule: `DATE(date, 'start of month', '+1 month', '-1 day')`; integration tests added
- [x] `NEXT_DAY(date, dayname)` — custom SQLite function using Julian Day arithmetic; integration test added
- [x] `CONVERT_TIMEZONE(tz, ts)` / three-arg form — custom function; returns timestamp unchanged (SQLite limitation); integration test added
- [x] `EXTRACT(part FROM expr)` syntax — translator rewrite to `CAST(STRFTIME(...) AS INTEGER)`; integration tests added

### Semi-Structured Functions (`src/connection.rs`)
- [x] `OBJECT_KEYS(obj)` — custom function; returns JSON array of top-level keys; integration test added
- [x] `ARRAY_SLICE(arr, start, end)` — custom function; 0-indexed slice [start, end); integration test added
- [x] `ARRAY_APPEND(arr, val)` — custom function; appends to JSON array; integration test added
- [x] `ARRAY_CONCAT(arr1, arr2)` — custom function; concatenates two JSON arrays; integration test added
- [x] `ARRAY_COMPACT(arr)` — custom function; removes null elements; integration test added
- [x] `ARRAY_UNIQUE(arr)` — custom function; deduplicates preserving first occurrence; integration test added
- [x] `TYPEOF(variant)` — translator rewrites to `snowflake_typeof()`; custom function returns Snowflake-style type names; integration tests added
- [x] `STRIP_NULL_VALUE(obj)` — custom function; removes null-valued keys from JSON object; integration test added
- [ ] `FLATTEN(input, ...)` — document as unsupported (requires lateral join / table-valued function)

### DDL Constructs (`src/translator/rewriter.rs` + `src/translator/noop.rs`)
- [x] `CREATE TEMPORARY TABLE` — passes through to SQLite natively; integration test added
- [x] `CREATE TRANSIENT TABLE` — simple rule strips `TRANSIENT`; integration test added
- [x] `ALTER TABLE ... ADD COLUMN` — passes through; integration test added
- [x] `ALTER TABLE ... RENAME COLUMN old TO new` — passes through (SQLite 3.25+); integration test added
- [ ] `ALTER TABLE ... DROP COLUMN` — passes through (SQLite 3.35+); add test
- [x] `CREATE DATABASE` / `DROP DATABASE` — added to noop list; integration tests added
- [x] `ANALYZE` — added to noop list; integration test added

### Operators (`src/translator/functions.rs` or `src/translator/rewriter.rs`)
- [x] `::` cast operator (e.g. `val::INTEGER`) — regex rewrite to `CAST(val AS INTEGER)`; integration tests added

### Statements (no-op list — `src/translator/noop.rs`)
- [x] `MERGE INTO … USING … WHEN MATCHED` — added to no-op list; integration test added

---

## Priority 3 — LOW IMPACT / NICE TO HAVE

### Math Functions
- [ ] `LOG(base, x)` two-arg form — `LOG(x) / LOG(base)` (SQLite only has natural log)
- [ ] `RANDOM()` — already works via SQLite; add test to document behaviour
- [ ] `WIDTH_BUCKET(val, min, max, buckets)` — custom function

### Aggregate Functions
- [ ] `MEDIAN(expr)` — custom aggregate SQLite function
- [ ] `ANY_VALUE(expr)` — map to `MIN(expr)` (acceptable approximation for testing)
- [ ] `APPROX_COUNT_DISTINCT(expr)` — map to `COUNT(DISTINCT expr)`
- [ ] `ARRAY_AGG(expr)` — map to `JSON_GROUP_ARRAY(expr)` (SQLite 3.38+)
- [ ] `OBJECT_AGG(key, val)` — map to `JSON_GROUP_OBJECT(key, val)` (SQLite 3.38+)

### Type System (`src/types.rs` + `src/row.rs`)
- [ ] `GEOGRAPHY` / `GEOMETRY` types — map to `TEXT` in type rewriter with a `log::warn!`
- [ ] `FromValue` impls for `i16`, `u32`, `i8`, `u8` in `src/row.rs`
- [ ] Convenience `FromValue` for `serde_json::Value` (deserialize from `Value::Text`)

### Code Quality
- [ ] Remove unused `chrono` dependency from `Cargo.toml`
- [ ] Fix `SELECT TOP N ... ORDER BY col` rewrite — currently emits `SELECT ... LIMIT N ORDER BY col`
      (invalid SQL); should become `SELECT ... ORDER BY col LIMIT N` (`src/translator/rewriter.rs`)
- [ ] Add fuzz testing for translator regex patterns (prevent ReDoS on adversarial SQL)
- [ ] Add query timeout / statement size limits in `src/connection.rs`

### Bug Fixes (known incorrect behaviour)
- [ ] `LPAD`/`RPAD` with empty pad string — currently returns original string; Snowflake raises an error (`src/connection.rs`)
- [ ] `get_path(col, 'a.b')` multi-segment paths — identifier stripper corrupts dotted paths; fix by protecting string literal arguments in the identifier stripper (`src/translator/identifiers.rs`)
- [ ] Decimal precision — `NUMBER(p, s)` stored as SQLite `REAL` (64-bit float); document clearly and add a test showing the precision limit
- [ ] String collation — `COLLATE` clauses are stripped; add a test documenting where case-sensitivity differs from Snowflake
- [ ] Recursive CTEs — SQLite recursion depth limit differs from Snowflake; add a test to document the behaviour

### Unsupported (document-only, no fix possible in SQLite)
- [ ] `CONVERT_TIMEZONE` — document workaround clearly in limitations.md (already done)
- [ ] `FLATTEN` / lateral joins — document workaround clearly in limitations.md (already done)
- [ ] `MERGE INTO` — emit a descriptive error rather than a cryptic SQLite parse error

---

## Files Affected (Reference)

| File | What changes |
|------|-------------|
| `src/translator/functions.rs` | REGEXP_LIKE, GREATEST/LEAST, LISTAGG, `::` cast, EXTRACT, string functions, date constructors |
| `src/translator/rewriter.rs` | TOP N + ORDER BY fix, TRANSIENT TABLE, `::` cast operator |
| `src/translator/noop.rs` | CREATE/DROP DATABASE, ANALYZE |
| `src/connection.rs` | New custom functions: REGEXP_REPLACE, REGEXP_SUBSTR, LPAD, RPAD, INITCAP, REPEAT, REVERSE, ARRAY_SLICE, ARRAY_APPEND, ARRAY_CONCAT, ARRAY_COMPACT, TYPEOF, MEDIAN, ARRAY_AGG |
| `src/row.rs` | Additional `FromValue` impls |
| `src/types.rs` | GEOGRAPHY/GEOMETRY → TEXT mapping |
| `Cargo.toml` | Remove unused `chrono` |
| `tests/integration_tests.rs` | Tests for all of the above |
