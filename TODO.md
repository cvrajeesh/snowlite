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
- [ ] `GREATEST(v1, v2, ...)` — SQLite `MAX()` is aggregate-only; rewrite to `CASE WHEN` chain
- [ ] `LEAST(v1, v2, ...)` — same; `CASE WHEN` chain

---

## Priority 2 — MEDIUM IMPACT

### Date / Time Functions (`src/translator/functions.rs`)
- [ ] `TO_DATE(str, format)` — two-arg form with format string (only single-arg handled today)
- [ ] `TO_CHAR(date, format)` — date-to-formatted-string using `strftime` pattern mapping
- [ ] `TIMESTAMP_FROM_PARTS(y, m, d, hh, mm, ss)` — custom function or `DATETIME(printf(...))`
- [ ] `DATE_FROM_PARTS(y, m, d)` — `DATE(printf('%04d-%02d-%02d', y, m, d))`
- [ ] `TIME_FROM_PARTS(h, m, s)` — `TIME(printf(...))`
- [ ] `LAST_DAY(date)` — SQLite date arithmetic
- [ ] `NEXT_DAY(date, dayname)` — custom function or CASE expression
- [ ] `CONVERT_TIMEZONE(tz, ts)` / three-arg form — document as unsupported (SQLite limitation)
- [ ] `EXTRACT(part FROM expr)` syntax — map to existing `STRFTIME()` rewrites (syntax gap only)

### Semi-Structured Functions (`src/connection.rs`)
- [ ] `OBJECT_KEYS(obj)` — `JSON_EACH(obj)` (tricky in scalar context; may need table-valued approach)
- [ ] `ARRAY_SLICE(arr, start, end)` — custom function using `JSON_EACH`
- [ ] `ARRAY_APPEND(arr, val)` — `JSON_INSERT(arr, '$[#]', val)`
- [ ] `ARRAY_CONCAT(arr1, arr2)` — custom function
- [ ] `ARRAY_COMPACT(arr)` — remove NULLs; custom function
- [ ] `ARRAY_UNIQUE(arr)` — deduplicate; custom function
- [ ] `TYPEOF(variant)` — return `'array'` / `'object'` / `'string'` / `'integer'` etc.; custom function
- [ ] `STRIP_NULL_VALUE(obj)` — remove null-valued keys; custom function
- [ ] `FLATTEN(input, ...)` — document as unsupported (requires lateral join / table-valued function)

### DDL Constructs (`src/translator/rewriter.rs` + `src/translator/noop.rs`)
- [ ] `CREATE TEMPORARY TABLE` — pass through (SQLite supports natively); add test
- [ ] `CREATE TRANSIENT TABLE` — strip `TRANSIENT`, treat as `CREATE TABLE`; add test
- [ ] `ALTER TABLE ... ADD COLUMN` — pass through (SQLite supports); add test
- [ ] `ALTER TABLE ... DROP COLUMN` — pass through (SQLite 3.35+); add test
- [ ] `ALTER TABLE ... RENAME COLUMN old TO new` — pass through (SQLite 3.25+); add test
- [ ] `CREATE DATABASE` / `DROP DATABASE` — add to noop list
- [ ] `ANALYZE` — add to noop list

### Operators (`src/translator/functions.rs` or `src/translator/rewriter.rs`)
- [ ] `::` cast operator (e.g. `val::INTEGER`) — rewrite to `CAST(val AS INTEGER)`

### Statements (no-op list — `src/translator/noop.rs`)
- [ ] `MERGE INTO … USING … WHEN MATCHED` — add to no-op list with a clear error message (SQLite fundamental limitation)

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
