# Known Limitations

snowlite is a best-effort translator, not a full Snowflake emulator.
The items below are known gaps. PRs welcome — see [TODO.md](../TODO.md) for the tracked backlog.

---

## Fundamental SQLite constraints

These gaps exist because SQLite has no equivalent feature. They cannot be worked around
by the translator alone.

| Feature | Notes |
|---|---|
| `FLATTEN(input, …)` | Requires a lateral/table-valued function join — SQLite does not support this |
| `MERGE INTO … USING … WHEN MATCHED` | Not supported in SQLite |
| `CONVERT_TIMEZONE(tz, ts)` | SQLite has no timezone database |
| `GEOGRAPHY` / `GEOMETRY` types | No spatial support in SQLite |
| Recursive CTEs with Snowflake semantics | SQLite's recursion depth limit may differ |

---

## Missing translator rules

These Snowflake constructs are not yet translated. They will likely produce a SQLite error.

### Date / time

| Missing | Workaround |
|---|---|
| `TO_DATE(str, format)` — two-arg form | Use `DATE(str)` (ISO-8601 input) |
| `TO_CHAR(date, format)` — format string | Use `STRFTIME(format, date)` directly |
| `TIMESTAMP_FROM_PARTS(y,m,d,h,m,s)` | Use `DATETIME(printf(…))` |
| `DATE_FROM_PARTS(y, m, d)` | Use `DATE(printf('%04d-%02d-%02d', y, m, d))` |
| `LAST_DAY(date)` | Not yet implemented |
| `EXTRACT(part FROM expr)` syntax | Use `YEAR(d)`, `MONTH(d)`, etc. instead |

### String

| Missing | Workaround |
|---|---|
| `CONCAT_WS` with non-string args | Cast args to TEXT first |

### Aggregate

| Missing | Notes |
|---|---|
| `MEDIAN(expr)` | No built-in equivalent in SQLite |
| `APPROX_COUNT_DISTINCT(expr)` | Use `COUNT(DISTINCT expr)` (exact, not approximate) |
| `ARRAY_AGG(expr)` | Use `JSON_GROUP_ARRAY(expr)` directly (SQLite 3.38+) |
| `OBJECT_AGG(key, val)` | Use `JSON_GROUP_OBJECT(key, val)` (SQLite 3.38+) |
| `LISTAGG ORDER BY` inside | `GROUP_CONCAT` does not support internal `ORDER BY`; order is non-deterministic |

### Semi-structured

| Missing | Notes |
|---|---|
| `OBJECT_KEYS(obj)` | No scalar equivalent; use `JSON_EACH` table function directly |
| `ARRAY_SLICE(arr, start, end)` | Not yet implemented |
| `ARRAY_APPEND(arr, val)` | Use `JSON_INSERT(arr, '$[#]', val)` directly |
| `ARRAY_CONCAT(arr1, arr2)` | Not yet implemented |
| `ARRAY_COMPACT(arr)` | Not yet implemented |
| `TYPEOF(variant)` | Not yet implemented |

### Operators

| Missing | Workaround |
|---|---|
| `::` cast operator (`val::INTEGER`) | Use `CAST(val AS INTEGER)` |

### DDL

| Missing | Notes |
|---|---|
| `CREATE DATABASE` / `DROP DATABASE` | Not yet in the no-op list; will cause an error |

---

## Known incorrect behaviour

| Issue | Details |
|---|---|
| `SELECT TOP n … ORDER BY col` | The current rewrite produces `SELECT … LIMIT n ORDER BY col` (invalid SQL). Use `SELECT … ORDER BY col LIMIT n` directly until this is fixed. |
| `LPAD` / `RPAD` with empty pad string | Returns the original string unchanged (Snowflake raises an error) |
| `get_path('a.b', path)` | Multi-segment paths in `get_path()` are corrupted by the identifier stripper (dots are treated as qualifiers). Use `JSON_EXTRACT(col, '$.a.b')` directly. |
| Decimal precision | All `NUMBER(p, s)` values are stored as SQLite `REAL` (64-bit float). Arbitrary-precision arithmetic is not supported. |
| String collation | `COLLATE` clauses are stripped; case-sensitivity rules differ from Snowflake. |

---

## Thread safety

`Connection` is **not** `Send` or `Sync` — this mirrors `rusqlite::Connection`.
Create one `Connection` per thread or task. Do not share across threads.
