# Supported SQL Reference

This page documents every Snowflake SQL construct that snowlite translates to SQLite.
If something you need is missing, check [limitations.md](./limitations.md) or open an issue.

---

## Data Types

| Snowflake type | SQLite affinity | Notes |
|---|---|---|
| `NUMBER(p, 0)` / `INT` / `BIGINT` / `SMALLINT` / `BYTEINT` | `INTEGER` | |
| `NUMBER(p, s>0)` / `DECIMAL` / `NUMERIC` | `REAL` | Precision is not enforced |
| `FLOAT` / `FLOAT4` / `FLOAT8` / `DOUBLE` / `DOUBLE PRECISION` | `REAL` | |
| `VARCHAR(n)` / `NVARCHAR` / `STRING` / `TEXT` / `CHAR` / `NCHAR` | `TEXT` | Length limit not enforced |
| `BOOLEAN` | `INTEGER` | `0` = false, `1` = true |
| `DATE` | `TEXT` | ISO-8601 `YYYY-MM-DD` |
| `TIME` | `TEXT` | `HH:MM:SS` |
| `TIMESTAMP` / `TIMESTAMP_NTZ` / `TIMESTAMP_LTZ` / `TIMESTAMP_TZ` / `DATETIME` | `TEXT` | ISO-8601 datetime |
| `VARIANT` / `OBJECT` / `ARRAY` | `TEXT` | JSON stored as text |
| `BINARY` / `VARBINARY` / `BYTES` | `BLOB` | |

Column options `COMMENT`, `COLLATE`, `AUTOINCREMENT`, `DEFAULT seq.NEXTVAL` are stripped silently.

---

## DDL

| Snowflake | SQLite translation |
|---|---|
| `CREATE OR REPLACE TABLE t (…)` | `CREATE TABLE IF NOT EXISTS t (…)` (default) |
| `CREATE OR REPLACE TABLE t (…)` | `DROP TABLE IF EXISTS t; CREATE TABLE t (…)` (with `drop_before_create`) |
| `CREATE TRANSIENT TABLE t (…)` | `CREATE TABLE t (…)` (`TRANSIENT` stripped) |
| `ALTER TABLE t ADD COLUMN …` | passed through (SQLite native) |
| `ALTER TABLE t DROP COLUMN …` | passed through (SQLite 3.35+) |
| `ALTER TABLE t RENAME COLUMN old TO new` | passed through (SQLite 3.25+) |
| `CLUSTER BY (…)` | stripped |
| `COMMENT = '…'` | stripped |
| `DATA_RETENTION_TIME_IN_DAYS = n` | stripped |
| `CHANGE_TRACKING = TRUE` | stripped |
| `ENABLE_SCHEMA_EVOLUTION = TRUE` | stripped |

---

## Functions

### NULL handling

| Snowflake | SQLite equivalent |
|---|---|
| `NVL(a, b)` | `COALESCE(a, b)` |
| `NVL2(a, b, c)` | `CASE WHEN a IS NOT NULL THEN b ELSE c END` |
| `ZEROIFNULL(a)` | `COALESCE(a, 0)` |
| `NULLIFZERO(a)` | `NULLIF(a, 0)` |
| `EMPTYTONULL(a)` | `NULLIF(a, '')` |

### Conditional

| Snowflake | SQLite equivalent |
|---|---|
| `IFF(cond, t, f)` | `CASE WHEN cond THEN t ELSE f END` |
| `DECODE(expr, s1,r1,…,default)` | `CASE expr WHEN s1 THEN r1 … ELSE default END` |

### Type conversion

| Snowflake | SQLite equivalent |
|---|---|
| `TO_VARCHAR(a)` / `TO_CHAR(a)` | `CAST(a AS TEXT)` |
| `TO_NUMBER(a)` / `TO_DECIMAL(a)` / `TO_NUMERIC(a)` / `TO_DOUBLE(a)` | `CAST(a AS REAL)` |
| `TO_BOOLEAN(a)` | `CAST(a AS INTEGER)` |
| `TO_BINARY(a)` | `CAST(a AS BLOB)` |
| `TO_DATE(a)` | `DATE(a)` |
| `TO_TIME(a)` | `TIME(a)` |
| `TO_TIMESTAMP(a)` / `TO_TIMESTAMP_NTZ(a)` / `TO_TIMESTAMP_LTZ(a)` / `TO_TIMESTAMP_TZ(a)` | `DATETIME(a)` |

### Date and time

| Snowflake | SQLite equivalent |
|---|---|
| `CURRENT_TIMESTAMP()` / `CURRENT_TIMESTAMP` | `DATETIME('now')` |
| `GETDATE()` / `SYSDATE()` | `DATETIME('now')` |
| `CURRENT_DATE()` / `CURRENT_DATE` | `DATE('now')` |
| `CURRENT_TIME()` / `LOCALTIME()` | `TIME('now')` |
| `LOCALTIMESTAMP()` | `DATETIME('now')` |
| `DATEADD(unit, n, d)` | `DATE(d, n \|\| ' days')` etc. (all units: year/quarter/month/week/day/hour/minute/second) |
| `DATEDIFF(unit, d1, d2)` | `JULIANDAY(d2) - JULIANDAY(d1)` etc. |
| `DATE_TRUNC('month', d)` | `DATE(d, 'start of month')` etc. |
| `YEAR(d)` | `CAST(STRFTIME('%Y', d) AS INTEGER)` |
| `MONTH(d)` | `CAST(STRFTIME('%m', d) AS INTEGER)` |
| `DAY(d)` | `CAST(STRFTIME('%d', d) AS INTEGER)` |
| `HOUR(d)` | `CAST(STRFTIME('%H', d) AS INTEGER)` |
| `MINUTE(d)` | `CAST(STRFTIME('%M', d) AS INTEGER)` |
| `SECOND(d)` | `CAST(STRFTIME('%S', d) AS INTEGER)` |
| `DAYOFWEEK(d)` | `CAST(STRFTIME('%w', d) AS INTEGER)` |
| `DAYOFYEAR(d)` | `CAST(STRFTIME('%j', d) AS INTEGER)` |
| `WEEKOFYEAR(d)` | `CAST(STRFTIME('%W', d) AS INTEGER)` |
| `QUARTER(d)` | `((CAST(STRFTIME('%m', d) AS INTEGER) + 2) / 3)` |

### String

| Snowflake | Mechanism | Notes |
|---|---|---|
| `CONTAINS(s, sub)` | translator rule | → `INSTR(s, sub) > 0` |
| `STARTSWITH(s, prefix)` | translator rule | → `s LIKE prefix \|\| '%'` |
| `ENDSWITH(s, suffix)` | translator rule | → `s LIKE '%' \|\| suffix` |
| `CHARINDEX(sub, s)` / `STRPOS(s, sub)` | translator rule | → `INSTR(s, sub)` |
| `SPACE(n)` | translator rule | → `SUBSTR('…40 spaces…', 1, n)` |
| `LTRIM(s)` / `RTRIM(s)` / `TRIM(s)` | SQLite native | |
| `UPPER(s)` / `LOWER(s)` / `LENGTH(s)` | SQLite native | |
| `SUBSTR(s, pos, len)` / `SUBSTRING(s, pos, len)` | SQLite native | |
| `REPLACE(s, from, to)` | SQLite native | |
| `SPLIT_PART(s, delim, n)` | custom function | 1-based part number |
| `STRTOK(s, delims, n)` | custom function | multi-char delimiter set |
| `REGEXP_LIKE(s, pattern)` | custom function | boolean; text-first arg order |
| `s RLIKE pattern` | translator rule | → `s REGEXP pattern` (SQLite operator) |
| `REGEXP_REPLACE(s, pattern, replacement)` | custom function | replaces all matches; capture groups via `$1` |
| `REGEXP_SUBSTR(s, pattern [, pos [, n]])` | custom function | returns nth occurrence; `NULL` on no match |
| `LPAD(s, len [, pad])` | custom function | pads with spaces or custom string; truncates if longer |
| `RPAD(s, len [, pad])` | custom function | same, right-side |
| `INITCAP(s)` | custom function | capitalises first letter of each word |
| `REPEAT(s, n)` | custom function | returns `''` for n ≤ 0 |
| `REVERSE(s)` | custom function | unicode-safe |
| `CONCAT_WS(sep, s1, s2, …)` | custom function | variadic; skips `NULL` arguments |

### Boolean and bitwise

| Snowflake | SQLite equivalent |
|---|---|
| `BOOLAND(a, b)` | `(a AND b)` |
| `BOOLOR(a, b)` | `(a OR b)` |
| `BOOLXOR(a, b)` | `((a OR b) AND NOT (a AND b))` |
| `BITAND(a, b)` | `(a & b)` |
| `BITOR(a, b)` | `(a \| b)` |
| `BITXOR(a, b)` | `(a ^ b)` |
| `BITSHIFTLEFT(a, n)` | `(a << n)` |
| `BITSHIFTRIGHT(a, n)` | `(a >> n)` |

### Math

| Snowflake | SQLite equivalent |
|---|---|
| `DIV0(a, b)` | `CASE WHEN b = 0 THEN 0 ELSE a / b END` |
| `DIV0NULL(a, b)` | `CASE WHEN b = 0 THEN NULL ELSE a / b END` |
| `SQUARE(x)` | `((x) * (x))` |
| `CBRT(x)` | `POWER(x, 1.0/3.0)` |
| `LN(x)` | `LOG(x)` |
| `ABS` / `ROUND` / `MOD` / `POWER` / `SQRT` / `CEIL` / `FLOOR` | SQLite native | |

### Aggregate

| Snowflake | SQLite equivalent | Notes |
|---|---|---|
| `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` | SQLite native | |
| `COUNT(DISTINCT …)` | SQLite native | |
| `LISTAGG(expr, sep) WITHIN GROUP (ORDER BY …)` | `GROUP_CONCAT(expr, sep)` | `ORDER BY` inside LISTAGG not supported |

### Window / Analytic

All Snowflake window functions pass through the translator unchanged and are executed by SQLite's
native window function engine (requires SQLite ≥ 3.25.0, which rusqlite bundles).

| Function | Example |
|---|---|
| `ROW_NUMBER()` | `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)` |
| `RANK()` | `RANK() OVER (ORDER BY score DESC)` |
| `DENSE_RANK()` | `DENSE_RANK() OVER (ORDER BY score DESC)` |
| `NTILE(n)` | `NTILE(4) OVER (ORDER BY val)` |
| `LAG(expr, offset, default)` | `LAG(revenue, 1, 0) OVER (ORDER BY period)` |
| `LEAD(expr, offset, default)` | `LEAD(revenue, 1, 0) OVER (ORDER BY period)` |
| `FIRST_VALUE(expr)` | `FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` |
| `LAST_VALUE(expr)` | same frame spec as `FIRST_VALUE` |
| `NTH_VALUE(expr, n)` | `NTH_VALUE(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` |
| `SUM / AVG / COUNT` with frame | `SUM(amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |

### Semi-structured (VARIANT / JSON)

| Snowflake | Mechanism | Notes |
|---|---|---|
| `col:field` | translator rule | → `JSON_EXTRACT(col, '$.field')` |
| `col:field.sub` | translator rule | → `JSON_EXTRACT(col, '$.field.sub')` |
| `col['field']` | translator rule | → `JSON_EXTRACT(col, '$.field')` |
| `col[n]` | translator rule | → `JSON_EXTRACT(col, '$[n]')` |
| `PARSE_JSON(s)` | translator rule | passthrough (value is stored as JSON text) |
| `TRY_PARSE_JSON(s)` | custom function | passthrough |
| `ARRAY_SIZE(arr)` / `ARRAY_LENGTH(arr)` | translator rule | → `JSON_ARRAY_LENGTH(arr)` |
| `ARRAY_CONSTRUCT(v1, v2, …)` | custom function | → JSON array text |
| `OBJECT_CONSTRUCT(k1, v1, …)` | custom function | → JSON object text |
| `GET_PATH(col, 'a.b.c')` | custom function | dot-separated path lookup |
| `AS_OBJECT(v)` / `AS_ARRAY(v)` / `AS_VARCHAR(v)` | custom function | passthrough VARIANT casts |

---

## Operators and Syntax

| Snowflake | SQLite translation |
|---|---|
| `col ILIKE pattern` | `LOWER(col) LIKE LOWER(pattern)` |
| `col RLIKE pattern` | `col REGEXP pattern` (calls `regexp()` custom function) |
| `SELECT TOP n …` | `SELECT … LIMIT n` |

---

## No-op Statements (silently ignored)

The following statements are recognised and silently dropped rather than causing errors:

- `USE DATABASE / SCHEMA / WAREHOUSE / ROLE`
- `ALTER SESSION …` / `ALTER WAREHOUSE …` / `ALTER ACCOUNT …`
- `CREATE / DROP / ALTER / SUSPEND / RESUME WAREHOUSE`
- `SHOW TABLES / SCHEMAS / COLUMNS / …`
- `COPY INTO …`
- `CREATE / DROP / ALTER STAGE / PIPE / STREAM / TASK`
- `PUT FILE … / GET @… / REMOVE @…`
- `GRANT … / REVOKE …`
- `CREATE / DROP ROLE`
- `CREATE RESOURCE MONITOR`
- `COMMENT ON …`
- `SET var = value` / `UNSET var`
