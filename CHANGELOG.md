# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/cvrajeesh/snowlite/releases/tag/v0.2.0) - 2026-03-10

### Added

- Snowflake-compatible HTTP server for multi-language access (Python, Node.js, Go, etc.) via legacy REST wire protocol ([#8](https://github.com/cvrajeesh/snowlite/pull/8))
- Rust server test suite using in-process tower `oneshot` (78 tests covering connection lifecycle, DML rowcount, noop statements, transactions, string escapes, types, and Snowflake functions) ([#10](https://github.com/cvrajeesh/snowlite/pull/10))

### Fixed

- ZEROIFNULL translation now correctly emits `COALESCE(x, 0)` instead of stripping the argument ([#10](https://github.com/cvrajeesh/snowlite/pull/10))
- Server normalises C-style backslash escapes (`\'`, `\n`, `\t`) emitted by the Python connector's pyformat paramstyle ([#10](https://github.com/cvrajeesh/snowlite/pull/10))
- DML responses now report `total: affected_rows` so connector `cursor.rowcount` is correct ([#10](https://github.com/cvrajeesh/snowlite/pull/10))
- COMMIT/ROLLBACK with no active transaction no longer returns an error ([#10](https://github.com/cvrajeesh/snowlite/pull/10))
- Hardened SQL translation against injection and parsing edge cases ([#7](https://github.com/cvrajeesh/snowlite/pull/7))

### Other

- Comprehensive Snowflake SQL test coverage with documented failure roadmap ([#6](https://github.com/cvrajeesh/snowlite/pull/6))
- Added HTTP server section and Python connector examples to README ([#9](https://github.com/cvrajeesh/snowlite/pull/9))
- Added experimental/vibe coding disclaimer to README ([#10](https://github.com/cvrajeesh/snowlite/pull/10))

## [0.1.0](https://github.com/cvrajeesh/snowlite/releases/tag/v0.1.0) - 2026-03-08

### Added

- initial implementation of Snowflake-to-SQLite local driver

### Fixed

- use raw string r#""# delimiter to allow literal quotes in regex

### Other

- update checkout to v6 in release workflow ([#2](https://github.com/cvrajeesh/snowlite/pull/2))
- add GitHub Actions CI/CD with conventional commits and release-plz ([#1](https://github.com/cvrajeesh/snowlite/pull/1))
- add CLAUDE.md with build commands, architecture, and key patterns
