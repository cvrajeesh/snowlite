# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.3](https://github.com/cvrajeesh/snowlite/compare/v0.2.2...v0.2.3) - 2026-03-11

### Added

- internal stage improvements ([#37](https://github.com/cvrajeesh/snowlite/pull/37))

### Fixed

- resolve math functions, date functions, TRY_CAST, and LAG/LEAD type issues ([#42](https://github.com/cvrajeesh/snowlite/pull/42))

### Other

- add complex Snowflake join integration tests ([#35](https://github.com/cvrajeesh/snowlite/pull/35))

## [0.2.2](https://github.com/cvrajeesh/snowlite/compare/v0.2.1...v0.2.2) - 2026-03-10

### Fixed

- trigger release-binaries via workflow_run instead of release event ([#33](https://github.com/cvrajeesh/snowlite/pull/33))

## [0.2.1](https://github.com/cvrajeesh/snowlite/compare/snowlite-v0.2.0...snowlite-v0.2.1) - 2026-03-10

### Added

- return descriptive Translation error for FLATTEN() ([#28](https://github.com/cvrajeesh/snowlite/pull/28))
- implement Priority 3 — LOW IMPACT / NICE TO HAVE features ([#25](https://github.com/cvrajeesh/snowlite/pull/25))
- implement Priority 2 — MEDIUM IMPACT features ([#23](https://github.com/cvrajeesh/snowlite/pull/23))
- translate GREATEST/LEAST to SQLite scalar MAX/MIN ([#20](https://github.com/cvrajeesh/snowlite/pull/20))

### Fixed

- correct field name from tag_name_pattern to git_tag_name ([#31](https://github.com/cvrajeesh/snowlite/pull/31))
- use package-scoped tag pattern to resolve snowlite rename collision ([#30](https://github.com/cvrajeesh/snowlite/pull/30))
- add git_only = true to resolve release-plz skipping package ([#29](https://github.com/cvrajeesh/snowlite/pull/29))
- remove publish=false from Cargo.toml to unblock automated release PRs ([#27](https://github.com/cvrajeesh/snowlite/pull/27))
- release-plz release-pr skips package when publish=false ([#26](https://github.com/cvrajeesh/snowlite/pull/26))
- disable semver_check and add publish=false to unblock release-plz PR creation ([#24](https://github.com/cvrajeesh/snowlite/pull/24))
- enable git releases in release-plz to unblock PR creation ([#21](https://github.com/cvrajeesh/snowlite/pull/21))

### Other

- add /merge-pr slash command ([#22](https://github.com/cvrajeesh/snowlite/pull/22))
- only run release job on release PR merge or manual trigger ([#19](https://github.com/cvrajeesh/snowlite/pull/19))

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
