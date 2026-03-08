# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/cvrajeesh/local-db/releases/tag/v0.1.0) - 2026-03-08

### Added

- initial implementation of Snowflake-to-SQLite local driver

### Fixed

- use raw string r#""# delimiter to allow literal quotes in regex

### Other

- update checkout to v6 in release workflow ([#2](https://github.com/cvrajeesh/local-db/pull/2))
- add GitHub Actions CI/CD with conventional commits and release-plz ([#1](https://github.com/cvrajeesh/local-db/pull/1))
- add CLAUDE.md with build commands, architecture, and key patterns
