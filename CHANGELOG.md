# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://book.async.rs/overview/stability-guarantees.html).

## [Unreleased]

## [0.5.2](https://github.com/nooberfsh/prusto/compare/prusto-v0.5.1...prusto-v0.5.2) - 2024-12-06

### Added

- add timestamp with timezone support
- add json support

### Fixed

- fix cargo clippy warnings

### Other

- add auth info to the next_uri request
- support up to 32 fields for Structs
- Adds visiting unit type for optional data
- allow unverified certificates
- update changelog

## [0.5.1] - 2023-10-19
- Make Client::get and some functions public [#29](https://github.com/nooberfsh/prusto/pull/29)

## [0.5.0] - 2023-02-27
- v0.5.0 can be used with stable rust.
- Add SSL root certificate support [#22](https://github.com/nooberfsh/prusto/pull/22)
- Provide a feature flag for running as presto client [#19](https://github.com/nooberfsh/prusto/pull/19)

## [0.4.0] - 2022-02-07
- Use `Rust 2021`

## [0.3.0] - 2021-05-26
- Use `Trino` protocol
- Add `execute` to `Client`
- Add more session properties
- Fix deserialization of `ClientTypeSignatureParameter`

## [0.2.0] - 2021-01-06
- Add `len`, `as_slice` methods to `DataSet<T>`
- Update `tokio` stack to 1.0
- Use `rustls` instead of `native-tls`

## [0.1.2] - 2020-10-30
-  Make `QueryError::error_location` optional

## [0.1.1] - 2020-10-09
- Add `'static` bound to key and value types of map like types

## [0.1.0] - 2020-10-01
- Initial release

[Unreleased]: https://github.com/nooberfsh/prusto/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/nooberfsh/prusto/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/nooberfsh/prusto/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nooberfsh/prusto/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nooberfsh/prusto/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nooberfsh/prusto/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/nooberfsh/prusto/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nooberfsh/prusto/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nooberfsh/prusto/tree/v0.1.0