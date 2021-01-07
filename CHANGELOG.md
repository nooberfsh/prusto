# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://book.async.rs/overview/stability-guarantees.html).

## [Unreleased]

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

[Unreleased]: https://github.com/nooberfsh/prusto/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/nooberfsh/prusto/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/nooberfsh/prusto/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nooberfsh/prusto/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nooberfsh/prusto/tree/v0.1.0