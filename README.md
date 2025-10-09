# df-regexp-extract

Homework repo for a Spark-style `regexp_extract` UDF on DataFusion.

## What works
- Two- and three-argument forms (`idx` defaults to 1); group 0 returns the full match.
- Nulls propagate; unmatched groups return `""`.
- Kernel operates on UTF-8 arrays (LargeUtf8 pending).

## Known gaps
- Advanced regex features (look-around, backrefs, named groups) still rely on Rust’s regex crate and fail the Spark parity tests.

## Commands
```bash
cargo fmt
cargo clippy -- -D warnings
cargo test -q
cargo run --bin demo
```
