# df-regexp-extract

Spark-compatible `regexp_extract` as a DataFusion scalar UDF, written in Rust.

## Behavior

- `regexp_extract(str, pattern[, idx])`. `idx` defaults to 1. `idx = 0` returns the full match.
- No match or a missing group returns `""`, not NULL. NULL inputs return NULL.
- A negative `idx` is an error.
- Input and pattern accept `Utf8`, `LargeUtf8` and `Utf8View`, as scalars or columns. `idx` is `Int32`. Output is `Utf8View`.
- Patterns compile with the `regex` crate first. With the `pcre2-fallback` feature (on by default), patterns `regex` rejects (lookaround, backreferences) fall back to PCRE2 with match, depth and heap limits.
- Column patterns are compiled once per batch through an LRU cache.

## Use

Not on crates.io. Add it as a git dependency:

```toml
[dependencies]
df-regexp-extract = { git = "https://github.com/avifenesh/df-regexp-extract" }
```

```rust
use datafusion::prelude::*;
use df_regexp_extract::make_regexp_extract_udf;

let ctx = SessionContext::new();
let udf = make_regexp_extract_udf();
ctx.register_udf(udf.clone());

let expr = udf.call(vec![col("text"), lit(r"user_(\d+)"), lit(1)]);
```

Build without PCRE2 with `default-features = false`.

## Configuration

Environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `DF_REGEX_CACHE_SIZE` | 64 | LRU capacity for column patterns |
| `DF_MAX_ZERO_COPY_BUFFER_MB` | 128 | Largest input buffer used for zero-copy views |
| `DF_LARGE_UTF8_BLOCK_SIZE_MB` | 2 | Block size for `LargeUtf8` coalescing |
| `DF_PCRE2_MATCH_LIMIT` | 1000000 | PCRE2 match limit |
| `DF_PCRE2_DEPTH_LIMIT` | match limit | PCRE2 depth limit |
| `DF_PCRE2_HEAP_LIMIT_KIB` | 8192 | PCRE2 heap limit in KiB |

## Develop

```bash
cargo test
cargo bench
cargo run --bin generate_csv_data
```

See [benches/README.md](benches/README.md) and [data/README.md](data/README.md).

## License

Apache-2.0
