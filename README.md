# df-regexp-extract

Spark-compatible `regexp_extract` as a **DataFusion Scalar UDF** (no SQL API).

- Returns empty string on no match / unmatched group.
- idx=0 returns the full match; default idx=1.
- NULL input -> NULL output.
- Works with Utf8; you can extend to LargeUtf8/StringView.

This repo avoids a direct `arrow` dependency to prevent version mismatches; we use `datafusion::arrow::*` re-exports.

## Quick start

```bash
cargo run --bin demo
cargo test -q
```

If your project pins a different DataFusion major, update `Cargo.toml` accordingly.

## Concepts & Knowledge Hub

- See `knowledge-hub/README.md` for an ELI5-to-expert walkthrough of UDFs, Arrow arrays, broadcasting, null propagation, `StringArray`/`LargeUtf8`, Spark `regexp_extract` semantics, and DataFusion regex functions.

## Performance

- Benchmarks: `cargo bench -q` (Criterion; see `benches/regexp_extract.rs`).
- Flamegraph (bench): `cargo flamegraph --bench regexp_extract -- --sample-size 15`.
- Flamegraph (demo): `cargo flamegraph --bin demo --release`.
- Tip: ensure `perf` is installed and kernel permits profiling; details in `knowledge-hub/CONCEPTS.md`.
