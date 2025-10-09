# Benchmark Data

Pre-generated CSV files for reproducible benchmarking (~11MB total, all committed).

## Regenerate

```bash
cargo run --bin generate_csv_data
```

## Files

- `scalar_pattern/` - 1K, 10K, 100K rows (baseline performance)
- `match_rates/` - 0%, 25%, 50%, 75%, 100% match rates
- `null_rates/` - 0%, 20%, 50% null rates
- `string_lengths/` - Short/medium/long strings
- `realistic/` - Logs, URLs, structured data
- `column_patterns/` - Low/mid/high cardinality patterns
- `edge_cases/` - All nulls, identical strings

All files are 10K rows except `scalar_pattern/` (varies).
