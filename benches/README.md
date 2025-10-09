# Benchmark Suite

Comprehensive benchmarks for `regexp_extract` UDF.

**10 groups, 43+ benchmarks** testing: data sizes (1K-1M), string lengths, match/null rates, pattern complexity, batch sizes, realistic workloads, edge cases, and column pattern cardinality.

## Quick Start

```bash
# Run all benchmarks (~15-30 min)
cargo bench --bench comprehensive

# Run specific group
cargo bench --bench comprehensive scalar_pattern

# Quick validation (10 samples)
cargo bench --bench comprehensive edge_cases -- --sample-size 10

# List all benchmarks
cargo bench --bench comprehensive -- --list
```

## Benchmark Groups

1. **scalar_pattern**: Data sizes (1K-1M) with scalar pattern
2. **match_rates**: 0%, 25%, 50%, 75%, 100%
3. **string_lengths**: Short/Medium/Long
4. **null_rates**: 0%, 20%, 50%
5. **group_indices**: idx=0/1/2
6. **pattern_complexity**: Simple/Medium/Complex
7. **realistic_workloads**: Logs, URLs, Structured data
8. **edge_cases**: All nulls, identical strings
9. **batch_sizes**: 8K/64K/256K
10. **column_pattern_cardinality**: Low/Mid/High cardinality (baseline before LRU/parallel optimizations)

## Run Specific Groups

```bash
cargo bench --bench comprehensive <group_name>
```

Examples:
- `scalar_pattern` - Baseline performance across data sizes
- `column_pattern_cardinality` - Per-row compilation overhead (before LRU/parallel optimizations)
- `batch_sizes` - Batch size impact testing

## Baseline Management

```bash
# Save baseline for comparison
cargo bench --bench comprehensive -- --save-baseline step1

# Compare to saved baseline
cargo bench --bench comprehensive -- --baseline step1
```

## Results

- **Location**: `target/criterion/<group>/<benchmark>/`
- **HTML reports**: `target/criterion/*/report/index.html`
- **Auto-compare**: Every run compares to previous, shows % change + p-value

```bash
# View HTML report
open target/criterion/scalar_pattern/1000/report/index.html
```

## Files

- `comprehensive.rs` - Main benchmark suite (10 groups)
- `test_data.rs` - Data generators
- `regexp_extract.rs` - Original simple benchmark
