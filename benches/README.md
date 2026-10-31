# Benchmark Suite

**10 groups, ~25 benchmarks** optimized for fast iteration (2-3 min). Tests: data sizes (10K-100K), string lengths, match/null rates, pattern complexity, batch sizes, realistic workloads, edge cases, and column pattern cardinality.

**Sample sizes:**
- Standard: 20 samples
- 100K rows: 75 samples
- Batch 64K: 55 samples
- Column patterns: 55 samples

## Quick Start

```bash
# Run all benchmarks (~2-3 min)
cargo bench

# Run specific group
cargo bench scalar_pattern

# Quick validation (10 samples)
cargo bench edge_cases -- --sample-size 10

# List all benchmarks
cargo bench -- --list
```

## Benchmark Groups

1. **scalar_pattern**: 10K, 100K rows (scalar pattern - fast path)
2. **match_rates**: 0%, 50%, 100%
3. **string_lengths**: Short/Medium/Long
4. **null_rates**: 0%, 50%
5. **group_indices**: idx=0/1
6. **pattern_complexity**: Simple/Complex
7. **realistic_workloads**: Logs, URLs
8. **edge_cases**: All nulls, identical strings
9. **batch_sizes**: 8K/64K
10. **column_pattern_cardinality**: Low(4)/High(10K) - **KEY for LRU cache improvements**

## Run Specific Groups

```bash
cargo bench <group_name>
```

Examples:
- `cargo bench scalar_pattern` - Baseline performance across data sizes
- `cargo bench column_pattern_cardinality` - Per-row compilation overhead
- `cargo bench batch_sizes` - Batch size impact testing

## Baseline Management

```bash
# Save baseline for comparison
cargo bench -- --save-baseline step1

# Compare to saved baseline
cargo bench -- --baseline step1
```

## Results

- **Location**: `target/criterion/<group>/<benchmark>/`
- **HTML reports**: `target/criterion/*/report/index.html`
- **Auto-compare**: Every run compares to previous, shows % change + p-value

```bash
# View HTML report
open target/criterion/scalar_pattern/10000/report/index.html
```

## Files

- `benchmarks.rs` - Main benchmark suite (10 groups, ~25 benchmarks)
- `test_data.rs` - Data generators
