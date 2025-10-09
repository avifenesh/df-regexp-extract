# Benchmark Suite for df-regexp-extract

This directory contains a comprehensive benchmark suite for the `regexp_extract` UDF implementation, aligned with the optimization roadmap in PLAN-V1.md. The suite provides extensive coverage across multiple dimensions to measure performance characteristics and validate optimizations.

## Overview

The benchmark suite tests the UDF across 9 key dimensions:
- **Data Sizes**: 1K, 10K, 100K, 1M rows
- **String Lengths**: Short (10-20 chars), Medium (50-100 chars), Long (500-1000 chars)
- **Pattern Complexity**: Simple (`\w+`), Medium (`(\w+)-(\d+)`), Complex (alternations)
- **Match Rates**: 0%, 25%, 50%, 75%, 100%
- **Null Rates**: 0%, 20%, 50%
- **Group Indices**: idx=0 (full match), idx=1, idx=2
- **Batch Sizes**: 8K, 64K, 256K (per PLAN-V1 Step 0)
- **Realistic Workloads**: Log lines, URLs, structured data
- **Edge Cases**: All nulls, identical matching/non-matching

**Total: 40+ individual benchmarks** across 9 groups in `comprehensive.rs`.

### Benchmark Files
- **`comprehensive.rs`**: Main suite (9 groups, 40+ benchmarks)
- **`test_data.rs`**: Data generation utilities (6 generators, configurable via `DataConfig`)
- **`regexp_extract.rs`**: Original simple benchmark (baseline)

Data generators include:
- `generate_plan_v1_dataset()`: Mixed lengths with UTF-8
- `generate_email_like_data()`: user-123 patterns
- `generate_log_lines()`: [LEVEL] Component: message
- `generate_urls()`: protocol://domain/path
- `generate_structured_data()`: KEY=value|ID:12345
- `generate_all_nulls()` / `generate_identical_strings()`: Edge cases

## Running Benchmarks

### Quick Commands
```bash
# List all benchmarks (no execution)
cargo bench --bench comprehensive -- --list

# Quick validation (10 samples, <1 min)
cargo bench --bench comprehensive edge_cases -- --sample-size 10

# Single group (2-5 min, 100 samples)
cargo bench --bench comprehensive scalar_pattern

# All benchmarks (15-30 min)
cargo bench --bench comprehensive

# Quiet mode
cargo bench --bench comprehensive -q

# Original simple benchmark
cargo bench --bench regexp_extract
```

### Specific Groups
```bash
# Scalar pattern (data sizes)
cargo bench --bench comprehensive scalar_pattern

# Match rates
cargo bench --bench comprehensive match_rates

# String lengths
cargo bench --bench comprehensive string_lengths

# Null rates
cargo bench --bench comprehensive null_rates

# Group indices
cargo bench --bench comprehensive group_indices

# Pattern complexity
cargo bench --bench comprehensive pattern_complexity

# Realistic workloads
cargo bench --bench comprehensive realistic_workloads

# Edge cases
cargo bench --bench comprehensive edge_cases

# Batch sizes (PLAN-V1 Step 0)
cargo bench --bench comprehensive batch_sizes
```

### Advanced Usage
- **Sample Size**: `-- --sample-size N` (default: 100 for precision)
- **Baseline Management**:
  ```bash
  # Save baseline
  cargo bench --bench comprehensive -- --save-baseline step1

  # Compare to baseline
  cargo bench --bench comprehensive -- --baseline step1
  ```
- **Flamegraphs** (profiling):
  ```bash
  cargo flamegraph --bench comprehensive -- --bench
  ```

## Benchmark Groups

1. **`scalar_pattern`** (Baseline): Data sizes (1K-1M) with scalar pattern `(\w+)-(\d+)`.
2. **`match_rates`**: 0-100% match rates (non-matches vs. matches).
3. **`string_lengths`**: Short/Medium/Long strings.
4. **`null_rates`**: 0%/20%/50% nulls (propagation overhead).
5. **`group_indices`**: idx=0/1/2 extraction.
6. **`pattern_complexity`**: Simple/Medium/Complex patterns.
7. **`realistic_workloads`**: Logs (level/component), URLs (protocol/domain), Structured (ID extraction).
8. **`edge_cases`**: All nulls, identical matching/non-matching.
9. **`batch_sizes`**: 8K/64K/256K rows (PLAN-V1 Step 0).

## Metrics & Interpretation

Each benchmark reports:
- **Time**: [lower median upper] µs (95% CI via bootstrap resampling)
- **Throughput**: Melem/s (millions of elements/rows per second)
- **Change**: % vs. previous run (p-value for significance)

### Example Output
```
scalar_pattern/1000     time:   [45.234 µs 45.891 µs 46.612 µs]
                        thrpt:  [21.456 Melem/s 21.791 Melem/s 22.104 Melem/s]
                        change: [-2.12% -0.57% +1.23%] (p = 0.23 > 0.05)
                        No change in performance detected.
```

- **Good Signs**: High throughput (>20 Melem/s), low ns/row (<50 ns), consistent across rates.
- **Optimization Opportunities**: Large best/worst-case gap, high variance, poor scaling.
- **Outliers**: Criterion detects and flags (high mild/severe).

### Viewing Results
```bash
# Open HTML report (charts, comparisons)
open target/criterion/scalar_pattern/1000/report/index.html

# Find all reports
find target/criterion -name "index.html"
```

Results saved automatically to `target/criterion/`:
- `base/`: Previous baseline
- `new/`: Current run
- `report/`: HTML with violin plots, line charts, stats

## Validation Summary

### Coverage ✅
- 9 groups, 40+ benchmarks across all PLAN-V1 dimensions.
- Verified: `cargo bench --bench comprehensive -- --list`

### Fast Execution ✅
- Quick: <1 min with `--sample-size 10`
- Group: 2-5 min
- Full: 15-30 min

### Multiple Iterations & Stats ✅
- Default: 100 samples + 3s warm-up
- Adaptive: More if high variance
- Metrics: Mean/median/std dev/MAD, 95% CI, outlier detection
- Bootstrap: 100k resamples for precision

### Automatic Save & Compare ✅
- Saves every run to `target/criterion/`
- Compares to baseline: % change + p-value (significant if p < 0.05)
- Color-coded: Green (improvement), Red (regression), Yellow (no change)

### PLAN-V1 Alignment ✅
- Implements Step 0: Fixtures (1M mixed lengths, UTF-8), batch sizes, scalar pattern.
- Ready for Steps 1-5: Brute force → Fallback → Column patterns → LRU cache → Parallel compile.

## Tips
- Run in release mode (automatic).
- Close apps to minimize noise.
- Run multiple times for stable baseline.
- Save before changes: `cp -r target/criterion target/criterion-baseline`
- Check compilation: `cargo check --benches`

## Next Steps
1. Establish baseline: `cargo bench --bench comprehensive`
2. Implement PLAN-V1 optimizations (Steps 1-5).
3. Re-run: Automatic comparison shows impact.
4. Review reports for regressions/improvements.
5. Document in PLAN-V1 progress sections.
