## Criterion Benchmarking

🧸 Criterion explained like you're 6 years old

Imagine you want to know which toy car is the fastest. How do you figure it out?

**The wrong way:**
- Race them once
- "This car won! It's the fastest!"
- But wait... maybe you pushed it harder? Maybe the floor was bumpy?

**The Criterion way (the SMART way):**
1. **Race many times** - Do it 100 times, not just once!
2. **Warm up first** - Let the cars roll a bit before the real race
3. **Measure carefully** - Use a super precise stopwatch
4. **Check if it makes sense** - If one car was way slower, race it again to be sure
5. **Make a report** - Write down all the times and find the average

**Why is this better?**
- If you race 100 times, one bumpy spot doesn't matter
- You can see if the car is ALWAYS fast or just got lucky once
- You know FOR SURE which car is fastest, not just guessing!

**Criterion is like a robot that:**
- Races your code many times
- Writes down all the results
- Does the math to find the average
- Tells you "This code is 2.5× faster than before!" with PROOF!

⸻

🧠 Criterion explained like you're a senior systems engineer

Criterion is a Rust microbenchmarking framework that provides statistically rigorous performance measurements with outlier detection, regression analysis, and HTML report generation. It addresses common pitfalls in naive benchmarking through proper sampling, warmup, and statistical analysis.

### 1️⃣ Core Concepts

**What Criterion Measures:**
- **Execution Time:** Wall-clock time per iteration
- **Throughput:** Operations per second (optional)
- **Statistical Properties:** Mean, median, standard deviation, confidence intervals

**Why Not Just `std::time::Instant`?**
```rust
// ❌ NAIVE BENCHMARK (Unreliable)
let start = Instant::now();
my_function();
let duration = start.elapsed();
println!("Took: {:?}", duration);

// Problems:
// 1. Single measurement (no statistical confidence)
// 2. No warmup (cold caches, JIT effects)
// 3. No outlier detection (GC, OS interrupts)
// 4. No comparison baseline
// 5. No regression detection
```

```rust
// ✅ CRITERION BENCHMARK (Reliable)
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_my_function(c: &mut Criterion) {
    c.bench_function("my_function", |b| b.iter(|| {
        my_function(black_box(42))  // Prevent compiler optimization
    }));
}

criterion_group!(benches, benchmark_my_function);
criterion_main!(benches);

// Benefits:
// ✅ Runs 100+ iterations automatically
// ✅ Warms up CPU caches
// ✅ Detects and reports outliers
// ✅ Compares against previous runs
// ✅ Generates HTML reports with plots
```

### 2️⃣ Installation and Setup

**Add to `Cargo.toml`:**
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "my_benchmark"
harness = false  # Required: disables default benchmark harness
```

**Create Benchmark File:**
```bash
mkdir -p benches
touch benches/my_benchmark.rs
```

**Basic Benchmark Structure:**
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn fibonacci_recursive(n: u64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        n => fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2),
    }
}

fn bench_fibonacci(c: &mut Criterion) {
    c.bench_function("fib 20", |b| {
        b.iter(|| fibonacci_recursive(black_box(20)))
    });
}

criterion_group!(benches, bench_fibonacci);
criterion_main!(benches);
```

**Run Benchmarks:**
```bash
cargo bench
```

### 3️⃣ Criterion's Measurement Process

**Execution Flow:**

```
┌─────────────────────────────────────────────────────────┐
│ 1. WARMUP PHASE                                         │
│    Run function 10-100 times to warm CPU caches         │
│    Discard these measurements                           │
├─────────────────────────────────────────────────────────┤
│ 2. INITIAL SAMPLE                                       │
│    Run function to estimate iteration time              │
│    Determine how many iterations to run                 │
├─────────────────────────────────────────────────────────┤
│ 3. MEASUREMENT PHASE                                    │
│    Run function 100+ times (configurable)               │
│    Measure total time for each sample                   │
├─────────────────────────────────────────────────────────┤
│ 4. OUTLIER DETECTION                                    │
│    Identify measurements > 3 std deviations             │
│    Flag as outliers (OS interrupts, GC, etc.)           │
├─────────────────────────────────────────────────────────┤
│ 5. STATISTICAL ANALYSIS                                 │
│    Calculate: mean, median, std dev                     │
│    Compute 95% confidence intervals                     │
├─────────────────────────────────────────────────────────┤
│ 6. REGRESSION CHECK                                     │
│    Compare against previous run (if exists)             │
│    Report: faster, slower, or no change                 │
├─────────────────────────────────────────────────────────┤
│ 7. REPORT GENERATION                                    │
│    Console output with summary                          │
│    HTML report with plots (target/criterion/)           │
└─────────────────────────────────────────────────────────┘
```

**Sample Size Determination:**
Criterion automatically adjusts sample size based on iteration time:
- **Fast functions (<1μs):** 10,000+ iterations per sample
- **Medium functions (1μs-1ms):** 100-1000 iterations per sample
- **Slow functions (>1ms):** 10-100 iterations per sample

### 4️⃣ Black Box: Preventing Compiler Optimizations

**Problem: Dead Code Elimination**
```rust
// ❌ WRONG: Compiler optimizes away the function call!
fn bench_add(c: &mut Criterion) {
    c.bench_function("add", |b| {
        b.iter(|| {
            let result = 2 + 2;  // Compiler: "result is never used, skip this!"
        });
    });
}
// Measures: ~0.1ns (just loop overhead, not actual addition)
```

**Solution: black_box**
```rust
// ✅ CORRECT: black_box prevents optimization
fn bench_add(c: &mut Criterion) {
    c.bench_function("add", |b| {
        b.iter(|| {
            black_box(2 + 2);  // Compiler: "Result is used, must compute!"
        });
    });
}
// Measures: ~0.3ns (actual addition time)
```

**How black_box Works:**
```rust
#[inline]
pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        // Tells compiler: "This value might be used by external code"
        // Forces computation without actually doing anything
        let ret = std::ptr::read_volatile(&dummy as *const T);
        std::mem::forget(dummy);
        ret
    }
}
```

**When to Use black_box:**
- ✅ Function inputs (prevent constant folding)
- ✅ Function outputs (prevent dead code elimination)
- ❌ Inside the function being benchmarked (distorts results)

### 5️⃣ Comparing Multiple Implementations

**Example: Benchmark Different Regex Approaches**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use regex::Regex;

fn bench_regex_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("regex_extract");

    let data = vec!["abc-123"; 1000];

    // Approach 1: Compile per call
    group.bench_function("compile_per_call", |b| {
        b.iter(|| {
            for s in &data {
                let re = Regex::new(r"(\w+)-(\d+)").unwrap();
                black_box(re.captures(s));
            }
        });
    });

    // Approach 2: Precompile
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();
    group.bench_function("precompiled", |b| {
        b.iter(|| {
            for s in &data {
                black_box(re.captures(s));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_regex_extraction);
criterion_main!(benches);
```

**Output:**
```
regex_extract/compile_per_call
                        time:   [5.2453 ms 5.2712 ms 5.2989 ms]

regex_extract/precompiled
                        time:   [4.8234 µs 4.8567 µs 4.8934 µs]

Comparison: precompiled is 1082× faster!
```

### 6️⃣ Parameterized Benchmarks

**Benchmark Across Multiple Input Sizes:**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn fibonacci_recursive(n: u64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        n => fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2),
    }
}

fn bench_fibonacci_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("fibonacci");

    for size in [10, 15, 20, 25].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| fibonacci_recursive(black_box(size)))
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_fibonacci_sizes);
criterion_main!(benches);
```

**Output:**
```
fibonacci/10            time:   [847.23 ns 853.45 ns 860.12 ns]
fibonacci/15            time:   [9.8234 µs 9.9123 µs 10.012 µs]
fibonacci/20            time:   [127.45 µs 128.34 µs 129.45 µs]
fibonacci/25            time:   [1.5234 ms 1.5456 ms 1.5689 ms]
```

### 7️⃣ Throughput Measurement

**Measure Operations Per Second:**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn bench_string_processing(c: &mut Criterion) {
    let data = vec!["test string"; 10000];
    let data_bytes = data.iter().map(|s| s.len()).sum::<usize>();

    let mut group = c.benchmark_group("string_processing");
    group.throughput(Throughput::Bytes(data_bytes as u64));

    group.bench_function("uppercase", |b| {
        b.iter(|| {
            for s in &data {
                black_box(s.to_uppercase());
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_string_processing);
criterion_main!(benches);
```

**Output:**
```
string_processing/uppercase
                        time:   [1.2345 ms 1.2456 ms 1.2578 ms]
                        thrpt:  [87.5 MiB/s 88.3 MiB/s 89.1 MiB/s]
```

### 8️⃣ Configuration Options

**Customize Measurement Parameters:**

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

fn bench_custom_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("custom");

    // Configure sample size
    group.sample_size(500);  // Default: 100

    // Configure warmup time
    group.warm_up_time(Duration::from_secs(5));  // Default: 3s

    // Configure measurement time
    group.measurement_time(Duration::from_secs(10));  // Default: 5s

    // Configure significance level (for regression detection)
    group.significance_level(0.05);  // Default: 0.05 (95% confidence)

    // Configure noise threshold
    group.noise_threshold(0.02);  // Default: 0.01 (1%)

    group.bench_function("my_function", |b| {
        b.iter(|| {
            // benchmark code
        });
    });

    group.finish();
}

criterion_group!(benches, bench_custom_config);
criterion_main!(benches);
```

### 9️⃣ Real-World Example: This Codebase

**Benchmark regexp_extract:**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use arrow::array::StringArray;
use regex::Regex;
use df_regexp_extract::eval_regexp_extract_utf8;

fn bench_regexp_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("regexp_extract");

    // Create test data
    let data = vec!["abc-123"; 10000];
    let arr = StringArray::from(data.clone());
    let pattern = Regex::new(r"(\w+)-(\d+)").unwrap();

    // Calculate throughput
    let total_bytes: usize = data.iter().map(|s| s.len()).sum();
    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Benchmark kernel function
    group.bench_function("kernel_extract_group_1", |b| {
        b.iter(|| {
            eval_regexp_extract_utf8(
                black_box(&arr),
                black_box(&pattern),
                black_box(1)
            )
        });
    });

    // Benchmark different group indices
    for idx in [0, 1, 2].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(format!("group_{}", idx)),
            idx,
            |b, &idx| {
                b.iter(|| {
                    eval_regexp_extract_utf8(
                        black_box(&arr),
                        black_box(&pattern),
                        black_box(idx)
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_regexp_extract);
criterion_main!(benches);
```

**Run and Analyze:**
```bash
# Run benchmarks
cargo bench

# View results
open target/criterion/regexp_extract/report/index.html
```

### 🔟 Interpreting Results

**Console Output:**
```
regexp_extract/kernel_extract_group_1
                        time:   [12.345 µs 12.456 µs 12.578 µs]
                        thrpt:  [543.2 MiB/s 548.7 MiB/s 553.8 MiB/s]

Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe
```

**What This Means:**
- **time:** 95% confidence interval for execution time
  - Lower bound: 12.345 µs (best case)
  - Estimate: 12.456 µs (most likely)
  - Upper bound: 12.578 µs (worst case)
- **thrpt:** Throughput in bytes/second
  - Inverse of time (higher is better)
- **Outliers:** Measurements removed due to interference
  - 3% outliers is normal (OS interrupts, etc.)
  - >10% outliers indicates unstable environment

**Change Detection:**
```
Performance has improved!
                        time:   [-15.234% -14.567% -13.891%]
                        thrpt:  [+16.123% +17.045% +17.989%]

Previous run: 14.567 µs
Current run:  12.456 µs
Improvement:  14.5% faster
```

### 1️⃣1️⃣ HTML Reports

**Generated Reports:**
```
target/criterion/
├── regexp_extract/
│   ├── report/
│   │   ├── index.html           # Main report
│   │   ├── violin.svg           # Distribution plot
│   │   ├── pdf.svg              # Probability density
│   │   ├── regression.svg       # Regression analysis
│   │   └── ...
│   └── base/
│       └── estimates.json       # Baseline for comparisons
```

**Plots Included:**
1. **Violin Plot:** Distribution of measurements
2. **PDF:** Probability density function
3. **Regression:** Time vs iterations
4. **Comparison:** Current vs previous runs

### 1️⃣2️⃣ Best Practices

**✅ DO:**
- Use `black_box` on inputs and outputs
- Benchmark realistic data sizes
- Run on idle machine (no background tasks)
- Use `sample_size` to increase confidence
- Benchmark before and after optimizations
- Check for outliers (should be < 5%)

**❌ DON'T:**
- Benchmark in Debug mode (use `--release`)
- Benchmark trivial operations (<1ns)
- Run benchmarks on laptop (thermal throttling)
- Compare across different machines
- Micro-optimize without benchmarking
- Trust single measurements

### 1️⃣3️⃣ Common Pitfalls

**Pitfall 1: Debug Mode**
```bash
# ❌ WRONG: 10-100× slower
cargo bench

# ✅ CORRECT: Always use release mode
cargo bench --release
```

**Pitfall 2: Insufficient Warmup**
```rust
// ❌ WRONG: Cold caches
c.bench_function("cold", |b| {
    b.iter(|| expensive_operation());
});

// ✅ CORRECT: Criterion handles warmup automatically
// No manual warmup needed!
```

**Pitfall 3: Including Setup in Measurement**
```rust
// ❌ WRONG: Measures setup + operation
c.bench_function("with_setup", |b| {
    b.iter(|| {
        let data = vec![1, 2, 3];  // Setup included!
        black_box(data.iter().sum::<i32>());
    });
});

// ✅ CORRECT: Setup outside iter
c.bench_function("without_setup", |b| {
    let data = vec![1, 2, 3];  // Setup once
    b.iter(|| {
        black_box(data.iter().sum::<i32>());
    });
});
```

**Pitfall 4: Forgetting black_box**
```rust
// ❌ WRONG: Compiler optimizes away
b.iter(|| {
    let result = expensive_operation();
    // result unused → optimized away!
});

// ✅ CORRECT: Force computation
b.iter(|| {
    black_box(expensive_operation());
});
```

### 1️⃣4️⃣ Integration with CI/CD

**GitHub Actions Example:**
```yaml
name: Benchmarks

on:
  pull_request:
    branches: [ main ]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Run benchmarks
        run: cargo bench --bench regexp_extract

      - name: Store results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: target/criterion/

      - name: Compare with baseline
        run: |
          # Compare with previous run
          # Fail if regression > 10%
```

### 🧩 TL;DR

**What is Criterion?**
A statistical benchmarking framework for Rust that provides:
- ✅ Statistically rigorous measurements (100+ samples)
- ✅ Outlier detection and removal
- ✅ Regression detection (compare vs previous runs)
- ✅ HTML reports with plots
- ✅ Automatic warmup and sample sizing

**Basic Setup:**
```toml
# Cargo.toml
[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "my_bench"
harness = false
```

```rust
// benches/my_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_my_function(c: &mut Criterion) {
    c.bench_function("my_func", |b| {
        b.iter(|| my_function(black_box(42)))
    });
}

criterion_group!(benches, bench_my_function);
criterion_main!(benches);
```

```bash
# Run
cargo bench
```

**Key Features:**
- **black_box:** Prevents compiler optimizations
- **Throughput:** Measures MB/s or ops/s
- **Parameterized:** Test multiple input sizes
- **Comparison:** Detects regressions automatically

**Best Practices:**
- Always use `--release` mode
- Use `black_box` on inputs/outputs
- Run on idle machine
- Check outlier percentage (< 5%)
- Benchmark realistic data sizes

**This Codebase:**
Use Criterion to:
1. Measure `eval_regexp_extract_utf8` performance
2. Compare scalar vs array pattern performance
3. Validate 2-3× speedup claims vs Spark
4. Detect performance regressions in PRs
5. Generate reports for stakeholders

**Interview Context:**
Demonstrate ability to:
- Write statistically valid benchmarks
- Interpret performance results
- Identify optimization opportunities
- Quantify improvements with confidence
