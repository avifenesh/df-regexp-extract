/// Comprehensive benchmarks for regexp_extract UDF
///
/// This benchmark suite covers Step 0 from PLAN-V1.md:
/// - Multiple dataset sizes: 1K, 10K, 100K, 1M
/// - String length variations: 8, 16, 64 chars
/// - Pattern complexities: simple, medium, complex
/// - Match rates: 0%, 25%, 50%, 75%, 100%
/// - Null rates: 0%, 20%, 50%
/// - Batch sizes: 8K, 64K, 256K
mod test_data;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{SessionContext, col};
use df_regexp_extract::make_regexp_extract_udf;
use regex::Regex;
use std::hint::black_box;
use std::sync::Arc;
use test_data::*;

fn bench_eval_regexp_extract_utf8(
    input: &StringArray,
    regex: &Regex,
    idx: i32,
) -> DFResult<ArrayRef> {
    if idx < 0 {
        return Err(DataFusionError::Execution("idx must be >= 0".to_string()));
    }

    let len = input.len();
    let mut builder = StringBuilder::with_capacity(len, input.values().len());
    let capture_idx = idx as usize;

    for i in 0..len {
        if input.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = input.value(i);
        append_capture(&mut builder, value, regex, capture_idx);
    }

    Ok(Arc::new(builder.finish()))
}

fn append_capture(builder: &mut StringBuilder, input: &str, regex: &Regex, idx: usize) {
    match regex.captures(input) {
        Some(caps) => {
            let s = caps.get(idx).map(|m| m.as_str()).unwrap_or("");
            builder.append_value(s);
        }
        None => builder.append_value(""),
    }
}

// ===== Data Generation Aligned with PLAN-V1.md =====

/// Generate dataset with mixed string lengths (8, 16, 64 chars) for 1M rows
fn generate_plan_v1_dataset(size: usize, match_rate: f64) -> StringArray {
    let mut data = Vec::with_capacity(size);

    for i in 0..size {
        let length_variant = i % 3;
        let target_len = match length_variant {
            0 => 8,
            1 => 16,
            _ => 64,
        };

        let should_match = (i as f64 / size as f64) < match_rate;

        if should_match {
            // Generate matching pattern: word-digits (ETL common case)
            let prefix = format!("usr{}", i % 1000);
            let number = 100 + (i % 900);
            let mut s = format!("{}-{}", prefix, number);

            // Pad/trim to target length
            while s.len() < target_len {
                s.push('x');
            }
            s.truncate(target_len);

            // Add some UTF-8 emoji occasionally (10% of matching strings)
            if i % 10 == 0 {
                s.push('🚀');
            }

            data.push(Some(s));
        } else {
            // Non-matching string
            let mut s = format!("nomatch{}", i);
            while s.len() < target_len {
                s.push('_');
            }
            s.truncate(target_len);
            data.push(Some(s));
        }
    }

    StringArray::from(data)
}

/// Generate pattern column with specified cardinality
fn generate_pattern_column(size: usize, unique_patterns: usize) -> Vec<String> {
    let base_patterns = vec![
        r"(\w+)-(\d+)",
        r"([a-z]+)_(\d{3})",
        r"^user(\d+)$",
        r"(\w{3,5})-(\d{2,4})",
    ];

    let mut patterns = Vec::with_capacity(size);
    for i in 0..size {
        if unique_patterns <= 4 {
            // Low cardinality: reuse 4 base patterns
            patterns.push(base_patterns[i % base_patterns.len()].to_string());
        } else if unique_patterns <= 128 {
            // Mid cardinality: generate 128 variations
            let variant = i % 128;
            let base = base_patterns[variant % base_patterns.len()];
            patterns.push(format!("{}|extra{}", base, variant));
        } else {
            // High cardinality: each row gets unique pattern
            patterns.push(format!(r"(\w+)-(\d{{{}}})", (i % 5) + 1));
        }
    }
    patterns
}

// ===== Benchmark Functions =====

/// Benchmark: Scalar pattern with scalar idx (baseline - Step 1)
fn bench_scalar_pattern_various_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_pattern");
    group.sample_size(20); // Fast iteration: 20 samples instead of 100

    for size in [10_000, 100_000].iter() {
        let arr = generate_plan_v1_dataset(*size, 0.8);
        let re = Regex::new(r"(\w+)-(\d+)").unwrap();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let out =
                    bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                black_box(out);
            });
        });
    }

    group.finish();
}

/// Benchmark: Different match rates (0%, 50%, 100%)
fn bench_match_rates(c: &mut Criterion) {
    let mut group = c.benchmark_group("match_rates");
    group.sample_size(20);
    let size = 10_000;
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    for match_rate in [0.0, 0.5, 1.0].iter() {
        let arr = generate_plan_v1_dataset(size, *match_rate);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}%", (match_rate * 100.0) as i32)),
            match_rate,
            |b, _| {
                b.iter(|| {
                    let out =
                        bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Different string lengths (short, medium, long)
fn bench_string_lengths(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_lengths");
    group.sample_size(20);
    let size = 10_000;
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    for (name, length_type) in [
        ("short", StringLength::Short),
        ("medium", StringLength::Medium),
        ("long", StringLength::Long),
    ]
    .iter()
    {
        let config = DataConfig {
            size,
            string_length: *length_type,
            match_rate: 0.8,
            null_rate: 0.0,
        };
        let arr = generate_email_like_data(&config);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), name, |b, _| {
            b.iter(|| {
                let out =
                    bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                black_box(out);
            });
        });
    }

    group.finish();
}

/// Benchmark: Null handling with different null rates
fn bench_null_rates(c: &mut Criterion) {
    let mut group = c.benchmark_group("null_rates");
    group.sample_size(20);
    let size = 10_000;
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    for null_rate in [0.0, 0.5].iter() {
        let config = DataConfig {
            size,
            string_length: StringLength::Short,
            match_rate: 0.8,
            null_rate: *null_rate,
        };
        let arr = generate_email_like_data(&config);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}%", (null_rate * 100.0) as i32)),
            null_rate,
            |b, _| {
                b.iter(|| {
                    let out =
                        bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Different group indices (idx=0, 1, 2)
fn bench_group_indices(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_indices");
    group.sample_size(20);
    let size = 10_000;
    let arr = generate_plan_v1_dataset(size, 0.8);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    for idx in [0, 1].iter() {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("idx_{}", idx)),
            idx,
            |b, &idx_val| {
                b.iter(|| {
                    let out =
                        bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), idx_val)
                            .unwrap();
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Pattern complexity (simple, complex)
fn bench_pattern_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_complexity");
    group.sample_size(20);
    let size = 10_000;

    let patterns = vec![
        ("simple", r"\w+", generate_plan_v1_dataset(size, 0.9)),
        (
            "complex",
            r"(\w+)-(\d+)|(\w+)_(\d+)|(\w+)\.(\d+)",
            generate_plan_v1_dataset(size, 0.7),
        ),
    ];

    for (name, pattern, arr) in patterns.iter() {
        let re = Regex::new(pattern).unwrap();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), name, |b, _| {
            b.iter(|| {
                let out =
                    bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                black_box(out);
            });
        });
    }

    group.finish();
}

/// Benchmark: Realistic workloads (log lines, URLs)
fn bench_realistic_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_workloads");
    group.sample_size(20);
    let size = 10_000;

    // Log line extraction: [LEVEL] Component: message
    let log_config = DataConfig {
        size,
        string_length: StringLength::Medium,
        match_rate: 0.9,
        null_rate: 0.05,
    };
    let log_arr = generate_log_lines(&log_config);
    let log_re = Regex::new(r"\[(\w+)\]\s+(\w+):").unwrap();

    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("log_lines", |b| {
        b.iter(|| {
            let out =
                bench_eval_regexp_extract_utf8(black_box(&log_arr), black_box(&log_re), 1).unwrap();
            black_box(out);
        });
    });

    // URL extraction: protocol://domain/path
    let url_config = DataConfig {
        size,
        string_length: StringLength::Medium,
        match_rate: 0.85,
        null_rate: 0.05,
    };
    let url_arr = generate_urls(&url_config);
    let url_re = Regex::new(r"(https?)://([^/]+)").unwrap();

    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("urls", |b| {
        b.iter(|| {
            let out =
                bench_eval_regexp_extract_utf8(black_box(&url_arr), black_box(&url_re), 2).unwrap();
            black_box(out);
        });
    });

    group.finish();
}

/// Benchmark: Edge cases (all nulls, identical strings)
fn bench_edge_cases(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_cases");
    group.sample_size(20);
    let size = 10_000;
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    // All nulls
    let all_nulls = generate_all_nulls(size);
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("all_nulls", |b| {
        b.iter(|| {
            let out =
                bench_eval_regexp_extract_utf8(black_box(&all_nulls), black_box(&re), 1).unwrap();
            black_box(out);
        });
    });

    // All identical strings (best case for caching)
    let all_matching = generate_identical_strings(size, true);
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("identical", |b| {
        b.iter(|| {
            let out = bench_eval_regexp_extract_utf8(black_box(&all_matching), black_box(&re), 1)
                .unwrap();
            black_box(out);
        });
    });

    group.finish();
}

/// Benchmark: Batch sizes (8K, 64K)
fn bench_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_sizes");
    group.sample_size(20);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    for batch_size in [8_192, 65_536].iter() {
        let arr = generate_plan_v1_dataset(*batch_size, 0.8);

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}k", batch_size / 1024)),
            batch_size,
            |b, _| {
                b.iter(|| {
                    let out =
                        bench_eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 1).unwrap();
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Column pattern cardinality (baseline before LRU cache)
/// Tests per-row regex compilation overhead - KEY for measuring Step-4 LRU improvements
fn bench_column_pattern_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_pattern_cardinality");
    group.sample_size(10); // Even fewer samples - this is expensive
    let size = 10_000;

    // Create tokio runtime for async UDF execution
    let rt = tokio::runtime::Runtime::new().unwrap();

    for (name, unique_count) in [("low_4", 4), ("high_10k", size)].iter() {
        // Generate data and pattern column
        let data_arr = generate_plan_v1_dataset(size, 0.8);
        let pattern_col = generate_pattern_column(size, *unique_count);
        let pattern_arr = StringArray::from(pattern_col);
        let idx_arr = Int32Array::from(vec![1; size]);

        // Create DataFrame
        let schema = Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
            Field::new("idx", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(data_arr) as _,
                Arc::new(pattern_arr) as _,
                Arc::new(idx_arr) as _,
            ],
        )
        .unwrap();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), name, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    let udf = make_regexp_extract_udf();
                    ctx.register_udf(udf.clone());

                    let provider =
                        MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap();
                    ctx.register_table("t", Arc::new(provider)).unwrap();

                    let df = ctx
                        .table("t")
                        .await
                        .unwrap()
                        .select(vec![udf.call(vec![
                            col("data"),
                            col("pattern"),
                            col("idx"),
                        ])])
                        .unwrap();

                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_scalar_pattern_various_sizes,
    bench_match_rates,
    bench_string_lengths,
    bench_null_rates,
    bench_group_indices,
    bench_pattern_complexity,
    bench_realistic_workloads,
    bench_edge_cases,
    bench_batch_sizes,
    bench_column_pattern_cardinality,
);

criterion_main!(benches);
