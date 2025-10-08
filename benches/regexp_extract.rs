use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use datafusion::arrow::array::StringArray;
use df_regexp_extract::eval_regexp_extract_utf8;
use regex::Regex;

fn bench_kernel(c: &mut Criterion) {
    let s = vec![Some("abc-123"); 200_000];
    let arr = StringArray::from(s);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();
    c.bench_function("kernel scalar pattern idx=2", |b| {
        b.iter(|| {
            let out = eval_regexp_extract_utf8(black_box(&arr), black_box(&re), 2).unwrap();
            black_box(out);
        })
    });
}

criterion_group!(benches, bench_kernel);
criterion_main!(benches);
