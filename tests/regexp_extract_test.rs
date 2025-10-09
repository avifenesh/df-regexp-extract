use datafusion::arrow::array::{Array, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::prelude::*;
use df_regexp_extract::{eval_regexp_extract_utf8, make_regexp_extract_udf};
use regex::Regex;
use std::sync::Arc;

#[test]
fn kernel_happy_path() {
    let arr = StringArray::from(vec![Some("abc-123"), Some("zzz"), Some("123")]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();
    // idx=1
    let out = eval_regexp_extract_utf8(&arr, &re, 1).unwrap();
    let s = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "abc");
    assert_eq!(s.value(1), "");
    assert_eq!(s.value(2), "");
    // idx=0 full match
    let out0 = eval_regexp_extract_utf8(&arr, &re, 0).unwrap();
    let s0 = out0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s0.value(0), "abc-123");
    assert_eq!(s0.value(1), "");
    assert_eq!(s0.value(2), "");
}

#[test]
fn kernel_nulls_and_no_match() {
    let arr = StringArray::from(vec![None, Some("city-99"), Some("nomatch")]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    let out = eval_regexp_extract_utf8(&arr, &re, 2).unwrap();
    let s = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(s.is_null(0));
    assert_eq!(s.value(1), "99");
    assert_eq!(s.value(2), "");
}

#[test]
fn kernel_group_out_of_range_returns_empty() {
    let arr = StringArray::from(vec![Some("foo-42"), Some("onlytext")]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    let out = eval_regexp_extract_utf8(&arr, &re, 3).unwrap();
    let s = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "");
    assert_eq!(s.value(1), "");
}

#[test]
fn kernel_optional_group_yields_empty_capture() {
    let arr = StringArray::from(vec![Some("color:red"), Some("color:")]);
    let re = Regex::new(r"color:(red)?").unwrap();

    let out = eval_regexp_extract_utf8(&arr, &re, 1).unwrap();
    let s = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "red");
    assert_eq!(s.value(1), "");
}

#[test]
fn kernel_returns_first_match_only() {
    let arr = StringArray::from(vec![Some("id=10 id=20")]);
    let re = Regex::new(r"id=(\d+)").unwrap();

    let out = eval_regexp_extract_utf8(&arr, &re, 1).unwrap();
    let s = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "10");
}

#[tokio::test(flavor = "current_thread")]
async fn udf_via_dataframe_api() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("i", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("abc-123"), Some("zzz"), None])) as _,
            Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(1)])) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("(\\w+)-(\\d+)".to_string())), None),
                col("i"),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    // Validate content quickly
    let b = &out[0];
    let s_col = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s_col.value(0), "123"); // idx=2 group
    assert_eq!(s_col.value(1), ""); // no match -> ""
    assert!(b.column(0).is_null(2)); // input NULL propagated
}

#[tokio::test(flavor = "current_thread")]
async fn udf_idx_zero_returns_full_match() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("ab-12")])) as _],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t_idx0", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t_idx0")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("(\\w+)-(\\d+)".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(0)), None),
            ])
            .alias("full"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col_out.value(0), "ab-12");
}

#[tokio::test(flavor = "current_thread")]
async fn udf_large_utf8_and_null_inputs() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::LargeUtf8, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("i", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(LargeStringArray::from(vec![
                Some("alpha-100"),
                Some("zzz"),
                Some("beta-200"),
                None,
            ])) as _,
            Arc::new(StringArray::from(vec![
                Some("(\\w+)-(\\d+)"),
                None,
                Some("(\\w+)-(\\d+)"),
                Some("(\\w+)"),
            ])) as _,
            Arc::new(Int32Array::from(vec![Some(2), Some(1), None, Some(1)])) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t_large", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t_large")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![col("s"), col("p"), col("i")]).alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col_out.value(0), "100");
    assert!(col_out.is_null(1));
    assert!(col_out.is_null(2));
    assert!(col_out.is_null(3));
}

#[tokio::test(flavor = "current_thread")]
async fn udf_group_out_of_range_returns_empty() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, false),
        Field::new("i", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("foo-42"), Some("plain")])) as _,
            Arc::new(Int32Array::from(vec![Some(3), Some(5)])) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t_out_range", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("t_out_range")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("(\\w+)-(\\d+)".to_string())), None),
                col("i"),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col_out.value(0), "");
    assert_eq!(col_out.value(1), "");
}

#[tokio::test(flavor = "current_thread")]
async fn udf_invalid_pattern_errors() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, false),
        Field::new("p", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("abc")])) as _,
            Arc::new(StringArray::from(vec![Some("(")])) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t_invalid", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t_invalid")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                col("p"),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            ])
            .alias("g"),
        ])
        .unwrap();

    let err = df.collect().await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("invalid regex"),
        "expected invalid regex message, got {msg}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn udf_idx_array_negative_errors() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, false),
        Field::new("i", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("abc-123")])) as _,
            Arc::new(Int32Array::from(vec![Some(-1)])) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t_idx", Arc::new(provider)).unwrap();

    let df = ctx
        .table("t_idx")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("(\\w+)-(\\d+)".to_string())), None),
                col("i"),
            ])
            .alias("g"),
        ])
        .unwrap();

    let err = df.collect().await.unwrap_err();
    assert!(
        format!("{err}").contains("idx must be >= 0"),
        "expected idx >= 0 error, got {err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn spark_parity_doc_examples() {
    // Mirrors https://spark.apache.org/docs/latest/api/sql/index.html#regexp_extract examples
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("100-200"),
            Some("abc-def"),
            Some("no-digits"),
        ])) as _],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_docs", Arc::new(provider))
        .unwrap();

    let extract_expr = |idx: i32, alias: &str| {
        udf.call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("(\\d+)-(\\d+)".to_string())), None),
            Expr::Literal(ScalarValue::Int32(Some(idx)), None),
        ])
        .alias(alias)
    };

    let df = ctx
        .table("spark_docs")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            extract_expr(0, "full"),
            extract_expr(1, "first_group"),
            extract_expr(2, "second_group"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let full = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let first = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(full.value(0), "100-200");
    assert_eq!(full.value(1), "");
    assert_eq!(full.value(2), "");

    assert_eq!(first.value(0), "100");
    assert_eq!(first.value(1), "");
    assert_eq!(first.value(2), "");

    assert_eq!(second.value(0), "200");
    assert_eq!(second.value(1), "");
    assert_eq!(second.value(2), "");
}

#[test]
fn idx_negative_errors() {
    let arr = StringArray::from(vec![Some("abc-123")]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();
    let err = eval_regexp_extract_utf8(&arr, &re, -1).unwrap_err();
    assert!(format!("{err}").contains("idx must be >= 0"));
}
