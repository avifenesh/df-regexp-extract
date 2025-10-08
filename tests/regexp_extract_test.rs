use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
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

#[tokio::test(flavor = "current_thread")]
async fn udf_via_dataframe_api() {
    let mut ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf);

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

    use datafusion::prelude::col;
    use datafusion_expr::{expr_fn::scalar_udf, lit};

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            scalar_udf(
                "regexp_extract",
                vec![col("s"), lit("(\\w+)-(\\d+)"), col("i")],
            )
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

#[test]
fn idx_negative_errors() {
    let arr = StringArray::from(vec![Some("abc-123")]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();
    let err = eval_regexp_extract_utf8(&arr, &re, -1).unwrap_err();
    assert!(format!("{err}").contains("idx must be >= 0"));
}
