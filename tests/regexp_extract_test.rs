use datafusion::arrow::array::{Array, ArrayRef, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::prelude::*;
use df_regexp_extract::make_regexp_extract_udf;
use std::sync::Arc;

async fn run_extract(
    strings: Vec<Option<&'static str>>,
    pattern: &str,
    idx: Option<i32>,
) -> DFResult<Vec<Option<String>>> {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let array: ArrayRef = Arc::new(StringArray::from(strings));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("extract_input", Arc::new(provider))
        .unwrap();

    let pattern_expr = Expr::Literal(ScalarValue::Utf8(Some(pattern.to_string())), None);
    let call_expr = match idx {
        Some(i) => udf.call(vec![
            col("s"),
            pattern_expr.clone(),
            Expr::Literal(ScalarValue::Int32(Some(i)), None),
        ]),
        None => udf.call(vec![col("s"), pattern_expr.clone()]),
    };

    let df = ctx
        .table("extract_input")
        .await
        .unwrap()
        .select(vec![call_expr.alias("g")])
        .unwrap();
    let batches = df.collect().await?;
    let col = batches[0].column(0);

    // Handle both StringArray (Utf8) and StringViewArray (Utf8View)
    use datafusion::arrow::array::StringViewArray;

    if let Some(str_array) = col.as_any().downcast_ref::<StringArray>() {
        Ok((0..str_array.len())
            .map(|i| {
                if str_array.is_null(i) {
                    None
                } else {
                    Some(str_array.value(i).to_string())
                }
            })
            .collect())
    } else if let Some(view_array) = col.as_any().downcast_ref::<StringViewArray>() {
        Ok((0..view_array.len())
            .map(|i| {
                if view_array.is_null(i) {
                    None
                } else {
                    Some(view_array.value(i).to_string())
                }
            })
            .collect())
    } else {
        panic!("Expected StringArray or StringViewArray, got {:?}", col.data_type());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_happy_path() {
    let idx1 = run_extract(
        vec![Some("abc-123"), Some("zzz"), Some("123")],
        "(\\w+)-(\\d+)",
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(
        idx1,
        vec![
            Some("abc".to_string()),
            Some("".to_string()),
            Some("".to_string())
        ]
    );

    let idx0 = run_extract(
        vec![Some("abc-123"), Some("zzz"), Some("123")],
        "(\\w+)-(\\d+)",
        Some(0),
    )
    .await
    .unwrap();
    assert_eq!(
        idx0,
        vec![
            Some("abc-123".to_string()),
            Some("".to_string()),
            Some("".to_string())
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_nulls_and_no_match() {
    let out = run_extract(
        vec![None, Some("city-99"), Some("nomatch")],
        "(\\w+)-(\\d+)",
        Some(2),
    )
    .await
    .unwrap();
    assert_eq!(
        out,
        vec![None, Some("99".to_string()), Some("".to_string())]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_group_out_of_range_returns_empty() {
    let out = run_extract(
        vec![Some("foo-42"), Some("onlytext")],
        "(\\w+)-(\\d+)",
        Some(3),
    )
    .await
    .unwrap();
    assert_eq!(out, vec![Some("".to_string()), Some("".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_optional_group_yields_empty_capture() {
    let out = run_extract(
        vec![Some("color:red"), Some("color:")],
        "color:(red)?",
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(out, vec![Some("red".to_string()), Some("".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn kernel_returns_first_match_only() {
    let out = run_extract(vec![Some("id=10 id=20")], "id=(\\d+)", Some(1))
        .await
        .unwrap();
    assert_eq!(out, vec![Some("10".to_string())]);
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

#[tokio::test(flavor = "current_thread")]
async fn idx_negative_errors() {
    let err = run_extract(vec![Some("abc-123")], "(\\w+)-(\\d+)", Some(-1))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("idx must be >= 0"));
}

// ============================================================================
// SEMANTICS PARITY TESTS: Explicit edge case behavior
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn semantics_null_input_returns_null() {
    let result = run_extract(vec![None], "\\d+", Some(1)).await.unwrap();
    assert_eq!(result, vec![None]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_no_match_returns_empty_string() {
    let result = run_extract(vec![Some("abcdef")], "\\d+", Some(1))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_missing_group_returns_empty_string() {
    // Pattern has 1 group, ask for group 5
    let result = run_extract(vec![Some("foo123")], "(\\d+)", Some(5))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_index_0_returns_full_match() {
    // idx=0 should return full match, not a capture group
    let result = run_extract(vec![Some("foo123bar")], "\\d+", Some(0))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("123".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_index_0_with_groups_returns_full_match() {
    // Even with capture groups, idx=0 returns full match
    let result = run_extract(vec![Some("foo123bar")], "f(o+)(\\d+)b", Some(0))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("foo123b".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_index_1_returns_first_group() {
    let result = run_extract(vec![Some("foo123bar")], "f(o+)(\\d+)b", Some(1))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("oo".to_string())]);
}

#[tokio::test(flavor = "current_thread")]
async fn semantics_index_2_returns_second_group() {
    let result = run_extract(vec![Some("foo123bar")], "f(o+)(\\d+)b", Some(2))
        .await
        .unwrap();
    assert_eq!(result, vec![Some("123".to_string())]);
}
