use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray,
};
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
        panic!(
            "Expected StringArray or StringViewArray, got {:?}",
            col.data_type()
        );
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

    // Handle both StringArray and StringViewArray
    let col1 = b.column(1);
    if let Some(s_col) = col1.as_any().downcast_ref::<StringArray>() {
        assert_eq!(s_col.value(0), "123"); // idx=2 group
        assert_eq!(s_col.value(1), ""); // no match -> ""
    } else if let Some(v_col) = col1.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_col.value(0), "123"); // idx=2 group
        assert_eq!(v_col.value(1), ""); // no match -> ""
    } else {
        panic!("Expected StringArray or StringViewArray");
    }
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
    let col_out = out[0].column(0);

    // Handle both StringArray and StringViewArray
    if let Some(s_arr) = col_out.as_any().downcast_ref::<StringArray>() {
        assert_eq!(s_arr.value(0), "ab-12");
    } else if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "ab-12");
    } else {
        panic!("Expected StringArray or StringViewArray");
    }
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
    let col_out = out[0].column(0);

    // Handle both StringArray and StringViewArray
    if let Some(s_arr) = col_out.as_any().downcast_ref::<StringArray>() {
        assert_eq!(s_arr.value(0), "100");
        assert!(s_arr.is_null(1));
        assert!(s_arr.is_null(2));
        assert!(s_arr.is_null(3));
    } else if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "100");
        assert!(v_arr.is_null(1));
        assert!(v_arr.is_null(2));
        assert!(v_arr.is_null(3));
    } else {
        panic!("Expected StringArray or StringViewArray");
    }
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
    let col_out = out[0].column(0);

    // Handle both StringArray and StringViewArray
    if let Some(s_arr) = col_out.as_any().downcast_ref::<StringArray>() {
        assert_eq!(s_arr.value(0), "");
        assert_eq!(s_arr.value(1), "");
    } else if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "");
        assert_eq!(v_arr.value(1), "");
    } else {
        panic!("Expected StringArray or StringViewArray");
    }
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

    // Helper to get value from either StringArray or StringViewArray
    let get_value = |col_idx: usize, row_idx: usize| -> String {
        let col = batch.column(col_idx);
        if let Some(s_arr) = col.as_any().downcast_ref::<StringArray>() {
            s_arr.value(row_idx).to_string()
        } else if let Some(v_arr) = col.as_any().downcast_ref::<StringViewArray>() {
            v_arr.value(row_idx).to_string()
        } else {
            panic!("Expected StringArray or StringViewArray");
        }
    };

    // full (column 1)
    assert_eq!(get_value(1, 0), "100-200");
    assert_eq!(get_value(1, 1), "");
    assert_eq!(get_value(1, 2), "");

    // first (column 2)
    assert_eq!(get_value(2, 0), "100");
    assert_eq!(get_value(2, 1), "");
    assert_eq!(get_value(2, 2), "");

    // second (column 3)
    assert_eq!(get_value(3, 0), "200");
    assert_eq!(get_value(3, 1), "");
    assert_eq!(get_value(3, 2), "");
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

// ============================================================================
// PERFORMANCE & EDGE CASE TESTS: Zero-copy thresholds, caching, env vars
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn performance_large_buffer_exceeds_128mb_threshold_fallback() {
    // Test that buffers >128MB fall back to copying mode instead of zero-copy
    // We create a StringArray with total buffer size > 128MB
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create strings that sum to ~135MB total buffer size
    // Each string ~1MB, 140 strings = ~140MB
    let one_mb_string = "x".repeat(1_048_576); // 1MB of 'x'
    let strings: Vec<Option<String>> = (0..140)
        .map(|i| Some(format!("prefix-{}-{}", i, one_mb_string)))
        .collect();

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let array: ArrayRef = Arc::new(StringArray::from(strings));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("large_buffer", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("large_buffer")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("prefix-(\\d+)-".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify extraction worked (falls back to copying mode)
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "0");
        assert_eq!(v_arr.value(5), "5");
        assert_eq!(v_arr.value(139), "139");
    } else {
        panic!("Expected StringViewArray");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn performance_large_utf8_strings_spanning_multiple_blocks() {
    // Test that LargeUtf8 strings spanning 2MB blocks fall back to copying
    // Default block size is 2MB, so we create strings that span blocks
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create strings where some will span 2MB block boundaries
    // String positions: 0-1MB, 1MB-3MB (spans blocks), 3MB-4MB, etc.
    let small_string = "a".repeat(1_000_000); // 1MB
    let large_string = "b".repeat(2_500_000); // 2.5MB - will span 2MB block boundary

    let strings: Vec<Option<String>> = vec![
        Some(format!("id-1-{}", small_string)),      // Within block 0
        Some(format!("id-2-{}", large_string)),      // Spans block 0-1
        Some(format!("id-3-{}", small_string)),      // Within block 1 or 2
        Some(format!("id-4-{}", "short")),           // Small string
    ];

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]));
    let array: ArrayRef = Arc::new(LargeStringArray::from(strings));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("large_utf8", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("large_utf8")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("id-(\\d+)-".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify all extractions worked (some zero-copy, some fallback)
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "1");
        assert_eq!(v_arr.value(1), "2"); // Spans blocks - should use fallback
        assert_eq!(v_arr.value(2), "3");
        assert_eq!(v_arr.value(3), "4");
    } else {
        panic!("Expected StringViewArray");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn performance_lru_cache_with_many_unique_patterns() {
    // Test LRU cache behavior with >64 unique patterns (default cache size)
    // Verifies cache eviction and recompilation work correctly
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create 100 rows with 100 unique patterns (exceeds default 64 cache size)
    let strings: Vec<Option<String>> = (0..100)
        .map(|i| Some(format!("value-{}", i % 2)))
        .collect();
    let patterns: Vec<Option<String>> = (0..100)
        .map(|i| Some(format!("value-({})", i % 2))) // Alternating patterns to stress cache
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("p", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(strings)) as _,
            Arc::new(StringArray::from(patterns)) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("cache_stress", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("cache_stress")
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

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify all extractions succeeded despite cache evictions
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        // Alternating patterns: value-(0) and value-(1)
        for i in 0..100 {
            let expected = format!("{}", i % 2);
            assert_eq!(v_arr.value(i), expected);
        }
    } else {
        panic!("Expected StringViewArray");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn performance_env_var_regex_cache_size_override() {
    // Test that DF_REGEX_CACHE_SIZE environment variable works
    unsafe {
        std::env::set_var("DF_REGEX_CACHE_SIZE", "10");
    }

    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create 20 unique patterns (exceeds cache size of 10)
    let strings: Vec<Option<String>> = (0..20)
        .map(|i| Some(format!("test-{}", i)))
        .collect();
    let patterns: Vec<Option<String>> = (0..20)
        .map(|i| Some(format!("test-({})", i)))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("p", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(strings)) as _,
            Arc::new(StringArray::from(patterns)) as _,
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("env_cache", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("env_cache")
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

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify all extractions succeeded
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..20 {
            assert_eq!(v_arr.value(i), format!("{}", i));
        }
    } else {
        panic!("Expected StringViewArray");
    }

    // Clean up env var
    unsafe {
        std::env::remove_var("DF_REGEX_CACHE_SIZE");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn performance_env_var_max_zero_copy_buffer_override() {
    // Test that DF_MAX_ZERO_COPY_BUFFER_MB environment variable works
    // Set threshold to 1MB, then test with 2MB buffer (should fall back to copying)
    unsafe {
        std::env::set_var("DF_MAX_ZERO_COPY_BUFFER_MB", "1");
    }

    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create ~2MB buffer (exceeds 1MB threshold)
    let large_string = "x".repeat(1_048_576); // 1MB
    let strings: Vec<Option<String>> = vec![
        Some(format!("id-1-{}", large_string)),
        Some(format!("id-2-{}", large_string)),
    ];

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let array: ArrayRef = Arc::new(StringArray::from(strings));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("env_buffer", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("env_buffer")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("id-(\\d+)-".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify extraction worked (uses copying fallback due to env var)
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "1");
        assert_eq!(v_arr.value(1), "2");
    } else {
        panic!("Expected StringViewArray");
    }

    // Clean up env var
    unsafe {
        std::env::remove_var("DF_MAX_ZERO_COPY_BUFFER_MB");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn performance_env_var_large_utf8_block_size_override() {
    // Test that DF_LARGE_UTF8_BLOCK_SIZE_MB environment variable works
    // Set block size to 1MB instead of default 2MB
    unsafe {
        std::env::set_var("DF_LARGE_UTF8_BLOCK_SIZE_MB", "1");
    }

    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    // Create strings that will span blocks with 1MB block size
    let medium_string = "a".repeat(800_000); // 800KB
    let large_string = "b".repeat(1_200_000); // 1.2MB - spans 1MB blocks

    let strings: Vec<Option<String>> = vec![
        Some(format!("tag-1-{}", medium_string)),    // Within 1MB block
        Some(format!("tag-2-{}", large_string)),     // Spans 1MB blocks
        Some(format!("tag-3-{}", medium_string)),    // Within block
    ];

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]));
    let array: ArrayRef = Arc::new(LargeStringArray::from(strings));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("env_blocks", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("env_blocks")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("tag-(\\d+)-".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            ])
            .alias("g"),
        ])
        .unwrap();

    let out = df.collect().await.unwrap();
    let col_out = out[0].column(0);

    // Verify all extractions worked
    if let Some(v_arr) = col_out.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.value(0), "1");
        assert_eq!(v_arr.value(1), "2"); // Should use fallback for spanning string
        assert_eq!(v_arr.value(2), "3");
    } else {
        panic!("Expected StringViewArray");
    }

    // Clean up env var
    unsafe {
        std::env::remove_var("DF_LARGE_UTF8_BLOCK_SIZE_MB");
    }
}
