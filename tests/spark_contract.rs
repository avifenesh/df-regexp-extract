use datafusion::arrow::array::Int32Array;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, StringViewArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use df_regexp_extract::make_regexp_extract_udf;
use std::sync::Arc;

fn assert_single_string(result: Vec<RecordBatch>, expected: &str) {
    assert_string_column(result, &[expected]);
}

fn assert_string_column(result: Vec<RecordBatch>, expected: &[&str]) {
    assert_eq!(
        result.len(),
        1,
        "expected single batch, found {}",
        result.len()
    );
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 1, "expected single column output");
    let col = batch.column(0);

    // Handle both StringArray (Utf8) and StringViewArray (Utf8View)
    if let Some(str_array) = col.as_any().downcast_ref::<StringArray>() {
        assert_eq!(str_array.len(), expected.len(), "row count mismatch");
        for (idx, value) in expected.iter().enumerate() {
            assert_eq!(str_array.value(idx), *value, "mismatch at row {idx}");
        }
    } else if let Some(view_array) = col.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(view_array.len(), expected.len(), "row count mismatch");
        for (idx, value) in expected.iter().enumerate() {
            assert_eq!(view_array.value(idx), *value, "mismatch at row {idx}");
        }
    } else {
        panic!("Expected StringArray or StringViewArray, got {:?}", col.data_type());
    }
}

fn assert_string_column_with_nulls(result: Vec<RecordBatch>, expected: &[Option<&str>]) {
    assert_eq!(
        result.len(),
        1,
        "expected single batch, found {}",
        result.len()
    );
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 1, "expected single column output");
    let col = batch.column(0);

    // Handle both StringArray (Utf8) and StringViewArray (Utf8View)
    if let Some(str_array) = col.as_any().downcast_ref::<StringArray>() {
        assert_eq!(str_array.len(), expected.len(), "row count mismatch");
        for (idx, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(
                        !str_array.is_null(idx),
                        "expected non-null at row {idx}, found null"
                    );
                    assert_eq!(str_array.value(idx), *value, "value mismatch at row {idx}");
                }
                None => {
                    assert!(
                        str_array.is_null(idx),
                        "expected null at row {idx}, found non-null"
                    );
                }
            }
        }
    } else if let Some(view_array) = col.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(view_array.len(), expected.len(), "row count mismatch");
        for (idx, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(value) => {
                    assert!(
                        !view_array.is_null(idx),
                        "expected non-null at row {idx}, found null"
                    );
                    assert_eq!(view_array.value(idx), *value, "value mismatch at row {idx}");
                }
                None => {
                    assert!(
                        view_array.is_null(idx),
                        "expected null at row {idx}, found non-null"
                    );
                }
            }
        }
    } else {
        panic!("Expected StringArray or StringViewArray, got {:?}", col.data_type());
    }
}

fn assert_group_count(result: Vec<RecordBatch>, expected_key: &str, expected_count: u64) {
    assert_eq!(
        result.len(),
        1,
        "expected single batch, found {}",
        result.len()
    );
    let batch = &result[0];
    assert_eq!(batch.num_columns(), 2, "expected two columns (key, count)");

    // Handle both StringArray and StringViewArray for key column
    let key_col = batch.column(0);
    let key_value = if let Some(s_arr) = key_col.as_any().downcast_ref::<StringArray>() {
        assert_eq!(s_arr.len(), 1, "expected single row");
        s_arr.value(0)
    } else if let Some(v_arr) = key_col.as_any().downcast_ref::<StringViewArray>() {
        assert_eq!(v_arr.len(), 1, "expected single row");
        v_arr.value(0)
    } else {
        panic!("Expected StringArray or StringViewArray for key column");
    };

    let count_value = match batch.column(1).data_type() {
        DataType::UInt64 => batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 count column")
            .value(0),
        DataType::Int64 => batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count column")
            .value(0) as u64,
        other => panic!("unexpected count type {other:?}"),
    };
    assert_eq!(batch.column(1).len(), 1, "expected single row");
    assert_eq!(key_value, expected_key);
    assert_eq!(count_value, expected_count);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_doc_example_literal_pattern() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "100");
}

/// Spark docs suggest using raw string literals (with `r` prefix) to avoid double escaping.
/// DataFusion SQL does not currently support that prefix, but passing an already unescaped
/// string literal is equivalent. This test ensures that behavior.
#[tokio::test(flavor = "current_thread")]
async fn spark_doc_example_raw_string_pattern() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new(
        "placeholder",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["row"])) as ArrayRef],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_raw", Arc::new(provider)).unwrap();

    let df = ctx
        .table("spark_raw")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![lit("100-200"), lit(r"(\d+)-(\d+)"), lit(1i32)])
                .alias("g"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "100");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_string_regex_extract_dataset() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("100-200"),
                Some("100-200"),
                Some("100-200"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("(\\d+)-(\\d+)"),
                Some("(\\d+)-(\\d+)"),
                Some("(\\d+)"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("300"),
                Some("400"),
                Some("400"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_regex_df", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("spark_regex_df")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![col("a"), col("b"), lit(1i32)])
                .alias("extract"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_string_column(batches, &["100", "100", "100"]);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_string_regex_extract_with_filter() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("100-200"),
                Some("100-200"),
                Some("100-200"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("(\\d+)-(\\d+)"),
                Some("(\\d+)-(\\d+)"),
                Some("(\\d+)"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("300"),
                Some("400"),
                Some("400"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_regex_filter", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("spark_regex_filter")
        .await
        .unwrap()
        .filter(col("a").is_not_null())
        .unwrap()
        .select(vec![
            udf.call(vec![col("a"), col("b"), lit(1i32)])
                .alias("extract"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_string_column(batches, &["100", "100", "100"]);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_non_matching_optional_group() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("aaaac")])) as ArrayRef],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_optional", Arc::new(provider))
        .unwrap();

    let df_one = ctx
        .table("spark_optional")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![col("s"), lit("(foo)"), lit(1i32)])
                .alias("extract"),
        ])
        .unwrap();
    let out_one = df_one.collect().await.unwrap();
    assert_single_string(out_one, "");

    let df_two = ctx
        .table("spark_optional")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![col("s"), lit("(a+)(b)?(c)"), lit(2i32)])
                .alias("extract"),
        ])
        .unwrap();
    let out_two = df_two.collect().await.unwrap();
    assert_single_string(out_two, "");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_regexp_extract_group_key() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("val", DataType::Utf8, false),
        Field::new("pattern", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("some[thing]")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("random-string")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("([a-z]+)\\[")])) as ArrayRef,
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_group", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql(
            "SELECT \
                regexp_extract(key, pattern, CAST(1 AS INT)) AS g, \
                COUNT(val) AS cnt \
             FROM spark_group \
             GROUP BY g",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_group_count(batches, "some", 1);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_lookbehind_returns_captured_digits() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('foo1', '(?<=foo)(\\d+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "1");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_negative_lookbehind_preserves_full_match() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('bar', '(?<!foo)bar', CAST(0 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "bar");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_backreference_repeats_full_match() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('abab', '(\\w+)\\1', CAST(0 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "abab");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_default_index_defaults_to_group_one() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('100-200', '(\\d+)-(\\d+)') AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "100");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_empty_input_returns_empty_string() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('', '(\\d+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_unicode_capture_preserves_prefix() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('東京-99', '(.+?)-(\\d+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "東京");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_inline_case_insensitive_flag() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('MiXeD', '(?i)([a-z]+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "MiXeD");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_positive_lookahead_selects_alphanumeric_word() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql(
            "SELECT regexp_extract('passwd7 plain A1234 a1234', \
             '(?=[^ ]*[a-z])(?=[^ ]*[0-9])[^ ]+', CAST(0 AS INT)) AS g",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "passwd7");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_named_backreference_repeats_match() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df = ctx
        .sql("SELECT regexp_extract('abab', '(?<pair>\\w\\w)\\k<pair>', CAST(0 AS INT)) AS g")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_single_string(batches, "abab");
}

#[tokio::test(flavor = "current_thread")]
async fn spark_column_pattern_and_index_null_handling() {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("i", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("aa-11"),
                Some("bb-22"),
                Some("cc"),
                Some("dd"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("(\\w+)-(\\d+)"),
                Some("(\\w+)-(\\d+)"),
                Some("(\\w+)"),
                None,
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(1)])) as ArrayRef,
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("spark_col_args", Arc::new(provider))
        .unwrap();

    let df = ctx
        .table("spark_col_args")
        .await
        .unwrap()
        .select(vec![
            udf.call(vec![col("s"), col("p"), col("i")]).alias("g"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_string_column_with_nulls(batches, &[Some("11"), None, Some("cc"), None]);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_scalar_null_inputs_propagate() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    let df_str_null = ctx
        .sql("SELECT regexp_extract(NULL, '(\\d+)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let out_str_null = df_str_null.collect().await.unwrap();
    assert_string_column_with_nulls(out_str_null, &[None]);

    let df_pat_null = ctx
        .sql("SELECT regexp_extract('123', NULL, CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let out_pat_null = df_pat_null.collect().await.unwrap();
    assert_string_column_with_nulls(out_pat_null, &[None]);
}

#[tokio::test(flavor = "current_thread")]
async fn spark_negative_lookahead_filters_prefix() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_regexp_extract_udf());

    // Negative lookahead: (?!foo) matches where "foo" does NOT follow
    // At position 1 (after 'f'), lookahead sees "oob" (not "foo") → matches
    // Then (.*) captures "oobaz"
    let df = ctx
        .sql("SELECT regexp_extract('foobaz', '(?!foo)(.*)', CAST(1 AS INT)) AS g")
        .await
        .unwrap();
    let out = df.collect().await.unwrap();
    assert_string_column(out, &["oobaz"]);
}
