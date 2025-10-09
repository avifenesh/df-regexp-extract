# Regexp Extract Test Coverage

- `kernel_happy_path`: verifies successful extraction for capture index 1 and full-match index 0.
- `kernel_nulls_and_no_match`: ensures null inputs propagate and unmatched rows yield empty strings.
- `kernel_group_out_of_range_returns_empty`: checks out-of-range capture groups return empty results.
- `kernel_optional_group_yields_empty_capture`: confirms optional groups produce empty captures when missing.
- `kernel_returns_first_match_only`: asserts only the first regex match is returned.
- `udf_via_dataframe_api`: smoke-tests UDF registration, dynamic pattern literals, and argument columns.
- `udf_idx_zero_returns_full_match`: validates index 0 returns the entire match via DataFrame API.
- `udf_large_utf8_and_null_inputs`: covers LargeUtf8 columns, null patterns, null indices, and null inputs.
- `udf_group_out_of_range_returns_empty`: ensures UDF returns empty strings for out-of-range indices.
- `udf_invalid_pattern_errors`: checks invalid regex patterns surface an execution error.
- `udf_idx_array_negative_errors`: verifies negative indices sourced from a column raise an error.
- `spark_parity_doc_examples`: mirrors Spark docs to confirm parity for typical patterns.
- `idx_negative_errors`: asserts kernel-level negative index rejects with an error.
