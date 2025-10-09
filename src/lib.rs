//! TODO Roadmap (iterations)
//! 1) Step-2: Regex compatibility fallback
//!    - Auto-fallback to full PCRE features (look-around/backrefs) when `regex` compile fails.
//!    - Option A: `fancy-regex` (pure Rust). Option B (feature-gated): PCRE2 via FFI.
//!    - Safety knobs: match timeout / step budget; telemetry for fallback hits.
//!
//! 2) Step-3: Column pattern/idx baseline (done in Step-1 brute-force)
//!    - Keep per-row compile for correctness; measure cost (criterion benches).
//!    - Ensure NULL fast paths remain tight for column args.
//!
//! 3) Step-4: Per-batch LRU cache for column patterns
//!    - Tiny LRU (cap 32–128) mapping `pattern -> compiled` to avoid re-compiles in a batch.
//!    - Batch-scoped only (no global locks); record LRU hit/miss.
//!
//! 4) Step-5: Parallel compile of unique patterns (heuristic, bounded)
//!    - Detect many uniques (e.g., unique_count > 64 && unique_ratio > 0.25).
//!    - Pre-scan batch, compile uniques with a small thread pool (<= half physical cores).
//!    - Matching remains single-threaded for cache locality.
//!
//! 5) Step-6: Benchmarks & metrics
//!    - Criterion suites: scalar pat, low/mid/high-card column pat; batch sizes 8k/64k/256k.
//!    - Export counters: fallback_hits, lru_hits, parallel_compile_time_ms, rows/s.
//!
//! 6) Step-7: String ergonomics
//!    - Add zero-copy support for `LargeUtf8` / `StringView` (avoid copying into Utf8).
//!    - Confirm capacity hints stay accurate with new string forms.
//!
//! 7) Step-8: Spark parity polish
//!    - Confirm literal negative `idx` fails at plan time (done); column negative at runtime.
//!    - Document engine differences and fallback semantics.
//!
//! 8) Step-9: Configuration surface
//!    - Feature flag `pcre2` for PCRE backend; env/config for match limits and pool size.
//!    - Expose knobs via UDF registry metadata if needed.
//!
//! 9) Step-10: Robustness
//!    - Add property tests (proptest) and fuzzing for regex/match paths.
//!    - Add adversarial patterns (catastrophic backtracking) to validate guards.
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, StringArray, StringBuilder, new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
    expr_fn::SimpleScalarUDF,
};
use regex::Regex;
use std::sync::Arc;

/// Evaluate regexp_extract for a concrete `Utf8` array and pre-compiled pattern.
#[inline]
pub fn eval_regexp_extract_utf8(input: &StringArray, regex: &Regex, idx: i32) -> Result<ArrayRef> {
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

/// Build a DataFusion scalar UDF that performs Spark-compatible `regexp_extract`.
#[inline]
pub fn make_regexp_extract_udf() -> ScalarUDF {
    let signatures = vec![
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::Int32,
        ]),
    ];

    let signature = Signature::one_of(signatures, Volatility::Immutable);
    let fun: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| regexp_extract_udf_impl(args));

    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "regexp_extract",
        signature,
        DataType::Utf8,
        fun,
    ))
}

#[inline]
fn regexp_extract_udf_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(
            "regexp_extract expects three arguments".to_string(),
        ));
    }

    let result = regexp_extract_kernel(&args[0], &args[1], &args[2])?;
    Ok(ColumnarValue::Array(result))
}

#[inline]
fn all_null_utf8(n: usize) -> ArrayRef {
    new_null_array(&DataType::Utf8, n)
}

fn regexp_extract_kernel(
    str_arg: &ColumnarValue,
    pat_arg: &ColumnarValue,
    idx_arg: &ColumnarValue,
) -> Result<ArrayRef> {
    let string_input = StringInput::new(str_arg, "str")?;
    let pattern_input = StringInput::new(pat_arg, "pattern")?;
    let idx_input = Int32Input::new(idx_arg)?;

    if let Int32Input::Scalar(Some(k)) = idx_input {
        if k < 0 {
            return Err(DataFusionError::Plan(
                "regexp_extract: idx must be >= 0 (negative literal)".to_string(),
            ));
        }
    }

    let n = string_input
        .len()
        .max(pattern_input.len())
        .max(idx_input.len());

    if n == 0 {
        return Ok(all_null_utf8(0));
    }

    if pattern_input.is_scalar_null() || idx_input.is_scalar_null() {
        return Ok(all_null_utf8(n));
    }

    let data_capacity = string_input.byte_capacity_hint();
    let mut builder = StringBuilder::with_capacity(n, data_capacity);

    if let Some(pattern) = pattern_input.scalar_value() {
        let regex = compile_regex(pattern)?;
        for i in 0..n {
            if string_input.is_null(i) || idx_input.is_null(i) {
                builder.append_null();
                continue;
            }
            let s = string_input.value_unchecked(i);
            let idx_value = idx_input.value_unchecked(i);
            if idx_value < 0 {
                return Err(DataFusionError::Execution("idx must be >= 0".to_string()));
            }
            append_capture(&mut builder, s, &regex, idx_value as usize);
        }
        return Ok(Arc::new(builder.finish()));
    }

    for i in 0..n {
        if string_input.is_null(i) || pattern_input.is_null(i) || idx_input.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = string_input.value_unchecked(i);
        let pattern = pattern_input.value_unchecked(i);
        let idx_value = idx_input.value_unchecked(i);
        if idx_value < 0 {
            return Err(DataFusionError::Execution("idx must be >= 0".to_string()));
        }
        let regex = compile_regex(pattern)?;
        append_capture(&mut builder, s, &regex, idx_value as usize);
    }

    Ok(Arc::new(builder.finish()))
}

#[inline]
fn append_capture(builder: &mut StringBuilder, input: &str, regex: &Regex, idx: usize) {
    match regex.captures(input) {
        Some(caps) => {
            let s = caps.get(idx).map(|m| m.as_str()).unwrap_or("");
            builder.append_value(s);
        }
        None => builder.append_value(""),
    }
}

#[inline]
fn compile_regex(pattern: &str) -> Result<Regex> {
    Regex::new(pattern).map_err(|err| DataFusionError::Execution(format!("invalid regex: {err}")))
}

enum StringInput<'a> {
    Scalar(Option<String>),
    ArrayUtf8(&'a StringArray),
    ArrayLargeUtf8(&'a LargeStringArray),
}

impl<'a> StringInput<'a> {
    fn new(arg: &'a ColumnarValue, label: &str) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    return Ok(Self::Scalar(None));
                }
                match value {
                    ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => {
                        Ok(Self::Scalar(opt.clone()))
                    }
                    _ => Err(DataFusionError::Execution(format!(
                        "expected Utf8/LargeUtf8 scalar for {label}"
                    ))),
                }
            }
            ColumnarValue::Array(array) => {
                if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(Self::ArrayUtf8(utf8))
                } else if let Some(large) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(Self::ArrayLargeUtf8(large))
                } else {
                    Err(DataFusionError::Execution(format!(
                        "expected Utf8 or LargeUtf8 array for {label}"
                    )))
                }
            }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Scalar(_) => 1,
            Self::ArrayUtf8(arr) => arr.len(),
            Self::ArrayLargeUtf8(arr) => arr.len(),
        }
    }

    #[inline]
    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::ArrayUtf8(arr) => arr.is_null(index),
            Self::ArrayLargeUtf8(arr) => arr.is_null(index),
        }
    }

    #[inline]
    fn scalar_value(&self) -> Option<&str> {
        match self {
            Self::Scalar(Some(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    #[inline]
    fn is_scalar_null(&self) -> bool {
        matches!(self, Self::Scalar(None))
    }

    #[inline]
    fn value_unchecked(&self, index: usize) -> &str {
        match self {
            Self::Scalar(Some(value)) => value.as_str(),
            Self::ArrayUtf8(arr) => arr.value(index),
            Self::ArrayLargeUtf8(arr) => arr.value(index),
            Self::Scalar(None) => unreachable!("nulls filtered via mask"),
        }
    }

    #[inline]
    fn byte_capacity_hint(&self) -> usize {
        match self {
            Self::Scalar(Some(value)) => value.len(),
            Self::Scalar(None) => 0,
            Self::ArrayUtf8(arr) => arr.values().len(),
            Self::ArrayLargeUtf8(arr) => arr.values().len(),
        }
    }
}

enum Int32Input<'a> {
    Scalar(Option<i32>),
    Array(&'a Int32Array),
}

impl<'a> Int32Input<'a> {
    fn new(arg: &'a ColumnarValue) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    return Ok(Self::Scalar(None));
                }
                match value {
                    ScalarValue::Int32(opt) => Ok(Self::Scalar(*opt)),
                    _ => Err(DataFusionError::Execution(
                        "expected Int32 scalar for idx".to_string(),
                    )),
                }
            }
            ColumnarValue::Array(array) => {
                if let Some(ints) = array.as_any().downcast_ref::<Int32Array>() {
                    Ok(Self::Array(ints))
                } else {
                    Err(DataFusionError::Execution(
                        "expected Int32 array for idx".to_string(),
                    ))
                }
            }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Scalar(_) => 1,
            Self::Array(arr) => arr.len(),
        }
    }

    #[inline]
    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::Array(arr) => arr.is_null(index),
        }
    }

    #[inline]
    fn is_scalar_null(&self) -> bool {
        matches!(self, Self::Scalar(None))
    }

    #[inline]
    fn value_unchecked(&self, index: usize) -> i32 {
        match self {
            Self::Scalar(Some(value)) => *value,
            Self::Array(arr) => arr.value(index),
            Self::Scalar(None) => unreachable!("nulls filtered via mask"),
        }
    }
}
