use datafusion::arrow::array::{Array, ArrayRef, GenericStringArray, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
use regex::Regex;
use std::sync::Arc;

// Spark-compatible behavior:
// - idx >= 0
// - idx == 0 -> entire match
// - No match or group out-of-range -> empty string (not NULL)
// - Any NULL input -> result NULL
pub fn eval_regexp_extract_utf8(arr: &StringArray, regex: &Regex, idx: i32) -> Result<ArrayRef> {
    if idx < 0 {
        return Err(DataFusionError::Execution(
            "regexp_extract: idx must be >= 0".into(),
        ));
    }
    let len = arr.len();
    let mut b = StringBuilder::new();
    for i in 0..len {
        if arr.is_null(i) {
            b.append_null();
            continue;
        }
        let s = arr.value(i);
        if let Some(caps) = regex.captures(s) {
            let out = if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("")
            } else {
                caps.get(idx as usize).map(|m| m.as_str()).unwrap_or("")
            };
            b.append_value(out);
        } else {
            b.append_value("");
        }
    }
    Ok(Arc::new(b.finish()))
}

fn try_as_string_array(arr: &ArrayRef, dtype: &DataType) -> Result<StringArray> {
    match dtype {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>();
            if let Some(a) = a {
                Ok(a.clone())
            } else {
                Err(DataFusionError::Execution("expected Utf8".into()))
            }
        }
        DataType::LargeUtf8 => {
            let a64 = arr.as_any().downcast_ref::<GenericStringArray<i64>>();
            if a64.is_none() {
                return Err(DataFusionError::Execution("expected LargeUtf8".into()));
            }
            let a64 = a64.unwrap();
            let mut b = StringBuilder::new();
            for i in 0..a64.len() {
                if a64.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(a64.value(i));
                }
            }
            Ok(b.finish())
        }
        other => Err(DataFusionError::Execution(format!(
            "unsupported data type: {other:?}"
        ))),
    }
}

/// Create and return the Scalar UDF for registration on SessionContext.
/// Callers invoke via DataFrame/Expr API (no SQL text required).
pub fn make_regexp_extract_udf() -> datafusion::logical_expr::ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "regexp_extract expects 3 args: (str, pattern, idx)".into(),
            ));
        }

        // Determine result length with broadcasting rules
        let len = match (&args[0], &args[1], &args[2]) {
            (ColumnarValue::Array(a0), ColumnarValue::Array(a1), ColumnarValue::Array(a2)) => {
                let l = a0.len();
                if a1.len() != l || a2.len() != l {
                    return Err(DataFusionError::Execution(
                        "regexp_extract: array args must have same length".into(),
                    ));
                }
                l
            }
            (ColumnarValue::Array(a), _, _) => a.len(),
            (_, ColumnarValue::Array(a), _) => a.len(),
            (_, _, ColumnarValue::Array(a)) => a.len(),
            _ => 1,
        };

        // Helper to get idx per row
        fn get_idx_at(args: &[ColumnarValue], i: usize) -> Result<Option<i32>> {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Int32(v)) => Ok(*v),
                ColumnarValue::Array(col) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int32Array>()
                        .ok_or_else(|| DataFusionError::Execution("idx must be Int32".into()))?;
                    if arr.is_null(i) {
                        Ok(None)
                    } else {
                        Ok(Some(arr.value(i)))
                    }
                }
                _ => Err(DataFusionError::Execution(
                    "idx must be Int32/Scalar".into(),
                )),
            }
        }

        let mut out = StringBuilder::new();

        // Precompile scalar pattern
        let scalar_pattern = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(p))) => Some(
                Regex::new(p)
                    .map_err(|e| DataFusionError::Execution(format!("invalid regex: {e}")))?,
            ),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => None,
            _ => None,
        };

        for i in 0..len {
            // Null propagation
            let null_str = match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v.is_none(),
                ColumnarValue::Array(a) => match a.data_type() {
                    DataType::Utf8 => a.as_any().downcast_ref::<StringArray>().unwrap().is_null(i),
                    DataType::LargeUtf8 => a
                        .as_any()
                        .downcast_ref::<GenericStringArray<i64>>()
                        .unwrap()
                        .is_null(i),
                    _ => true,
                },
                _ => true,
            };

            let null_pat = match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v.is_none(),
                ColumnarValue::Array(a) => {
                    let sa = a
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Execution("pattern must be Utf8".into()))?;
                    sa.is_null(i)
                }
                _ => true,
            };

            let null_idx = get_idx_at(args, i)?.is_none();

            if null_str || null_pat || null_idx {
                out.append_null();
                continue;
            }

            // Row string
            let s_val: String = match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
                ColumnarValue::Array(a) => {
                    let s_arr = try_as_string_array(a, a.data_type())?;
                    s_arr.value(i).to_string()
                }
                _ => unreachable!(),
            };

            // Regex for this row
            let re_owned;
            let re_ref: &Regex = if let Some(re) = &scalar_pattern {
                re
            } else {
                match &args[1] {
                    ColumnarValue::Array(a) => {
                        let p_arr = a.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            DataFusionError::Execution("pattern must be Utf8".into())
                        })?;
                        let pat = p_arr.value(i);
                        re_owned = Regex::new(pat).map_err(|e| {
                            DataFusionError::Execution(format!("invalid regex: {e}"))
                        })?;
                        &re_owned
                    }
                    _ => unreachable!(),
                }
            };

            // Row idx
            let idx = get_idx_at(args, i)?.unwrap();
            if idx < 0 {
                return Err(DataFusionError::Execution(
                    "regexp_extract: idx must be >= 0".into(),
                ));
            }

            // Evaluate
            if let Some(caps) = re_ref.captures(&s_val) {
                let out_s = if idx == 0 {
                    caps.get(0).map(|m| m.as_str()).unwrap_or("")
                } else {
                    caps.get(idx as usize).map(|m| m.as_str()).unwrap_or("")
                };
                out.append_value(out_s);
            } else {
                out.append_value("");
            }
        }

        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    });

    create_udf(
        "regexp_extract",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
        DataType::Utf8,
        Volatility::Immutable,
        fun,
    )
}
