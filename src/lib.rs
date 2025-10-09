use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray, StringViewBuilder,
    new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
    expr_fn::SimpleScalarUDF,
};
use lru::LruCache;
use regex::Regex;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(feature = "pcre2-fallback")]
use pcre2::bytes::{Regex as Pcre2Regex, RegexBuilder as Pcre2Builder};

// ============================================================================
// COMPILED REGEX HELPERS
// ============================================================================

#[cfg(feature = "pcre2-fallback")]
/// Builds the PCRE2 limit directive prefix using optional env overrides.
fn pcre2_limit_prefix() -> String {
    // Defaults guard against runaway backtracking
    let match_limit: u32 = std::env::var("DF_PCRE2_MATCH_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000); // 1M attempts

    let depth_limit: u32 = std::env::var("DF_PCRE2_DEPTH_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(match_limit); // Mirror match_limit

    let heap_limit_kib: u32 = std::env::var("DF_PCRE2_HEAP_LIMIT_KIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8 * 1024); // 8MB heap

    format!(
        "(*LIMIT_MATCH={})(*LIMIT_DEPTH={})(*LIMIT_HEAP={})",
        match_limit, depth_limit, heap_limit_kib
    )
}

/// Regex wrapper that optionally falls back to PCRE2 for advanced features.
#[derive(Clone)]
enum CompiledRegex {
    Fast(Arc<Regex>),
    #[cfg(feature = "pcre2-fallback")]
    Pcre2(Arc<Pcre2Regex>),
}

impl CompiledRegex {
    /// Tries the fast regex crate first, then PCRE2 if enabled.
    fn new_with_fallback(pattern: &str) -> Result<Self> {
        // Prefer the fast regex crate
        if let Ok(r) = Regex::new(pattern) {
            return Ok(Self::Fast(Arc::new(r)));
        }

        // Use PCRE2 when available
        #[cfg(feature = "pcre2-fallback")]
        {
            // Apply guard rails before compiling
            let limited_pattern = format!("{}{}", pcre2_limit_prefix(), pattern);

            let regex = Pcre2Builder::new()
                .jit_if_available(true) // Enable JIT when possible
                .ucp(true) // Unicode-aware character classes
                .utf(true)
                .build(&limited_pattern)
                .map_err(|e| DataFusionError::Execution(format!("invalid regex: {e}")))?;
            Ok(Self::Pcre2(Arc::new(regex)))
        }

        // Fallback disabled
        #[cfg(not(feature = "pcre2-fallback"))]
        Err(DataFusionError::Execution(format!(
            "invalid regex: if this is a valid java-regex, enable the pcre2-fallback feature for lookaround/backrefs: {}",
            pattern
        )))
    }

    /// Extract capture group from text at specified index.
    /// Optimized fast path for idx=0 (full match) using find() instead of captures().
    #[inline(always)]
    fn get_capture<'t>(&self, text: &'t str, idx: usize) -> &'t str {
        // Fast path: idx=0 (full match) - use find() to avoid building capture groups
        if idx == 0 {
            return match self {
                Self::Fast(r) => r.find(text).map(|m| m.as_str()).unwrap_or(""),
                #[cfg(feature = "pcre2-fallback")]
                Self::Pcre2(r) => {
                    let bytes = text.as_bytes();
                    match r.find(bytes) {
                        Ok(Some(m)) => &text[m.start()..m.end()],
                        _ => "",
                    }
                }
            };
        }

        // Normal path: idx>0 - need capture groups
        match self {
            Self::Fast(r) => r
                .captures(text)
                .and_then(|caps| caps.get(idx).map(|m| m.as_str()))
                .unwrap_or(""),
            #[cfg(feature = "pcre2-fallback")]
            Self::Pcre2(r) => {
                let bytes = text.as_bytes();
                match r.captures(bytes) {
                    Ok(Some(caps)) => match caps.get(idx) {
                        Some(m) => &text[m.start()..m.end()],
                        None => "",
                    },
                    _ => "",
                }
            }
        }
    }
}

// ============================================================================
// PER-BATCH CACHE
// ============================================================================

/// LRU cache of compiled regexes scoped to a single batch.
struct RegexCache {
    lru: LruCache<String, CompiledRegex>,
}

impl RegexCache {
    /// Creates a cache with the given capacity.
    #[inline]
    fn new(capacity: usize) -> Self {
        Self {
            lru: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    /// Returns a cached regex or compiles and caches it.
    #[inline]
    fn get_or_insert(&mut self, pattern: &str) -> Result<CompiledRegex> {
        // Cache hit
        if let Some(cached) = self.lru.get(pattern) {
            return Ok(cached.clone());
        }

        // Cache miss
        let compiled = CompiledRegex::new_with_fallback(pattern)?;

        // Store result
        self.lru.put(pattern.to_string(), compiled.clone());

        Ok(compiled)
    }
}

// ============================================================================
// PUBLIC API: UDF Factory
// ============================================================================

/// Public entry point: build and return the DataFusion `ScalarUDF` registered as `"regexp_extract"`.
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
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View, DataType::Int32]),
    ];

    let signature = Signature::one_of(signatures, Volatility::Immutable);
    let fun: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| regexp_extract_udf_impl(args));

    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "regexp_extract",
        signature,
        DataType::Utf8View,
        fun,
    ))
}

// ============================================================================
// UDF IMPLEMENTATION: Argument Handling
// ============================================================================

/// Implements the core UDF logic, extracting ColumnarValue inputs and calling kernel.
#[inline]
fn regexp_extract_udf_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let default_idx = ColumnarValue::Scalar(ScalarValue::Int32(Some(1)));
    let (str_arg, pat_arg, idx_arg) = match args.len() {
        3 => (&args[0], &args[1], &args[2]),
        2 => (&args[0], &args[1], &default_idx),
        _ => {
            return Err(DataFusionError::Execution(
                "regexp_extract expects two or three arguments".to_string(),
            ));
        }
    };

    let result = regexp_extract_kernel(str_arg, pat_arg, idx_arg)?;
    Ok(ColumnarValue::Array(result))
}

// ============================================================================
// CORE KERNEL: Regex Extraction Logic
// ============================================================================

/// Core kernel for regexp_extract, handling scalar/columnar string, pattern, and index inputs.
/// Two execution paths: (1) Scalar pattern - compile once, (2) Column pattern - compile per row.
fn regexp_extract_kernel(
    str_arg: &ColumnarValue,
    pat_arg: &ColumnarValue,
    idx_arg: &ColumnarValue,
) -> Result<ArrayRef> {
    let string_input = ColumnString::new(str_arg, "str")?;
    let pattern_input = ColumnString::new(pat_arg, "pattern")?;
    let idx_input = Int32Input::new(idx_arg)?;

    if let Some(k) = idx_input.scalar_value()
        && k < 0
    {
        return Err(DataFusionError::Plan(
            "regexp_extract: idx must be >= 0 (negative literal)".to_string(),
        ));
    }

    let n = string_input
        .len()
        .max(pattern_input.len())
        .max(idx_input.len());

    if n == 0 {
        return Ok(new_null_array(&DataType::Utf8View, 0));
    }

    if pattern_input.is_scalar_null() || idx_input.is_scalar_null() {
        return Ok(new_null_array(&DataType::Utf8View, n));
    }

    // StringViewBuilder only takes row capacity (manages own buffer strategy)
    let mut builder = StringViewBuilder::with_capacity(n);

    // Fast path: Scalar pattern (compile regex once, reuse for all rows)
    if let Some(pattern) = pattern_input.scalar_value() {
        let compiled = CompiledRegex::new_with_fallback(pattern)?;

        match compiled {
            CompiledRegex::Fast(regex) => {
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
                    let capture = regex
                        .captures(s)
                        .and_then(|caps| caps.get(idx_value as usize).map(|m| m.as_str()))
                        .unwrap_or("");
                    builder.append_value(capture);
                }
            }
            #[cfg(feature = "pcre2-fallback")]
            CompiledRegex::Pcre2(regex) => {
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
                    let bytes = s.as_bytes();
                    let capture = match regex.captures(bytes) {
                        Ok(Some(caps)) => {
                            // Map PCRE2 byte offsets into the original string
                            match caps.get(idx_value as usize) {
                                Some(m) => &s[m.start()..m.end()],
                                None => "",
                            }
                        }
                        _ => "",
                    };
                    builder.append_value(capture);
                }
            }
        }
        return Ok(Arc::new(builder.finish()));
    }

    // Column patterns: use an LRU cache to limit recompilation
    let mut cache = RegexCache::new(64); // Batch-scoped cache

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

        // Compile each unique pattern once per batch
        let regex = cache.get_or_insert(pattern)?;
        let capture = regex.get_capture(s, idx_value as usize);
        builder.append_value(capture);
    }

    Ok(Arc::new(builder.finish()))
}

// ============================================================================
// GENERIC INPUT WRAPPERS
// ============================================================================

/// Wraps a scalar or array input while monomorphizing cleanly.
enum Input<'a, S, A> {
    Scalar(Option<S>),
    Array(&'a A),
}

impl<'a, S: Clone, A: Array> Input<'a, S, A> {
    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::Scalar(_) => 1,
            Self::Array(arr) => arr.len(),
        }
    }

    #[inline(always)]
    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::Array(arr) => arr.is_null(index),
        }
    }

    #[inline(always)]
    fn is_scalar_null(&self) -> bool {
        matches!(self, Self::Scalar(None))
    }
}

// Type-specific implementations for Int32
impl<'a> Input<'a, i32, Int32Array> {
    fn new(arg: &'a ColumnarValue) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    Ok(Self::Scalar(None))
                } else {
                    match value {
                        ScalarValue::Int32(opt) => Ok(Self::Scalar(*opt)),
                        _ => Err(DataFusionError::Execution(
                            "expected Int32 scalar for idx".to_string(),
                        )),
                    }
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

    #[inline(always)]
    fn value_unchecked(&self, index: usize) -> i32 {
        match self {
            Self::Scalar(Some(v)) => *v,
            Self::Array(arr) => arr.value(index),
            Self::Scalar(None) => unreachable!("null filtered via mask"),
        }
    }

    #[inline(always)]
    fn scalar_value(&self) -> Option<i32> {
        match self {
            Self::Scalar(v) => *v,
            _ => None,
        }
    }
}

// Type alias for Int32 inputs
type Int32Input<'a> = Input<'a, i32, Int32Array>;

// ============================================================================
// INPUT HELPERS: String (Utf8/LargeUtf8/Scalar)
// ============================================================================

struct ColumnString<'a> {
    scalar: Option<Option<String>>,
    utf8: Option<&'a StringArray>,
    large_utf8: Option<&'a LargeStringArray>,
    utf8_view: Option<&'a StringViewArray>,
}

impl<'a> ColumnString<'a> {
    fn new(arg: &'a ColumnarValue, label: &str) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    Ok(Self {
                        scalar: Some(None),
                        utf8: None,
                        large_utf8: None,
                        utf8_view: None,
                    })
                } else {
                    match value {
                        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => Ok(Self {
                            scalar: Some(opt.clone()),
                            utf8: None,
                            large_utf8: None,
                            utf8_view: None,
                        }),
                        ScalarValue::Utf8View(opt) => Ok(Self {
                            scalar: Some(opt.clone()),
                            utf8: None,
                            large_utf8: None,
                            utf8_view: None,
                        }),
                        _ => Err(DataFusionError::Execution(format!(
                            "expected Utf8/LargeUtf8/Utf8View scalar for {label}"
                        ))),
                    }
                }
            }
            ColumnarValue::Array(array) => {
                if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(Self {
                        scalar: None,
                        utf8: Some(utf8),
                        large_utf8: None,
                        utf8_view: None,
                    })
                } else if let Some(large) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(Self {
                        scalar: None,
                        utf8: None,
                        large_utf8: Some(large),
                        utf8_view: None,
                    })
                } else if let Some(utf8_view) = array.as_any().downcast_ref::<StringViewArray>() {
                    Ok(Self {
                        scalar: None,
                        utf8: None,
                        large_utf8: None,
                        utf8_view: Some(utf8_view),
                    })
                } else {
                    Err(DataFusionError::Execution(format!(
                        "expected Utf8 or LargeUtf8 or Utf8View array for {label}"
                    )))
                }
            }
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        if self.scalar.is_some() {
            1
        } else if let Some(arr) = self.utf8 {
            arr.len()
        } else if let Some(arr) = self.large_utf8 {
            arr.len()
        } else if let Some(arr) = self.utf8_view {
            arr.len()
        } else {
            0
        }
    }

    #[inline(always)]
    fn is_null(&self, index: usize) -> bool {
        if let Some(value) = &self.scalar {
            value.is_none()
        } else if let Some(arr) = self.utf8 {
            arr.is_null(index)
        } else if let Some(arr) = self.large_utf8 {
            arr.is_null(index)
        } else if let Some(arr) = self.utf8_view {
            arr.is_null(index)
        } else {
            true
        }
    }

    #[inline(always)]
    fn scalar_value(&self) -> Option<&str> {
        self.scalar.as_ref().and_then(|opt| opt.as_deref())
    }

    #[inline(always)]
    fn is_scalar_null(&self) -> bool {
        matches!(self.scalar, Some(None))
    }

    #[inline(always)]
    fn value_unchecked(&self, index: usize) -> &str {
        if let Some(Some(value)) = self.scalar.as_ref() {
            value
        } else if let Some(arr) = self.utf8 {
            arr.value(index)
        } else if let Some(arr) = self.large_utf8 {
            arr.value(index)
        } else if let Some(arr) = self.utf8_view {
            arr.value(index)
        } else {
            unreachable!("string input must be scalar or array")
        }
    }

    #[inline(always)]
    fn byte_capacity_hint(&self) -> usize {
        if let Some(Some(value)) = self.scalar.as_ref() {
            value.len()
        } else if let Some(arr) = self.utf8 {
            arr.values().len()
        } else if let Some(arr) = self.large_utf8 {
            arr.values().len()
        } else if let Some(arr) = self.utf8_view {
            // StringView stores data in buffers, not a single values array
            arr.data_buffers().iter().map(|buf| buf.len()).sum()
        } else {
            0
        }
    }
}
