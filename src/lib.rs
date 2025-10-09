//! DataFusion Scalar UDF providing Spark-compatible `regexp_extract` function.
//!
//! This library implements regex pattern extraction with optimizations for high-performance DataFusion query execution.
//!
//! # Key Features
//!
//! - **Zero-copy extraction**: Uses `StringView` to avoid copying matched substrings from input buffers
//! - **LRU cache**: Compiles each unique pattern once per batch for column pattern workloads
//! - **PCRE2 fallback**: Optional support for advanced regex features (lookaround, backreferences)
//! - **Configurable thresholds**: Runtime tuning via environment variables for cache size, buffer limits, and block size
//!
//! # Behavior
//!
//! - `idx=0`: Returns full match
//! - `idx=1+`: Returns specified capture group (default: 1)
//! - No match or missing group: Returns empty string `""` (NOT NULL)
//! - NULL inputs: Propagated to NULL output
//! - Negative idx: Returns error
//!
//! # Supported Types
//!
//! - String types: `Utf8`, `LargeUtf8`, `Utf8View`
//! - Pattern types: `Utf8`, `LargeUtf8`, `Utf8View` (scalar or column)
//! - Index type: `Int32` (scalar or column)
//!
//! # Example
//!
//! ```no_run
//! use datafusion::prelude::*;
//! use df_regexp_extract::make_regexp_extract_udf;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//! ctx.register_udf(make_regexp_extract_udf());
//!
//! // Use in DataFrame API via expr_fn::scalar_udf
//! // df.select(vec![scalar_udf("regexp_extract", vec![col("text"), lit(r"(\d+)"), lit(1)])])?;
//! # Ok(())
//! # }
//! ```
//!
//! # Configuration
//!
//! Environment variables control runtime behavior:
//!
//! - `DF_REGEX_CACHE_SIZE`: LRU cache capacity for column patterns (default: 64)
//! - `DF_MAX_ZERO_COPY_BUFFER_MB`: Maximum buffer size for zero-copy extraction in MB (default: 128)
//! - `DF_LARGE_UTF8_BLOCK_SIZE_MB`: Block size for LargeUtf8 coalescing in MB (default: 2)
//! - `DF_PCRE2_MATCH_LIMIT`: PCRE2 match attempt limit (default: 1,000,000)
//! - `DF_PCRE2_DEPTH_LIMIT`: PCRE2 recursion depth limit (default: matches match limit)
//! - `DF_PCRE2_HEAP_LIMIT_KIB`: PCRE2 heap memory limit in KiB (default: 8192 = 8MB)

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
use std::sync::{Arc, OnceLock};

#[cfg(feature = "pcre2-fallback")]
use pcre2::bytes::{Regex as Pcre2Regex, RegexBuilder as Pcre2Builder};

// ============================================================================
// ERROR MESSAGE CONSTANTS
// ============================================================================

/// Error message for negative index values detected at runtime during batch processing
const ERR_IDX_NEGATIVE: &str = "idx must be >= 0";

/// Error message for negative index values detected as scalar literals during planning
const ERR_IDX_NEGATIVE_LITERAL: &str = "regexp_extract: idx must be >= 0 (negative literal)";

/// Error message for incorrect number of arguments
const ERR_INVALID_ARG_COUNT: &str = "regexp_extract expects two or three arguments";

/// Error message for incorrect Int32 scalar type
const ERR_EXPECTED_INT32_SCALAR: &str = "expected Int32 scalar for idx";

/// Error message for incorrect Int32 array type
const ERR_EXPECTED_INT32_ARRAY: &str = "expected Int32 array for idx";

// Cold error constructors to optimize branch prediction
#[cold]
#[inline(never)]
fn err_idx_negative() -> DataFusionError {
    DataFusionError::Execution(ERR_IDX_NEGATIVE.to_string())
}

#[cold]
#[inline(never)]
fn err_idx_negative_literal() -> DataFusionError {
    DataFusionError::Plan(ERR_IDX_NEGATIVE_LITERAL.to_string())
}

#[cold]
#[inline(never)]
fn err_invalid_arg_count() -> DataFusionError {
    DataFusionError::Execution(ERR_INVALID_ARG_COUNT.to_string())
}

#[cold]
#[inline(never)]
fn err_expected_int32_scalar() -> DataFusionError {
    DataFusionError::Execution(ERR_EXPECTED_INT32_SCALAR.to_string())
}

#[cold]
#[inline(never)]
fn err_expected_int32_array() -> DataFusionError {
    DataFusionError::Execution(ERR_EXPECTED_INT32_ARRAY.to_string())
}

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
// CONFIGURATION CONSTANTS (Cached)
// ============================================================================

/// Cached configuration values (initialized once, read many times)
static REGEX_CACHE_CAPACITY: OnceLock<usize> = OnceLock::new();
static MAX_ZERO_COPY_BUFFER_SIZE: OnceLock<usize> = OnceLock::new();
static LARGE_UTF8_BLOCK_SIZE: OnceLock<i64> = OnceLock::new();

/// LRU cache capacity for column pattern compilation.
/// Balances memory usage vs cache hit rate for workloads with varying pattern cardinality.
/// Override with `DF_REGEX_CACHE_SIZE` environment variable.
#[inline(always)]
fn regex_cache_capacity() -> usize {
    *REGEX_CACHE_CAPACITY.get_or_init(|| {
        std::env::var("DF_REGEX_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64) // Default: 64 unique patterns per batch
    })
}

/// Maximum buffer size for zero-copy StringView extraction.
/// Prevents RSS bloat from pinning huge buffers with low match utilization.
/// Override with `DF_MAX_ZERO_COPY_BUFFER_MB` environment variable (in megabytes).
#[inline(always)]
fn max_zero_copy_buffer_size() -> usize {
    *MAX_ZERO_COPY_BUFFER_SIZE.get_or_init(|| {
        std::env::var("DF_MAX_ZERO_COPY_BUFFER_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(128 * 1024 * 1024) // Default: 128MB
    })
}

/// Block size for LargeUtf8 buffer coalescing.
/// Trades off block count vs granularity for zero-copy extraction.
/// Override with `DF_LARGE_UTF8_BLOCK_SIZE_MB` environment variable (in megabytes).
#[inline(always)]
fn large_utf8_block_size() -> i64 {
    *LARGE_UTF8_BLOCK_SIZE.get_or_init(|| {
        std::env::var("DF_LARGE_UTF8_BLOCK_SIZE_MB")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(2 * 1024 * 1024) // Default: 2MB
    })
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
            lru: LruCache::new(
                NonZeroUsize::new(capacity).expect("Cache capacity must be non-zero"),
            ),
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
// BUILDER HELPERS
// ============================================================================

#[inline(always)]
fn append_view(
    builder: &mut StringViewBuilder,
    block_id: u32,
    offset: u32,
    len: u32,
) -> Result<()> {
    builder
        .try_append_view(block_id, offset, len)
        .map_err(|err| DataFusionError::Execution(format!("regexp_extract: {err}")))?;
    Ok(())
}

/// Find which block contains a string, using O(1) calculation instead of linear search.
/// Returns (block_id, offset_in_block) if string is fully contained in one block, None otherwise.
#[inline(always)]
fn find_block_for_string(
    blocks: &[(i64, i64, u32)],
    block_size: i64,
    row_start: i64,
    row_end: i64,
) -> Option<(u32, u32)> {
    // Calculate block index directly (O(1) instead of O(n))
    let block_idx = (row_start / block_size) as usize;

    if block_idx < blocks.len() {
        // SAFETY: We just checked block_idx < blocks.len() above
        let (block_start, block_end, block_id) = unsafe { *blocks.get_unchecked(block_idx) };
        // Verify string is fully contained in this block
        if row_start >= block_start && row_end <= block_end {
            let offset_in_block = (row_start - block_start) as u32;
            return Some((block_id, offset_in_block));
        }
    }

    None
}

/// Extract capture group using zero-copy StringView when possible.
/// Returns true if match was found and extracted, false if no match (caller should append empty string).
#[inline(always)]
fn extract_match_zero_copy(
    regex: &Regex,
    text: &str,
    idx_value: i32,
    row_offset: u32,
    builder: &mut StringViewBuilder,
    block_id: u32,
) -> Result<bool> {
    if idx_value == 0 {
        // Fast path: idx=0 returns full match using find()
        if let Some(m) = regex.find(text) {
            let offset = row_offset + m.start() as u32;
            let len = m.len() as u32;
            append_view(builder, block_id, offset, len)?;
            return Ok(true);
        }
    } else {
        // Normal path: extract capture group
        if let Some(caps) = regex.captures(text)
            && let Some(m) = caps.get(idx_value as usize)
        {
            let offset = row_offset + m.start() as u32;
            let len = m.len() as u32;
            append_view(builder, block_id, offset, len)?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Extract capture group from PCRE2 regex using zero-copy StringView when possible.
/// Returns true if match was found and extracted, false if no match (caller should append empty string).
#[cfg(feature = "pcre2-fallback")]
#[inline(always)]
fn extract_match_zero_copy_pcre2(
    regex: &Pcre2Regex,
    text: &str,
    idx_value: i32,
    row_offset: u32,
    builder: &mut StringViewBuilder,
    block_id: u32,
) -> Result<bool> {
    let bytes = text.as_bytes();

    if idx_value == 0 {
        // Fast path: idx=0 returns full match using find()
        match regex.find(bytes) {
            Ok(Some(m)) => {
                let offset = row_offset + m.start() as u32;
                let len = (m.end() - m.start()) as u32;
                append_view(builder, block_id, offset, len)?;
                return Ok(true);
            }
            _ => return Ok(false),
        }
    } else {
        // Normal path: extract capture group
        match regex.captures(bytes) {
            Ok(Some(caps)) => {
                if let Some(m) = caps.get(idx_value as usize) {
                    let offset = row_offset + m.start() as u32;
                    let len = (m.end() - m.start()) as u32;
                    append_view(builder, block_id, offset, len)?;
                    return Ok(true);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(false)
}

// ============================================================================
// PUBLIC API: UDF Factory
// ============================================================================

/// Creates a DataFusion ScalarUDF implementing Spark-compatible `regexp_extract`.
///
/// This function constructs a UDF that extracts substrings matching a regex pattern,
/// with support for scalar and column patterns, multiple string types, and optional
/// zero-copy extraction for optimal performance.
///
/// # Function Signature
///
/// ```text
/// regexp_extract(str: Utf8/LargeUtf8/Utf8View, pattern: Utf8/LargeUtf8/Utf8View, idx: Int32 = 1) -> Utf8View
/// ```
///
/// # Arguments
///
/// - `str`: Input string to search (scalar or column)
/// - `pattern`: Regex pattern to match (scalar or column)
/// - `idx`: Zero-based capture group index (scalar or column, optional, default: 1)
///   - `idx=0`: Returns full match
///   - `idx=1+`: Returns specified capture group
///
/// # Return Behavior
///
/// - **Match found**: Returns matched substring or capture group as `Utf8View`
/// - **No match**: Returns empty string `""` (NOT NULL) - Spark-compatible behavior
/// - **NULL input**: Returns NULL if any input is NULL
/// - **Invalid idx**: Returns error if `idx < 0`
/// - **Missing group**: Returns empty string `""` if capture group doesn't exist
///
/// # Performance Characteristics
///
/// ## Scalar Pattern (Fast Path)
/// - Compiles regex once, reuses for all rows
/// - Zero-copy extraction when possible:
///   - `Utf8`: Single buffer ≤128MB → zero-copy views
///   - `LargeUtf8`: 2MB blocks → zero-copy views for strings within blocks
///   - `Utf8View`: Copies extracted matches (input already uses views)
/// - Fallback: Copies matched substrings when zero-copy not possible
///
/// ## Column Pattern (Cache Path)
/// - Uses LRU cache (default: 64 patterns per batch)
/// - Each unique pattern compiled once per batch
/// - Same zero-copy optimizations as scalar pattern path
///
/// # Registration Example
///
/// ```no_run
/// use datafusion::prelude::*;
/// use df_regexp_extract::make_regexp_extract_udf;
///
/// # async fn example() -> datafusion::error::Result<()> {
/// let ctx = SessionContext::new();
///
/// // Register the UDF
/// ctx.register_udf(make_regexp_extract_udf());
///
/// // Use in DataFrame API
/// // let df = ctx.read_csv("data.csv", Default::default()).await?;
/// // let result = df.select(vec![
/// //     scalar_udf("regexp_extract", vec![col("text"), lit(r"user_(\d+)"), lit(1)])
/// // ])?;
/// # Ok(())
/// # }
/// ```
///
/// # Configuration
///
/// Tunable via environment variables:
/// - `DF_REGEX_CACHE_SIZE`: LRU cache capacity (default: 64)
/// - `DF_MAX_ZERO_COPY_BUFFER_MB`: Buffer size threshold (default: 128)
/// - `DF_LARGE_UTF8_BLOCK_SIZE_MB`: Block coalescing size (default: 2)
///
/// # Features
///
/// - **`pcre2-fallback`** (default): Enables PCRE2 support for advanced regex features
///   (lookaround, backreferences) when standard regex compilation fails
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
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8]),
        TypeSignature::Exact(vec![
            DataType::Utf8View,
            DataType::Utf8View,
            DataType::Int32,
        ]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Int32,
        ]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View, DataType::Int32]),
        TypeSignature::Exact(vec![
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Int32,
        ]),
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
            return Err(err_invalid_arg_count());
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
        return Err(err_idx_negative_literal());
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

        // Zero-copy path for Utf8 input (StringArray)
        if let ColumnString::Utf8(utf8_arr) = &string_input {
            let values_buffer = utf8_arr.values().clone();

            // Retention clamp: avoid pinning huge buffers with low match utilization
            // If buffer exceeds threshold, fall back to copying to prevent RSS bloat
            let max_buffer_size = max_zero_copy_buffer_size();

            if values_buffer.len() <= max_buffer_size {
                // Register input buffer for zero-copy view creation
                let block_id = builder.append_block(values_buffer);
                let offsets = utf8_arr.offsets();

                match compiled {
                    CompiledRegex::Fast(regex) => {
                        for i in 0..n {
                            if utf8_arr.is_null(i) || idx_input.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            let idx_value = idx_input.value_unchecked(i);
                            if idx_value < 0 {
                                return Err(err_idx_negative());
                            }

                            let s = utf8_arr.value(i);
                            let row_start = offsets[i] as u32;

                            if !extract_match_zero_copy(&regex, s, idx_value, row_start, &mut builder, block_id)? {
                                builder.append_value(""); // Empty on no match
                            }
                        }
                    }
                    #[cfg(feature = "pcre2-fallback")]
                    CompiledRegex::Pcre2(regex) => {
                        for i in 0..n {
                            if utf8_arr.is_null(i) || idx_input.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            let idx_value = idx_input.value_unchecked(i);
                            if idx_value < 0 {
                                return Err(err_idx_negative());
                            }

                            let s = utf8_arr.value(i);
                            let row_start = offsets[i] as u32;

                            if !extract_match_zero_copy_pcre2(&regex, s, idx_value, row_start, &mut builder, block_id)? {
                                builder.append_value(""); // Empty on no match
                            }
                        }
                    }
                }
                return Ok(Arc::new(builder.finish()));
            }
            // If buffer > 128MB, fall through to copying path below
        }

        // Zero-copy path for LargeUtf8 input with block coalescing
        if let ColumnString::LargeUtf8(large_utf8_arr) = &string_input {
            let block_size = large_utf8_block_size();
            let values_buffer = large_utf8_arr.values();
            let offsets = large_utf8_arr.offsets();

            // Calculate block boundaries and register blocks
            // slice() creates a zero-copy view from offset to end, so we track valid ranges
            let total_bytes = values_buffer.len() as i64;
            let mut blocks = Vec::new();
            let mut block_start = 0i64;

            while block_start < total_bytes {
                let block_end = (block_start + block_size).min(total_bytes);
                let block_slice = values_buffer.slice(block_start as usize);
                let block_id = builder.append_block(block_slice);
                blocks.push((block_start, block_end, block_id));
                block_start = block_end;
            }

            match &compiled {
                CompiledRegex::Fast(regex) => {
                    for i in 0..n {
                        if large_utf8_arr.is_null(i) || idx_input.is_null(i) {
                            builder.append_null();
                            continue;
                        }
                        let idx_value = idx_input.value_unchecked(i);
                        if idx_value < 0 {
                            return Err(err_idx_negative());
                        }

                        let s = large_utf8_arr.value(i);
                        let row_start = offsets[i];
                        let row_end = offsets[i + 1];

                        // Find which block this string is in (O(1) calculation)
                        if let Some((block_id, offset_in_block)) =
                            find_block_for_string(&blocks, block_size, row_start, row_end)
                        {
                            // Zero-copy path: string is in one block
                            if !extract_match_zero_copy(regex, s, idx_value, offset_in_block, &mut builder, block_id)? {
                                builder.append_value("");
                            }
                        } else {
                            // Fallback: string spans multiple blocks, copy it
                            let capture = compiled.get_capture(s, idx_value as usize);
                            builder.append_value(capture);
                        }
                    }
                }
                #[cfg(feature = "pcre2-fallback")]
                CompiledRegex::Pcre2(regex) => {
                    for i in 0..n {
                        if large_utf8_arr.is_null(i) || idx_input.is_null(i) {
                            builder.append_null();
                            continue;
                        }
                        let idx_value = idx_input.value_unchecked(i);
                        if idx_value < 0 {
                            return Err(err_idx_negative());
                        }

                        let s = large_utf8_arr.value(i);
                        let row_start = offsets[i];
                        let row_end = offsets[i + 1];

                        // Find which block this string is in (O(1) calculation)
                        if let Some((block_id, offset_in_block)) =
                            find_block_for_string(&blocks, block_size, row_start, row_end)
                        {
                            // Zero-copy path
                            if !extract_match_zero_copy_pcre2(regex, s, idx_value, offset_in_block, &mut builder, block_id)? {
                                builder.append_value("");
                            }
                        } else {
                            // Fallback: string spans blocks, copy it
                            let capture = compiled.get_capture(s, idx_value as usize);
                            builder.append_value(capture);
                        }
                    }
                }
            }
            return Ok(Arc::new(builder.finish()));
        }

        // Fallback: copying path for Utf8View and scalar inputs
        for i in 0..n {
            if string_input.is_null(i) || idx_input.is_null(i) {
                builder.append_null();
                continue;
            }
            let s = string_input.value_unchecked(i);
            let idx_value = idx_input.value_unchecked(i);
            if idx_value < 0 {
                return Err(err_idx_negative());
            }
            let capture = compiled.get_capture(s, idx_value as usize);
            builder.append_value(capture);
        }
        return Ok(Arc::new(builder.finish()));
    }

    // Column patterns: use an LRU cache to limit recompilation
    let mut cache = RegexCache::new(regex_cache_capacity());

    // Zero-copy path for Utf8 input (StringArray)
    if let ColumnString::Utf8(utf8_arr) = &string_input {
        let values_buffer = utf8_arr.values().clone();

        // Retention clamp: same threshold as scalar pattern path
        let max_buffer_size = max_zero_copy_buffer_size();

        if values_buffer.len() <= max_buffer_size {
            let block_id = builder.append_block(values_buffer);
            let offsets = utf8_arr.offsets();

            for i in 0..n {
                if utf8_arr.is_null(i) || pattern_input.is_null(i) || idx_input.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = utf8_arr.value(i);
                let pattern = pattern_input.value_unchecked(i);
                let idx_value = idx_input.value_unchecked(i);
                if idx_value < 0 {
                    return Err(DataFusionError::Execution("idx must be >= 0".to_string()));
                }

                let row_start = offsets[i] as u32;
                let regex = cache.get_or_insert(pattern)?;

                // Extract using appropriate engine
                let matched = match &regex {
                    CompiledRegex::Fast(r) => {
                        extract_match_zero_copy(r, s, idx_value, row_start, &mut builder, block_id)?
                    }
                    #[cfg(feature = "pcre2-fallback")]
                    CompiledRegex::Pcre2(r) => {
                        extract_match_zero_copy_pcre2(r, s, idx_value, row_start, &mut builder, block_id)?
                    }
                };

                if !matched {
                    builder.append_value("");
                }
            }
            return Ok(Arc::new(builder.finish()));
        }
        // If buffer > 128MB, fall through to copying path below
    }

    // Zero-copy path for LargeUtf8 input with column patterns
    if let ColumnString::LargeUtf8(large_utf8_arr) = &string_input {
        let block_size = large_utf8_block_size();
        let values_buffer = large_utf8_arr.values();
        let offsets = large_utf8_arr.offsets();

        // Calculate block boundaries and register blocks
        let total_bytes = values_buffer.len() as i64;
        let mut blocks = Vec::new();
        let mut block_start = 0i64;

        while block_start < total_bytes {
            let block_end = (block_start + block_size).min(total_bytes);
            let block_slice = values_buffer.slice(block_start as usize);
            let block_id = builder.append_block(block_slice);
            blocks.push((block_start, block_end, block_id));
            block_start = block_end;
        }

        for i in 0..n {
            if large_utf8_arr.is_null(i) || pattern_input.is_null(i) || idx_input.is_null(i) {
                builder.append_null();
                continue;
            }
            let s = large_utf8_arr.value(i);
            let pattern = pattern_input.value_unchecked(i);
            let idx_value = idx_input.value_unchecked(i);
            if idx_value < 0 {
                return Err(err_idx_negative());
            }

            let row_start = offsets[i];
            let row_end = offsets[i + 1];
            let regex = cache.get_or_insert(pattern)?;

            // Find which block this string is in (O(1) calculation)
            if let Some((block_id, offset_in_block)) =
                find_block_for_string(&blocks, block_size, row_start, row_end)
            {
                // Zero-copy path
                let matched = match &regex {
                    CompiledRegex::Fast(r) => {
                        extract_match_zero_copy(r, s, idx_value, offset_in_block, &mut builder, block_id)?
                    }
                    #[cfg(feature = "pcre2-fallback")]
                    CompiledRegex::Pcre2(r) => {
                        extract_match_zero_copy_pcre2(r, s, idx_value, offset_in_block, &mut builder, block_id)?
                    }
                };

                if !matched {
                    builder.append_value("");
                }
            } else {
                // Fallback: string spans blocks, copy it
                let capture = regex.get_capture(s, idx_value as usize);
                builder.append_value(capture);
            }
        }
        return Ok(Arc::new(builder.finish()));
    }

    // Fallback: copying path for Utf8View and scalar inputs
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
                        _ => Err(err_expected_int32_scalar()),
                    }
                }
            }
            ColumnarValue::Array(array) => {
                if let Some(ints) = array.as_any().downcast_ref::<Int32Array>() {
                    Ok(Self::Array(ints))
                } else {
                    Err(err_expected_int32_array())
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

/// Represents a string input that can be either a scalar value or an array column.
enum ColumnString<'a> {
    /// Scalar string value (may be null)
    Scalar(Option<String>),
    /// Reference to a Utf8 (StringArray) column
    Utf8(&'a StringArray),
    /// Reference to a LargeUtf8 column
    LargeUtf8(&'a LargeStringArray),
    /// Reference to a Utf8View column
    Utf8View(&'a StringViewArray),
}

impl<'a> ColumnString<'a> {
    fn new(arg: &'a ColumnarValue, label: &str) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    Ok(Self::Scalar(None))
                } else {
                    match value {
                        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => {
                            Ok(Self::Scalar(opt.clone()))
                        }
                        ScalarValue::Utf8View(opt) => Ok(Self::Scalar(opt.clone())),
                        _ => Err(DataFusionError::Execution(format!(
                            "expected Utf8/LargeUtf8/Utf8View scalar for {label}"
                        ))),
                    }
                }
            }
            ColumnarValue::Array(array) => {
                if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(Self::Utf8(utf8))
                } else if let Some(large) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(Self::LargeUtf8(large))
                } else if let Some(utf8_view) = array.as_any().downcast_ref::<StringViewArray>() {
                    Ok(Self::Utf8View(utf8_view))
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
        match self {
            Self::Scalar(_) => 1,
            Self::Utf8(arr) => arr.len(),
            Self::LargeUtf8(arr) => arr.len(),
            Self::Utf8View(arr) => arr.len(),
        }
    }

    #[inline(always)]
    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Scalar(opt) => opt.is_none(),
            Self::Utf8(arr) => arr.is_null(index),
            Self::LargeUtf8(arr) => arr.is_null(index),
            Self::Utf8View(arr) => arr.is_null(index),
        }
    }

    #[inline(always)]
    fn scalar_value(&self) -> Option<&str> {
        match self {
            Self::Scalar(Some(s)) => Some(s),
            _ => None,
        }
    }

    #[inline(always)]
    fn is_scalar_null(&self) -> bool {
        matches!(self, Self::Scalar(None))
    }

    #[inline(always)]
    fn value_unchecked(&self, index: usize) -> &str {
        match self {
            Self::Scalar(Some(value)) => value,
            Self::Utf8(arr) => arr.value(index),
            Self::LargeUtf8(arr) => arr.value(index),
            Self::Utf8View(arr) => arr.value(index),
            Self::Scalar(None) => unreachable!("null filtered via mask"),
        }
    }
}
