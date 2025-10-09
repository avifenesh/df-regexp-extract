# Zero-Copy StringView Implementation Plan

**Goal**: Eliminate string copies by returning `Utf8View` (StringViewArray) instead of `Utf8`  
**Expected Impact**: 20-200% performance improvement on string-heavy workloads  
**Alignment**: Matches DataFusion's direction (Utf8View is default for VARCHAR)

## Why StringView?

### Current Problem (Copy-Heavy)

```rust
// Current: StringBuilder copies every match
for i in 0..10_000 {
    let capture = "matched substring";  // ~20 bytes
    builder.append_value(capture);      // ❌ Copies 20 bytes to builder's buffer
}
// Result: 10K × 20B = 200KB of copied data
```

### StringView Solution (Zero-Copy)

```rust
// StringView: Reference existing buffers
for i in 0..10_000 {
    let capture_start = 100;
    let capture_len = 20;
    builder.try_append_view(block_id, capture_start, capture_len)?;  // ✅ 16-byte view
}
// Result: 10K × 16B = 160KB of views (no data copied!)
```

### Benefits

| Benefit | Details |
|---------|---------|
| **Copy elimination** | No memcpy of match bytes, just store 16-byte views |
| **Memory traffic** | O(16 × rows) instead of O(total match bytes) |
| **Small string optimization** | Strings ≤12 bytes inline in the 16-byte view |
| **Composability** | Downstream ops (filter, take) shuffle views, not megabytes |
| **Cache efficiency** | Less data moved = fewer cache misses |

### Expected Performance

```
Scenario: 10K rows, avg 20-byte matches

Current (StringBuilder):
  Allocate: 200KB
  Copy: 200KB
  Cache lines: ~3,125 (64B/line)

With StringView:
  Allocate: 160KB (views only)
  Copy: 0KB (references existing buffers)
  Cache lines: ~2,500 (views) + input already cached

Speedup: 20-200% (DataFusion reports, depends on workload)
```

## Implementation Plan

### Phase 1: Update UDF Signature & Return Type

#### 1.1 Add Utf8View Signatures

```rust
pub fn make_regexp_extract_udf() -> ScalarUDF {
    let signatures = vec![
        // Existing Utf8/LargeUtf8 signatures
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8, DataType::Int32]),
        // ... existing 8 signatures ...
        
        // NEW: Utf8View signatures (zero-copy path)
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
        
        // Cross-type combinations (Utf8View + Utf8/LargeUtf8)
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View, DataType::Int32]),
        TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8, DataType::Int32]),
        // ... more combinations as needed ...
    ];

    let signature = Signature::one_of(signatures, Volatility::Immutable);
    let fun: ScalarFunctionImplementation =
        Arc::new(|args: &[ColumnarValue]| regexp_extract_udf_impl(args));

    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "regexp_extract",
        signature,
        DataType::Utf8View,  // ✅ Return Utf8View (zero-copy)
        fun,
    ))
}
```

#### 1.2 Import StringView Types

```rust
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, StringArray, 
    StringViewArray, StringViewBuilder,  // NEW
    new_null_array,
};
use datafusion::arrow::buffer::Buffer;  // NEW
```

### Phase 2: Replace StringBuilder with StringViewBuilder

#### 2.1 Core Builder Switch

```rust
fn regexp_extract_kernel(
    str_arg: &ColumnarValue,
    pat_arg: &ColumnarValue,
    idx_arg: &ColumnarValue,
) -> Result<ArrayRef> {
    // ... existing validation ...
    
    let n = string_input.len().max(pattern_input.len()).max(idx_input.len());
    
    if n == 0 {
        return Ok(new_null_array(&DataType::Utf8View, 0));  // Changed from Utf8
    }
    
    // NEW: StringViewBuilder instead of StringBuilder
    let mut builder = StringViewBuilder::with_capacity(n);
    
    // ... rest of kernel ...
}
```

#### 2.2 Zero-Copy Path for StringArray Input

```rust
match &string_input {
    ColumnString::Utf8(arr) => {
        // Register the values buffer ONCE
        let values_buffer = arr.values().clone();
        let block_id = builder.append_block(values_buffer);
        
        // Fast path: Scalar pattern
        if let Some(pattern) = pattern_input.scalar_value() {
            let compiled = CompiledRegex::new_with_fallback(pattern)?;
            match compiled {
                CompiledRegex::Fast(regex) => {
                    for i in 0..n {
                        if arr.is_null(i) || idx_input.is_null(i) {
                            builder.append_null();
                            continue;
                        }
                        
                        let value_start = arr.value_offsets()[i] as usize;
                        let value_end = arr.value_offsets()[i + 1] as usize;
                        let s = &values_buffer[value_start..value_end];
                        let s_str = std::str::from_utf8(s).unwrap();
                        
                        // Find match in original buffer
                        if let Some(caps) = regex.captures(s_str) {
                            if let Some(m) = caps.get(idx_value as usize) {
                                let match_start = m.start();
                                let match_len = m.len();
                                
                                // ✅ Zero-copy: append view to existing buffer
                                builder.try_append_view(
                                    block_id,
                                    (value_start + match_start) as u32,
                                    match_len as u32
                                )?;
                                continue;
                            }
                        }
                        // No match: append empty (inlined)
                        builder.append_value("");
                    }
                }
                // ... PCRE2 path similar ...
            }
        }
    }
    // ... other input types ...
}
```

#### 2.3 Handle LargeStringArray (i64 Offsets)

```rust
ColumnString::LargeUtf8(arr) => {
    // For LargeUtf8, offsets are i64; may need per-row blocks if offset > u32::MAX
    let values_buffer = arr.values().clone();
    
    for i in 0..n {
        let value_start = arr.value_offsets()[i] as usize;
        let value_len = (arr.value_offsets()[i + 1] - arr.value_offsets()[i]) as usize;
        
        // Check if offset fits in u32
        if value_start > u32::MAX as usize {
            // Fallback: create per-row block (still zero-copy, just more blocks)
            let slice = values_buffer.slice_with_length(value_start, value_len);
            let row_block = builder.append_block(slice);
            // ... capture logic with row_block, offset 0 ...
        } else {
            // Can use single block registered once
            // ... same as StringArray path ...
        }
    }
}
```

#### 2.4 Handle StringViewArray Input (View → View)

```rust
ColumnString::View(arr) => {
    // Register all input data buffers as builder blocks once
    let mut buffer_map: Vec<u32> = Vec::new();
    for buf in arr.data_buffers() {
        let block_id = builder.append_block(buf.clone());
        buffer_map.push(block_id);
    }
    
    for i in 0..n {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        
        let view_u128 = arr.views()[i];
        let view = arrow_array::ByteView::from(view_u128);
        
        if view.length <= 12 {
            // Short string: inlined in view, need to extract and re-match
            let s = arr.value(i);
            // ... match and append_value (will inline if result ≤12) ...
        } else {
            // Long string: reference buffer
            let block_id = buffer_map[view.buffer_index as usize];
            let value_offset = view.offset;
            
            // Get string for matching
            let s = arr.value(i);
            // ... match ...
            if let Some(caps) = regex.captures(s) {
                if let Some(m) = caps.get(idx_value as usize) {
                    let match_start = m.start();
                    let match_len = m.len();
                    
                    // ✅ Zero-copy: append view with adjusted offset
                    builder.try_append_view(
                        block_id,
                        value_offset + match_start as u32,
                        match_len as u32
                    )?;
                    continue;
                }
            }
            builder.append_value("");
        }
    }
}
```

### Phase 3: Extend Input Abstraction

#### 3.1 Add StringViewArray Support

```rust
enum ColumnString<'a> {
    Scalar(Option<String>),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    View(&'a StringViewArray),  // NEW
}

impl<'a> ColumnString<'a> {
    fn new(arg: &'a ColumnarValue, label: &str) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(value) => {
                // ... handle Utf8/LargeUtf8/Utf8View scalars ...
            }
            ColumnarValue::Array(array) => {
                if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(Self::Utf8(utf8))
                } else if let Some(large) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(Self::LargeUtf8(large))
                } else if let Some(view) = array.as_any().downcast_ref::<StringViewArray>() {
                    Ok(Self::View(view))  // NEW
                } else {
                    Err(DataFusionError::Execution(format!(
                        "expected Utf8/LargeUtf8/Utf8View array for {label}"
                    )))
                }
            }
        }
    }
    
    // Add helper to get buffer info for zero-copy
    fn buffer_info(&self, index: usize) -> BufferInfo {
        match self {
            Self::Utf8(arr) => BufferInfo::Utf8 {
                buffer: arr.values(),
                offset: arr.value_offsets()[index] as usize,
                len: (arr.value_offsets()[index + 1] - arr.value_offsets()[index]) as usize,
            },
            // ... similar for other types ...
        }
    }
}

struct BufferInfo<'a> {
    // Store enough info to create a view without copying
}
```

### Phase 4: Handle Edge Cases

#### 4.1 u32 Offset Overflow

```rust
// For LargeUtf8 with offset > u32::MAX
if value_start > u32::MAX as usize {
    // Per-row block fallback (still zero-copy)
    let slice = values_buffer.slice_with_length(value_start, value_len);
    let row_block = builder.append_block(slice);
    builder.try_append_view(row_block, match_start as u32, match_len as u32)?;
} else {
    // Use shared block
    builder.try_append_view(block_id, (value_start + match_start) as u32, match_len as u32)?;
}
```

#### 4.2 Small String Inlining

```rust
// Strings ≤12 bytes inline in the 16-byte view automatically
if match_len <= 12 {
    builder.append_value(capture);  // Will inline automatically
} else {
    builder.try_append_view(block_id, offset, len)?;  // Reference buffer
}
```

#### 4.3 No-Match Semantics

```rust
// Keep current behavior: empty string (not null)
if no_match {
    builder.append_value("");  // Empty string (inlined, 0 bytes)
} else {
    builder.append_null();     // For null propagation
}
```

## Critical Must-Haves (From External Review)

### 1. Retention Clamp (RSS Control) ⭐⭐⭐⭐⭐

**Problem**: Zero-copy can pin huge buffers for tiny matches

**Example**:
```
Input: 100MB buffer
Matches: 10 rows × 10 bytes = 100 bytes (0.0001% utilization)
Without clamp: Pin entire 100MB in memory ❌
With clamp: Detect low utilization → flatten → ~100KB ✅
```

**Implementation**:
```rust
// Constants
const MAX_RETAIN_MB: u64 = 128;
const MIN_FRACTION: f64 = 0.15;

// Track per block during building
struct BlockUsage {
    min_offset: u32,
    max_offset: u32,
    used_bytes: u64,
}

// After building all views
for (block_id, usage) in block_usage.iter().enumerate() {
    let span = usage.max_offset.saturating_sub(usage.min_offset) as u64;
    let span_mb = span / (1024 * 1024);
    let utilization = (usage.used_bytes as f64) / (span as f64);
    
    if span_mb > MAX_RETAIN_MB && utilization < MIN_FRACTION {
        // Flatten: copy rows using this block into Utf8 builder
        materialize_rows_for_block(block_id);
    }
}
```

**Priority**: P0 (Critical for production)

### 2. Semantics Parity Tests (Explicit)

**Required test coverage**:
```rust
#[test]
fn test_null_input_null_output() {
    // NULL in any arg → NULL result
}

#[test]
fn test_no_match_empty_string() {
    // Pattern doesn't match → ""
}

#[test]
fn test_missing_group_empty_string() {
    // idx=5 but only 2 groups → ""
}

#[test]
fn test_out_of_range_index_null() {
    // Verify current behavior (may be "" or NULL)
}
```

**Priority**: P0 (Correctness guarantee)

### 3. Index-0 Fast Path

**Optimization**: idx=0 (full match) is common, use `find()` instead of `captures()`

```rust
let capture = if idx_value == 0 {
    // Fast: find() avoids building capture groups
    regex.find(s).map(|m| m.as_str()).unwrap_or("")
} else {
    // Normal: need capture groups
    regex.captures(s)
        .and_then(|caps| caps.get(idx_value as usize).map(|m| m.as_str()))
        .unwrap_or("")
};
```

**Expected**: 10-30% faster for idx=0 (common case)  
**Priority**: P0 (Easy win)

### 4. LargeUtf8 Block Coalescing

**Problem**: Per-row `append_block()` creates 10K blocks  
**Solution**: Chunk into 2MB slices

```rust
const BLOCK_CHUNK_SIZE: usize = 2 * 1024 * 1024;  // 2MB

// For LargeUtf8: coalesce into chunks
let values = arr.values();
let mut chunks: Vec<(u32, Buffer)> = Vec::new();

for chunk_start in (0..values.len()).step_by(BLOCK_CHUNK_SIZE) {
    let chunk_len = BLOCK_CHUNK_SIZE.min(values.len() - chunk_start);
    let chunk = values.slice_with_length(chunk_start, chunk_len);
    let block_id = builder.append_block(chunk);
    chunks.push((block_id, chunk_start as u32));
}

// For each row, find which chunk it's in
let value_offset = arr.value_offsets()[i] as u32;
let chunk_idx = (value_offset as usize) / BLOCK_CHUNK_SIZE;
let (block_id, chunk_base) = chunks[chunk_idx];
let local_offset = value_offset - chunk_base;
```

**Priority**: P0 (Prevents O(rows) blocks)

### 5. PCRE2 Security Limits ✅

Already implemented with env-configurable defaults:
- `LIMIT_MATCH`: 1M attempts
- `LIMIT_DEPTH`: 1M recursion
- `LIMIT_HEAP`: 8MB

**Priority**: P0 (Done)

### 6. Adaptive LRU (Optional)

Proposed: `capacity = next_pow2(2 × sampled_distinct).clamp(64, 512)`

**Assessment**: 🟡 Nice-to-have, not critical  
**Decision**: Defer until benchmarks show fixed 64 is insufficient  
**Priority**: P2 (Conditional)

### 7. UTF-8 Unchecked (Optional)

Use `from_utf8_unchecked` in hot paths (debug assert only)

**Assessment**: 🟡 Micro-optimization  
**Decision**: Defer until profiling shows >1% impact  
**Priority**: P3 (Conditional)

## Implementation Steps (Revised)

### Step 1: Add Imports (5 minutes)

```rust
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, StringArray,
    StringViewArray, StringViewBuilder,  // NEW
    new_null_array,
};
use datafusion::arrow::buffer::Buffer;  // NEW
use datafusion::arrow::array::types::ByteView;  // NEW (for view parsing)
```

### Step 2: Update UDF Signature (15 minutes)

- Add Utf8View type signatures (8+ combinations)
- Change return type to `DataType::Utf8View`
- Update null array creation to use Utf8View

### Step 3: Extend ColumnString (30 minutes)

- Add `View(&'a StringViewArray)` variant
- Update `new()` to handle Utf8View scalars and arrays
- Add `buffer_info()` helper for zero-copy extraction

### Step 4: Implement StringViewBuilder Path (3 hours)

#### 4.1 Scalar Pattern Path

For each input type (Utf8, LargeUtf8, View):
- Register input buffers with `builder.append_block()`
- For each row, calculate match position in original buffer
- Append view (not copy) to result
- Handle edge cases (u32 overflow, inlining)

#### 4.2 Column Pattern Path

- Same as scalar, but per-row pattern compilation/caching
- Each row might use different engine (Fast vs PCRE2)

#### 4.3 Mixed Input Types

Handle all combinations:
- String input: Utf8/LargeUtf8/View
- Pattern input: Utf8/LargeUtf8/View/Scalar

### Step 5: Testing (1 hour)

```rust
#[tokio::test]
async fn test_utf8view_input_output() {
    // Create StringViewArray input
    let input = StringViewBuilder::new()
        .append_value("foo123bar")
        .finish();
    
    // Test regexp_extract returns Utf8View
    let result = ctx.sql("SELECT regexp_extract(col, '\\d+', 1)").await?;
    let array = result.collect().await?[0].column(0);
    
    assert!(array.as_any().is::<StringViewArray>());
    assert_eq!(array.value(0), "123");
}

#[tokio::test]
async fn test_zero_copy_verification() {
    // Verify no copying happens (via buffer pointer comparison)
    let input_buffer = Buffer::from("foo123bar".as_bytes());
    // ... test that result views point to same buffer ...
}

#[tokio::test]
async fn test_long_offset_fallback() {
    // Test u32 overflow handling for LargeUtf8
}

#[tokio::test]
async fn test_small_string_inlining() {
    // Test strings ≤12 bytes are inlined
}
```

### Step 6: Benchmark (30 minutes)

```rust
// benches/stringview_zero_copy.rs

fn bench_utf8_vs_utf8view(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_copy");
    
    // Test with Utf8 (current, copies)
    group.bench_function("utf8_builder", |b| {
        b.iter(|| {
            // regexp_extract with StringBuilder (copies)
        });
    });
    
    // Test with Utf8View (zero-copy)
    group.bench_function("utf8view_builder", |b| {
        b.iter(|| {
            // regexp_extract with StringViewBuilder (zero-copy)
        });
    });
}
```

Expected improvements:
- **Short matches** (≤12B): 30-50% faster (inlining)
- **Medium matches** (20-100B): 50-100% faster (zero-copy)
- **Long matches** (>100B): 100-200% faster (zero-copy)

## Expected Performance Impact

### Memory Allocation

```
Current (StringBuilder):
  10K rows × 20-byte avg match = 200KB allocated + copied

StringView:
  10K rows × 16-byte view = 160KB allocated
  + 0KB copied (references input buffers)
  
Memory savings: 20-80% depending on match length
```

### CPU Impact

```
Current:
  - Allocate output buffer
  - memcpy each match into buffer
  - Defrag/compact string data

StringView:
  - Write 16-byte views (or inline ≤12B)
  - No memcpy
  - No defragmentation

CPU savings: 20-200% (DataFusion benchmarks)
```

### Cache Efficiency

```
Current: Touch 200KB of output data + input data
StringView: Touch 160KB of views (input data already cached)

Cache line reduction: 20-40%
```

## Edge Cases & Fallbacks

### 1. u32 Offset Overflow (LargeUtf8)

```rust
if base_offset > u32::MAX as usize {
    // Rare: per-row block (still zero-copy)
    let slice = buffer.slice_with_length(base_offset, value_len);
    let row_block = builder.append_block(slice);
    builder.try_append_view(row_block, match_start as u32, match_len as u32)?;
}
```

**Frequency**: Rare (only with >4GB single string or very late in large buffer)  
**Impact**: Still zero-copy, just more blocks

### 2. Small String Inlining

```rust
if match_len <= 12 {
    // Automatically inlined by StringViewBuilder
    builder.append_value(capture);
}
```

**Benefit**: No buffer reference needed for small strings  
**Performance**: Even faster than zero-copy (pure inline)

### 3. Cross-Type Inputs

```rust
// String: Utf8View, Pattern: Utf8 (scalar)
// Result: Can still use zero-copy for string input
```

**Handling**: Extract string value normally, build zero-copy views

## Compatibility & Migration

### DataFusion Alignment

```
DataFusion 50.x: Utf8View is default SQL VARCHAR type
Our UDF: Returns Utf8View (zero-copy)
Pipeline: End-to-end Utf8View (no implicit casts)
```

**Benefit**: Entire query pipeline stays in view mode (composable zero-copy)

### Backward Compatibility

```sql
-- Old queries still work (auto-cast if needed)
SELECT regexp_extract(utf8_col, '\\d+', 1)  -- Returns Utf8View (transparent)

-- Can explicitly cast if needed
SELECT regexp_extract(col, pat, 1)::VARCHAR  -- Cast to Utf8 if downstream needs it
```

### Persistence

```rust
// For Parquet/IPC write (if needed)
let result = ctx.sql("SELECT regexp_extract(...)")
    .await?
    .with_column("extracted", cast(col("extracted").as_utf8()))?  // Cast at boundary
    .write_parquet("output.parquet")?;
```

**Note**: Parquet StringView support is maturing; may not need cast in future

## Success Criteria

### Must Have
1. ✅ All 31 tests still pass (18 Spark + 13 core)
2. ✅ Returns Utf8View arrays
3. ✅ Accepts Utf8View input
4. ✅ Zero-copy for StringArray input (verified via buffer pointers)
5. ✅ Small strings ≤12B inline correctly

### Performance Goals
1. **20-50% faster** for short matches (≤12B) - inlining
2. **50-100% faster** for medium matches (20-100B) - zero-copy
3. **100-200% faster** for long matches (>100B) - zero-copy  
4. **0% regression** on any existing benchmark

### If Goals Not Met
- < 20% improvement: Investigate (but keep if no regression)
- Any regression > 2%: Revert or fix

## Implementation Timeline (Revised with Must-Haves)

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| Update signatures | 15 min | Utf8View in/out |
| Extend ColumnString | 30 min | View variant |
| Implement Utf8 zero-copy | 2 hours | StringArray path |
| Implement LargeUtf8 + coalescing | 1.5 hours | 2MB chunks, not per-row |
| Implement View→View | 1 hour | StringViewArray path |
| **Retention clamp** | 1.5 hours | RSS control (>128MB, <15% → flatten) |
| **Semantics tests** | 30 min | 4 unit tests |
| **Index-0 fast path** | 30 min | find() optimization |
| Testing | 1 hour | All paths tested |
| Benchmarking | 30 min | Performance validated |
| **Total** | **~9-10 hours** | Production-ready with guardrails |

**Added**: +3 hours for must-have guardrails (retention clamp, semantics, index-0, coalescing)

## Risks & Mitigation

### Risk 1: Complexity

**Concern**: Supporting 3 input types × 2 paths = 6 code paths  
**Mitigation**: Share logic via helpers, keep zero-copy isolated

### Risk 2: Buffer Lifetime

**Concern**: Views reference input buffers - lifetime correctness  
**Mitigation**: StringViewBuilder handles this; buffers are Arc<Buffer> (ref-counted)

### Risk 3: u32 Overflow

**Concern**: Large offsets don't fit in u32  
**Mitigation**: Per-row block fallback (still zero-copy)

## Next Steps After Implementation

1. Update README with Utf8View feature
2. Add examples showing zero-copy benefits
3. Benchmark against current implementation
4. Consider: Make Utf8View default, Utf8 as fallback
5. Document when zero-copy kicks in vs copying

## References

- Arrow StringView Spec: https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
- DataFusion StringView: https://github.com/apache/datafusion/issues/8865
- StringViewBuilder API: https://docs.rs/arrow/latest/arrow/array/struct.StringViewBuilder.html
- ByteView Layout: length (u32) + prefix (4B) + buffer_index (u32) + offset (u32)
