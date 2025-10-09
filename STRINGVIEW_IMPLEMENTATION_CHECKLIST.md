# StringView Zero-Copy Implementation Checklist

**Estimated Time**: 9-10 hours  
**Goal**: 20-200% performance improvement via zero-copy

## Phase 1: Foundation (2 hours)

### 1.1 Add Utf8View Signatures (30 min)
- [ ] Add Utf8View × Utf8View signatures
- [ ] Add cross-type combinations (Utf8View + Utf8/LargeUtf8)
- [ ] Change return type to `DataType::Utf8View`
- [ ] Update null array creation
- [ ] Test: Verify UDF accepts Utf8View inputs

### 1.2 Extend ColumnString for Utf8View (30 min)
- [ ] Add `View(&'a StringViewArray)` variant
- [ ] Update `new()` to downcast StringViewArray
- [ ] Update `len()`, `is_null()`, `value_unchecked()` for View
- [ ] Update `scalar_value()` for Utf8View scalars
- [ ] Test: Verify ColumnString handles all types

### 1.3 Replace StringBuilder with StringViewBuilder (1 hour)
- [ ] Change builder type in kernel
- [ ] Update all `builder.append_value()` calls
- [ ] Update `builder.append_null()` calls
- [ ] Update return to use Utf8View
- [ ] Test: Verify basic functionality (will copy for now, optimize next)

## Phase 2: Zero-Copy Paths (4 hours)

### 2.1 Zero-Copy for StringArray (Utf8) (1.5 hours)
- [ ] Register values buffer with `append_block()`
- [ ] Calculate match offset in original buffer
- [ ] Use `try_append_view()` instead of copying
- [ ] Handle small strings (≤12B auto-inline)
- [ ] Test: Verify zero-copy (buffer pointer comparison)

### 2.2 Zero-Copy for LargeStringArray + Coalescing (1.5 hours)
- [ ] Implement 2MB block coalescing (not per-row)
- [ ] Handle u32 overflow gracefully
- [ ] Track which chunk each row belongs to
- [ ] Use `try_append_view()` with coalesced blocks
- [ ] Test: Verify works with large offsets

### 2.3 Zero-Copy for StringViewArray (View→View) (1 hour)
- [ ] Register all input data buffers as blocks
- [ ] Map input buffer indices to output block IDs
- [ ] Handle inline strings (≤12B)
- [ ] Handle buffer-referenced strings (>12B)
- [ ] Adjust offsets for captures
- [ ] Test: Verify view→view zero-copy

## Phase 3: Critical Must-Haves (3 hours)

### 3.1 Retention Clamp (RSS Control) (1.5 hours)
- [ ] Define constants: `MAX_RETAIN_MB=128`, `MIN_FRACTION=0.15`
- [ ] Track block usage: `min_offset`, `max_offset`, `used_bytes`
- [ ] Calculate span and utilization per block
- [ ] Flatten sparse blocks (span>128MB, util<15%)
- [ ] Test: Verify RSS stays bounded with huge input / tiny matches

### 3.2 Update Both Fast and Slow Paths (1 hour)
- [ ] Scalar pattern path: Match enum outside loop (already done)
- [ ] Column pattern path: Use zero-copy in cache loop
- [ ] Ensure no per-row enum overhead
- [ ] Test: Verify no performance regression

### 3.3 Integration Testing (30 min)
- [ ] Test all input type combinations
- [ ] Test small/medium/long matches
- [ ] Test null handling
- [ ] Test semantics parity with new builder
- [ ] Test PCRE2 fallback still works

## Phase 4: Benchmarking & Validation (1 hour)

### 4.1 Performance Benchmarks (30 min)
- [ ] Run existing benchmarks with StringView
- [ ] Verify acceptance gates:
  - [ ] Short matches (≤12B): ≥20% faster
  - [ ] Medium matches (20-100B): ≥50% faster
  - [ ] Long matches (>100B): ≥100% faster
  - [ ] No regression >2% on other benchmarks

### 4.2 Memory Validation (30 min)
- [ ] Create huge input / tiny matches test
- [ ] Measure RSS without clamp (should be high)
- [ ] Measure RSS with clamp (should be bounded)
- [ ] Document results

## Phase 5: Code Polish (1 hour)

### 5.1 Clean Up Code (30 min)
- [ ] Remove unused StringBuilder imports
- [ ] Add inline comments for complex logic
- [ ] Ensure consistent formatting
- [ ] Run clippy and fix any warnings

### 5.2 Documentation (30 min)
- [ ] Update IMPLEMENTATION_SUMMARY with StringView results
- [ ] Update ARCHITECTURE with final state
- [ ] Add StringView examples
- [ ] Document when zero-copy kicks in

## Success Criteria

### Must Pass
- [ ] All 38 tests pass
- [ ] Returns StringViewArray
- [ ] Accepts StringViewArray input
- [ ] Zero-copy verified (buffer pointers)
- [ ] RSS bounded (retention clamp)
- [ ] Performance gates met

### Performance Gates
- [ ] Short (≤12B): +20% minimum
- [ ] Medium (20-100B): +50% minimum
- [ ] Long (>100B): +100% minimum
- [ ] Others: <2% regression

## Current Progress

- [x] Index-0 fast path implemented
- [x] Semantics parity tests added
- [ ] StringView signatures (starting now)

**Ready to implement**: Starting Phase 1.1
