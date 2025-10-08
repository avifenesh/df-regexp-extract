# Knowledge Hub: DataFusion & Spark-Compatible UDFs

> **Purpose:** Comprehensive interview preparation for a Performance Specialist / Staff Rust Engineer role at a company building 2-3× Spark acceleration solutions on Apache Arrow and DataFusion.

## 📖 About This Guide

Each document follows a **dual-layer methodology**:

1. 🧸 **ELI5 (Explain Like I'm 5)** - Intuitive explanations using everyday analogies
2. 🧠 **Senior Systems Engineer** - Technical depth with code examples, performance analysis, memory layouts, and optimization strategies

This approach ensures both **conceptual understanding** (the "why") and **implementation mastery** (the "how").

---
## 🗺️ Reading Path

### Foundation Layer (Start Here)
Read these first to build core understanding:

1. **[ARROW.md](./ARROW.md)** - Columnar memory format fundamentals
   - Memory layout, zero-copy sharing, SIMD enablement
   - Why Arrow is 2-3× faster than row-based formats

2. **[DATAFUSION.md](./DATAFUSION.md)** - Query engine architecture
   - Vectorized execution model, batch processing
   - Why DataFusion outperforms Spark 2-3× on single-node workloads

3. **[STRING_ARRAY.md](./STRING_ARRAY.md)** - Arrow's string storage
   - i32 offset layout, null bitmaps, O(1) access
   - 27% less memory, 2-3× faster than Vec<String>

### UDF Implementation Layer
Core concepts for writing high-performance UDFs:

4. **[UDF.md](./UDF.md)** - User-Defined Functions overview
   - Vectorization requirements (8192 rows/batch)
   - Types: Scalar, Aggregate, Window, Table
   - 10-100× speedup vs Spark Python UDFs

5. **[COLUMNAR_VALUE.md](./COLUMNAR_VALUE.md)** - Scalar vs Array abstraction
   - The four combinations: S×S, S×A, A×S, A×A
   - Constant folding, broadcasting setup
   - Foundation for optimization

6. **[BROADCASTING.md](./BROADCASTING.md)** - Scalar replication optimization
   - Precompute expensive operations for scalar args
   - 1000× speedup for regex patterns (1 compilation vs 1M)
   - Memory: 16 bytes vs 48 MB for 1M rows

7. **[NULL_PROPAGATION.md](./NULL_PROPAGATION.md)** - SQL three-valued logic
   - NULL input → NULL output semantics
   - Early null checks save 10-30% on real data
   - Critical: NULL ≠ empty string

### Advanced Performance Layer
Deep optimization techniques:

8. **[KERNEL_FUNCTIONS.md](./KERNEL_FUNCTIONS.md)** - Low-level primitives
   - Type-specific, zero abstraction overhead
   - 5-15% faster than UDF wrappers
   - SIMD-friendly patterns

9. **[SCALAR_UDF.md](./SCALAR_UDF.md)** - UDF metadata and registration
   - Signature, volatility, type system integration
   - Query planner optimization opportunities
   - Immutable vs Stable vs Volatile

### Compatibility & Edge Cases
Ensuring Spark compatibility:

10. **[LARGEUTF8_VS_UTF8.md](./LARGEUTF8_VS_UTF8.md)** - String type handling
    - i32 vs i64 offsets (2GB vs 8 exabytes)
    - Type conversion strategies
    - 99% use Utf8, 1% need LargeUtf8

11. **[SPARK_REGEXP_EXTRACT.md](./SPARK_REGEXP_EXTRACT.md)** - Spark semantics
    - Group 0 = full match
    - No match → "" (not NULL)
    - Exact compatibility requirements

12. **[SIMD-ARCH.md](./SIMD-ARCH.md)** - SIMD vectorization
    - AVX2/AVX-512 instructions
    - 8-16 values processed per instruction
    - Arrow compute kernels leverage SIMD

---

## 🎯 Core Concepts Quick Reference

| Concept | ELI5 Analogy | Technical Essence | Performance Impact |
|---------|--------------|-------------------|-------------------|
| **Arrow** | LEGO trays sorted by color | Columnar memory format | 2-3× faster access |
| **DataFusion** | Super-fast robot helper | Rust query engine on Arrow | 2-3× faster than Spark |
| **StringArray** | Name cards on a tape | i32 offsets + contiguous UTF-8 | 27% less memory |
| **UDF** | Teaching robot a new trick | Custom vectorized function | 10-100× vs Python |
| **ColumnarValue** | One toy vs toy box | Scalar or Array enum | Enables broadcasting |
| **Broadcasting** | Magic copy machine | Scalar reused without duplication | 1000× for patterns |
| **Null Propagation** | Missing ingredient rule | NULL in → NULL out | 10-30% savings |
| **Kernel** | Simple fast stamp | Type-specific primitive | 5-15% faster |
| **ScalarUDF** | Recipe card for robot | Metadata wrapper | Enables optimization |
| **Utf8 vs LargeUtf8** | Regular vs giant toy box | i32 vs i64 offsets | 4 vs 8 bytes/string |

---

## 🚀 Performance Hierarchy

Understand where your code sits on the performance spectrum:

```
┌─────────────────────────────────────────────────────────────┐
│ Throughput (MB/s) for regexp_extract on 1M strings          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ Spark Python UDF:         10-50    █                        │
│ Spark Scala UDF:          100-200  █████                    │
│ DataFusion (naive):       200      ██████                   │
│ DataFusion (optimized):   400      ████████████             │
│ Arrow Compute Kernel:     3000+    ██████████████████████   │
│                                                              │
└─────────────────────────────────────────────────────────────┘

Key Optimizations:
├─ Vectorization:      Process 8192 rows at once
├─ Broadcasting:       Precompile regex once for scalar patterns
├─ Early Null Checks:  Skip expensive ops for NULLs
├─ Kernel Functions:   Eliminate abstraction overhead
└─ Arrow Compute:      Use built-in SIMD kernels
```

---

## 💡 Interview Question Bank

### Q1: "Why is DataFusion 2-3× faster than Spark?"

**Answer Framework:**
1. **No JVM/GC:** Rust zero-cost abstractions eliminate garbage collection pauses
2. **Vectorization:** Processes 8192 rows/batch vs Spark's row-at-a-time Python UDFs
3. **Zero Serialization:** Native Arrow format (no Pickle, no Java serialization overhead)
4. **Cache Efficiency:** Columnar layout optimizes L2/L3 cache utilization
5. **Instant Startup:** Native binary vs JIT warmup (~10s)

**Key Metrics:**
- Memory: 50% less (zero-copy Arrow vs JVM heap)
- Throughput: 2-3× higher (measured on real workloads)
- Latency: Sub-second for short queries (no warmup)

---

### Q2: "How would you optimize a slow UDF?"

**Systematic Approach:**
1. **Profile First:** Use `perf` or flamegraph to identify hot paths
2. **Check Broadcasting:** Ensure scalar args precomputed outside loop
3. **Early Null Checks:** Bitmap check before expensive operations
4. **Extract Kernel:** Move hot computation to type-specific kernel function
5. **Leverage Arrow:** Replace custom logic with compute kernels if available
6. **Verify Vectorization:** Confirm processing batches, not individual rows

**Example Optimization:**
```rust
// Before: 200 MB/s
for i in 0..len {
    let pattern = get_pattern(args, i);
    let regex = Regex::new(pattern)?;  // Compile per row!
    results.push(regex.is_match(strings[i]));
}

// After: 400 MB/s
let precompiled = match &args[1] {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(p))) => {
        Some(Regex::new(p)?)  // Compile ONCE
    },
    _ => None,
};

for i in 0..len {
    let regex = precompiled.as_ref().unwrap_or_else(|| /* per-row */);
    results.push(regex.is_match(strings[i]));
}
// Speedup: 2× (eliminated 1M compilations)
```

---

### Q3: "Explain ColumnarValue and why it matters"

**Concise Answer:**
ColumnarValue is an enum representing either `Scalar` (constant) or `Array` (column). It enables:

1. **Broadcasting:** Scalar values reused without duplication (16B vs 48MB for 1M rows)
2. **Constant Folding:** All-scalar operations computed at plan time (zero runtime cost)
3. **Performance:** Precompiling regex once for scalar patterns = 1000× speedup

**Must handle 4 combinations:** Scalar×Scalar, Scalar×Array, Array×Scalar, Array×Array

**Implementation Pattern:**
```rust
// Determine batch size
let len = args.iter()
    .filter_map(|arg| match arg {
        ColumnarValue::Array(a) => Some(a.len()),
        _ => None,
    })
    .max()
    .unwrap_or(1);

// Constant folding: all scalars → return scalar
if len == 1 && all_scalars {
    return Ok(ColumnarValue::Scalar(compute_once(args)?));
}

// Otherwise: broadcast and build array
```

---

### Q4: "What's the difference between a kernel function and a UDF?"

**Quick Comparison:**

| Aspect | Kernel Function | UDF Wrapper |
|--------|----------------|-------------|
| **Input** | `&StringArray` (concrete) | `&[ColumnarValue]` (abstract) |
| **Handles** | Pure computation | Type checking, broadcasting, nulls |
| **Performance** | Optimal (0% overhead) | +5-10% overhead |
| **Complexity** | Simple (30 LOC) | Complex (50 LOC) |
| **Reusability** | Low (type-specific) | High (all combinations) |

**Best Practice:** Write both!
- **Kernel:** Hot path performance
- **UDF:** Usability and orchestration

**Code Pattern:**
```rust
// Kernel: Direct array processing
pub fn kernel(arr: &StringArray, param: &T) -> Result<ArrayRef> { /* ... */ }

// UDF: Wrapper with broadcasting
pub fn make_udf() -> ScalarUDF {
    create_udf(/* ... */, Arc::new(|args| {
        let arr = extract_array(args)?;
        let param = precompute_if_scalar(args)?;
        kernel(&arr, &param)
    }))
}
```

---

### Q5: "How does null propagation work and why optimize it?"

**SQL Three-Valued Logic:**
- NULL input → NULL output (with exceptions: COALESCE, IS NULL)

**Performance Optimization:**
```rust
// ❌ BAD: Expensive operation even for NULLs
for i in 0..len {
    let s = if arr.is_null(i) { "" } else { arr.value(i) };
    let result = expensive_regex(s);  // Wasted work!
    builder.append_value(result);
}

// ✅ GOOD: Early null check
for i in 0..len {
    if arr.is_null(i) {  // Bitmap check: 0.1 ns
        builder.append_null();
        continue;  // Skip expensive work
    }
    let s = arr.value(i);  // Only if non-null
    let result = expensive_regex(s);  // 100 ns
    builder.append_value(result);
}
```

**Impact:** 10-30% speedup on datasets with 10-30% NULLs

**Critical Distinction:**
- **NULL:** Missing/unknown value (propagates)
- **Empty string:** Known zero-length value (does NOT propagate)

Example: `regexp_extract("no-match", pattern, 1)` → `""` (not NULL)

---

### Q6: "Design a Spark-compatible UDF for regexp_extract"

**Requirements:**
1. ✅ Group 0 = full match
2. ✅ No match → `""` (not NULL)
3. ✅ Out-of-range group → `""` (not NULL)
4. ✅ NULL input → NULL output
5. ✅ Negative idx → error

**Architecture:**
```
make_regexp_extract_udf()  ← ScalarUDF wrapper
    ├─ Handle ColumnarValue (Scalar vs Array)
    ├─ Precompile scalar regex (broadcasting)
    ├─ Type conversion (LargeUtf8 → Utf8)
    ├─ Null propagation
    └─ Call kernel ↓

eval_regexp_extract_utf8()  ← Kernel function
    ├─ Direct &StringArray processing
    ├─ Spark-compatible semantics
    └─ Tight loop, minimal branching
```

**Key Optimizations:**
```rust
// 1. Precompile scalar pattern (1000× speedup)
let scalar_pattern = match &args[1] {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(p))) => {
        Some(Regex::new(p)?)
    },
    _ => None,
};

// 2. Spark semantics: no match → ""
if let Some(caps) = regex.captures(s) {
    builder.append_value(caps.get(idx as usize).map_or("", |m| m.as_str()));
} else {
    builder.append_value("");  // NOT NULL
}
```

---

## 🛠️ Development Workflow

### Running Examples
```bash
# Demo showing kernel function usage
cargo run --bin demo

# Run all tests
cargo test

# Run specific test
cargo test kernel_happy_path

# Quiet output
cargo test -q
```

### Benchmarking
```bash
# Run Criterion benchmarks
cargo bench

# Flamegraph profiling
cargo flamegraph --bin demo
cargo flamegraph --bench regexp_extract -- --sample-size 15

# Note: Requires perf installed and kernel permissions
```

### Common Patterns

**Pattern 1: Scalar Precomputation**
```rust
let precomputed = match &args[N] {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(val))) => {
        Some(expensive_setup(val)?)
    }
    _ => None,
};

for i in 0..len {
    let setup = precomputed.as_ref().unwrap_or_else(|| {
        &expensive_setup(get_value_at(&args[N], i)?)
    });
    // Use setup
}
```

**Pattern 2: Early Null Check**
```rust
for i in 0..len {
    if arr.is_null(i) {
        builder.append_null();
        continue;
    }
    let s = arr.value(i);
    builder.append_value(process(s));
}
```

**Pattern 3: Kernel + UDF**
```rust
// Kernel: Pure computation
pub fn kernel(arr: &StringArray, param: &T) -> Result<ArrayRef> { /* ... */ }

// UDF: Orchestration
pub fn make_udf() -> ScalarUDF {
    create_udf(/* ... */, Arc::new(|args| {
        let arr = extract_and_convert(args)?;
        let param = precompute_if_scalar(args)?;
        kernel(&arr, &param)
    }))
}
```

---

## 🎓 Additional Resources

### Official Documentation
- **DataFusion:** https://datafusion.apache.org/
- **Arrow Format:** https://arrow.apache.org/docs/format/Columnar.html
- **Arrow Rust:** https://docs.rs/arrow/latest/arrow/
- **Spark SQL Functions:** https://spark.apache.org/docs/latest/api/sql/

### Community
- **DataFusion Discord:** https://discord.gg/wHb8dTjrWU
- **Arrow Mailing List:** dev@arrow.apache.org
- **GitHub Examples:** https://github.com/apache/datafusion/tree/main/datafusion-examples

---

## ✅ Interview Preparation Checklist

### Core Concepts (Must Know)
- [ ] Explain Arrow's columnar format and zero-copy benefits
- [ ] Describe DataFusion's vectorized execution model (batch size: 8192)
- [ ] Define UDF types: Scalar, Aggregate, Window, Table
- [ ] Explain ColumnarValue and broadcasting optimization
- [ ] Describe null propagation and three-valued logic

### Performance Analysis (Must Demonstrate)
- [ ] Profile a UDF using perf/flamegraph
- [ ] Identify and fix scalar pattern inefficiency (broadcasting)
- [ ] Implement early null checks for expensive operations
- [ ] Write kernel + UDF wrapper pattern
- [ ] Use Arrow compute kernels where applicable

### Spark Compatibility (Must Understand)
- [ ] Explain Spark's regexp_extract exact semantics
- [ ] Distinguish NULL vs empty string correctly
- [ ] Handle all edge cases: group 0, out-of-range, negative idx
- [ ] Achieve 2-3× performance improvement over Spark

### System Design (Nice to Have)
- [ ] Design a Spark-compatible function library architecture
- [ ] Explain distributed execution strategy (Ballista)
- [ ] Memory management and resource limits
- [ ] Error handling, logging, and debugging approach

---

## 📊 Performance Benchmarks (Reference)

**Workload:** 1M strings, pattern `"(\w+)-(\d+)"`, extract group 1

| Implementation | Throughput | Memory | Regex Compilations | Speedup |
|----------------|------------|--------|---------------------|---------|
| Spark Python UDF | 10 MB/s | 300 MB | 1M | 1× (baseline) |
| Spark Scala UDF | 150 MB/s | 200 MB | 1M | 15× |
| DataFusion (scalar pattern) | 400 MB/s | 50 MB | 1 | **40×** |
| DataFusion (array patterns) | 80 MB/s | 100 MB | 1M | 8× |

**Key Insight:** Broadcasting (scalar pattern) provides 5× speedup over array patterns.

---

## 🧩 Quick Reference Card

### Performance Optimization Checklist
- [ ] **Vectorized:** Process batches (8192 rows), not individual rows
- [ ] **Preallocated:** Use `StringBuilder::with_capacity(len)`
- [ ] **Broadcasted:** Precompute scalar operations outside loop
- [ ] **Null-aware:** Check `is_null()` before expensive operations
- [ ] **Kernel-based:** Extract hot paths to type-specific kernels
- [ ] **Arrow-native:** Leverage compute kernels for common operations

### Common Pitfalls to Avoid
- ❌ Building array for single scalar (missing constant folding)
- ❌ Accessing value before null check (bitmap first!)
- ❌ Per-row regex compilation for scalar patterns (precompute!)
- ❌ Confusing NULL with empty string (semantics matter)
- ❌ Missing scalar NULL fast path (entire output NULL)
- ❌ Forgetting LargeUtf8 handling (type conversion needed)

### Performance Gains Summary
- **Vectorization:** 10-100× vs row-at-a-time
- **Broadcasting:** 1000× for scalar patterns
- **Early null checks:** 10-30% on real data
- **Kernel functions:** 5-15% vs UDF wrapper
- **Arrow compute:** 5-10× vs manual loops

---

## 🎯 Success Criteria

By the end of studying this knowledge hub, you should be able to:

1. ✅ **Explain** Arrow's columnar format and why it enables 2-3× speedups
2. ✅ **Implement** a Spark-compatible UDF with correct semantics
3. ✅ **Optimize** UDFs using broadcasting, early null checks, and kernels
4. ✅ **Profile** performance bottlenecks using perf and flamegraphs
5. ✅ **Achieve** 2-3× Spark performance on realistic workloads

**Interview Goal:** Demonstrate both **deep technical knowledge** (memory layouts, SIMD, cache efficiency) and **practical implementation skills** (vectorization, broadcasting, optimization).

---

## 📝 Final Notes

This knowledge hub represents a **bottom-up learning path** from basic analogies to systems-level implementation. The dual-layer methodology ensures:

- **Conceptual clarity** through ELI5 explanations
- **Technical depth** through senior engineer deep-dives
- **Practical application** through code examples from this codebase
- **Interview readiness** through Q&A and performance analysis

Companies building Spark accelerators need engineers who understand both the **what** (Arrow format, DataFusion API) and the **why** (performance characteristics, optimization opportunities).

**Good luck with your interview! 🚀**

---

*This knowledge hub is part of the `df-regexp-extract` project, demonstrating production-quality Spark-compatible UDF implementation on DataFusion and Arrow.*
