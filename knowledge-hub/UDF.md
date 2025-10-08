## UDF (User-Defined Function)

🧸 UDF explained like you're 6 years old

Remember that super-smart robot (DataFusion) that helps you with your toys? A UDF is like teaching the robot a NEW trick!

The robot already knows basic tricks like:
- Counting your toys
- Sorting them by color
- Finding toys by name

But what if you want it to do something special that it doesn't know yet? Like "take a toy's name and tell me the first letter!"

That's when you create a UDF — you're literally teaching the robot a new trick!

**Here's how it works:**
1. You write down the instructions for the new trick (that's your code)
2. You give it a name, like "get_first_letter"
3. You tell the robot what kind of toys it works on (strings? numbers? colors?)
4. The robot learns it and remembers forever!

**The cool part:** Once the robot learns your trick, it can do it for ALL your toys at once, not one at a time!

If you have 1,000 toys and you want the first letter of each name, the robot doesn't go:
- "Toy 1: B"
- "Toy 2: C"
- "Toy 3: D"
...one by one.

Instead, it grabs ALL 1,000 toys at once and says "DONE!" in the time it used to take for 10 toys!

That's called **vectorization** — doing lots of things at once instead of one at a time.

⸻

🧠 UDF explained like you're a senior systems engineer

A User-Defined Function (UDF) is a mechanism to extend a query engine's built-in function library with custom, user-provided operations while maintaining the engine's performance characteristics (vectorization, optimization integration, parallelization).

### 1️⃣ Types of UDFs in DataFusion

**Scalar UDF (SUDF):**
- Input: N columns, M rows
- Output: 1 column, M rows (same row count)
- Operates row-wise logically, but must be implemented vectorized
- Examples: `regexp_extract`, `custom_hash`, `decode_json`

**Aggregate UDF (UDAF):**
- Input: N columns, M rows per group
- Output: 1 value per group
- Examples: `custom_percentile`, `geometric_mean`, `array_agg`

**Window UDF (UDWF):**
- Input: N columns, M rows per partition
- Output: 1 column, M rows (with window context)
- Examples: `custom_rank`, `moving_average`, `lag_with_interpolation`

**Table UDF (UDTF):**
- Input: Table (or scalar parameters)
- Output: Table (different schema/row count)
- Examples: `explode`, `range`, `generate_series`

This codebase implements a **Scalar UDF**.

⸻

### 2️⃣ Scalar UDF Anatomy in DataFusion

**Core Components:**
```rust
pub struct ScalarUDF {
    name: String,                          // Function identifier
    signature: Signature,                  // Type signature + volatility
    return_type: Arc<dyn ReturnTypeFunction>, // Output type (can depend on input types)
    fun: ScalarFunctionImplementation,     // Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue>>
}
```

**Creation (DataFusion 50.x API):**
```rust
use datafusion::logical_expr::create_udf;
use datafusion::arrow::datatypes::DataType;

let udf = create_udf(
    "function_name",                    // Name for SQL/catalog lookup
    vec![DataType::Utf8, DataType::Int32], // Input types
    DataType::Utf8,                     // Output type
    Volatility::Immutable,              // Optimization hint
    Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        // Vectorized implementation
        todo!("implement vectorized logic")
    }),
);

ctx.register_udf(udf);
```

⸻

### 3️⃣ Volatility and Query Optimization

**Volatility Levels:**

| Volatility | Meaning | Optimization Impact | Examples |
|------------|---------|---------------------|----------|
| **Immutable** | Pure function (f(x) always = same) | Constant folding, CSE, caching | `abs`, `upper`, `regexp_extract` |
| **Stable** | Same within query, differs between queries | CSE within query, no cross-query cache | `now()`, `current_user()` |
| **Volatile** | Different every call | No optimization | `random()`, `uuid()` |

**Optimization Example:**
```sql
-- Query with Immutable UDF
SELECT my_udf(column, 'constant_pattern') FROM table;

-- Optimizer behavior:
-- 1. Recognizes 'constant_pattern' is literal
-- 2. If my_udf is Immutable, can precompute parts (e.g., compile regex once)
-- 3. Reuse result if my_udf appears multiple times with same args
```

⸻

### 4️⃣ Vectorization: The Performance Key

**❌ WRONG: Row-at-a-Time Implementation**
```rust
// This is how Python Spark UDFs work (slow!)
let udf = create_udf(
    "bad_extract",
    vec![DataType::Utf8],
    DataType::Utf8,
    Volatility::Immutable,
    Arc::new(|args| {
        let arr = args[0].as_array();
        let mut results = vec![];

        // Process one row at a time
        for i in 0..arr.len() {
            let val = get_string_at(arr, i);
            results.push(process_single_value(val)); // ❌ Slow!
        }

        Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
    }),
);

// Performance: ~50 MB/s (limited by per-row overhead)
```

**✅ CORRECT: Vectorized Implementation**
```rust
let udf = create_udf(
    "good_extract",
    vec![DataType::Utf8],
    DataType::Utf8,
    Volatility::Immutable,
    Arc::new(|args| {
        let arr = args[0].as_array().as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Preallocate output builder
        let mut builder = StringBuilder::with_capacity(arr.len());

        // Tight loop: CPU can optimize (SIMD, prefetch, branch prediction)
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(process_value(arr.value(i)));
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }),
);

// Performance: ~400 MB/s (10x faster!)
```

**Why Vectorization Matters:**
1. **Amortized Allocation:** Single builder for all rows vs. per-row allocation
2. **CPU Pipeline:** Tight loop allows branch prediction and instruction pipelining
3. **Cache Locality:** Sequential array access (not scattered heap objects)
4. **SIMD Potential:** Compiler can auto-vectorize simple operations

⸻

### 5️⃣ ColumnarValue Handling

UDFs receive arguments as `&[ColumnarValue]`, which can be:
- `Scalar(ScalarValue)`: Single value broadcast to all rows
- `Array(ArrayRef)`: Column of values

**Must Handle All Combinations:**
```rust
fn my_udf(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Determine batch size (broadcasting rules)
    let len = args.iter()
        .filter_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .next()
        .unwrap_or(1);

    // If all scalars, return scalar (constant folding)
    if len == 1 {
        let result = compute_scalar(args)?;
        return Ok(ColumnarValue::Scalar(result));
    }

    // Build array with broadcasting
    let mut builder = StringBuilder::new();
    for i in 0..len {
        let arg0 = get_value_at(&args[0], i); // Broadcasts scalar if needed
        let arg1 = get_value_at(&args[1], i);
        builder.append_value(compute(arg0, arg1));
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}
```

**Performance Optimization: Scalar Precomputation**
```rust
// CRITICAL: Precompute expensive operations for scalar arguments
let precompiled_regex: Option<Regex> = match &args[1] {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) => {
        Some(Regex::new(pattern)?) // Compile ONCE
    }
    _ => None,
};

for i in 0..len {
    let text = get_string_at(&args[0], i)?;

    let regex = if let Some(re) = &precompiled_regex {
        re // Zero-cost reuse
    } else {
        // Compile per-row (if pattern is an array column)
        &Regex::new(get_string_at(&args[1], i)?)?
    };

    builder.append_value(regex.find(text).map_or("", |m| m.as_str()));
}

// Speedup: 1000x for scalar patterns (1 compilation vs 1M compilations)
```

⸻

### 6️⃣ Type Handling and Coercion

**Signature Matching:**
```rust
// Exact signature
Signature::exact(vec![DataType::Utf8, DataType::Int32], Volatility::Immutable)

// Variadic signature
Signature::variadic(vec![DataType::Float64], Volatility::Immutable) // any(x, y, z, ...)

// One-of signature
Signature::one_of(
    vec![
        TypeSignature::Exact(vec![DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8]),
    ],
    Volatility::Immutable,
)
```

**Type Coercion:**
DataFusion's planner automatically coerces types when possible:
```sql
SELECT my_udf(int_column) FROM table
-- If my_udf expects Float64, planner inserts CAST(int_column AS DOUBLE)
```

**Manual Type Handling (this codebase):**
```rust:src/lib.rs
fn try_as_string_array(arr: &ArrayRef, dtype: &DataType) -> Result<StringArray> {
    match dtype {
        DataType::Utf8 => {
            arr.as_any().downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| DataFusionError::Execution("expected Utf8".into()))
        }
        DataType::LargeUtf8 => {
            // Convert LargeUtf8 → Utf8
            let large_arr = arr.as_any().downcast_ref::<GenericStringArray<i64>>()?;
            let mut builder = StringBuilder::new();
            for i in 0..large_arr.len() {
                if large_arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(large_arr.value(i));
                }
            }
            Ok(builder.finish())
        }
        other => Err(DataFusionError::Execution(format!("unsupported: {other:?}"))),
    }
}
```

⸻

### 7️⃣ Error Handling

**Two Types of Errors:**

**1. Plan-Time Errors (Early, Cheap):**
```rust
// Validate argument count at plan time
if args.len() != 3 {
    return Err(DataFusionError::Plan(
        "regexp_extract expects 3 arguments".into()
    ));
}
```

**2. Execution-Time Errors (Late, Expensive):**
```rust
// Validate runtime constraints
for i in 0..len {
    let idx = get_int_at(&args[2], i)?;
    if idx < 0 {
        return Err(DataFusionError::Execution(
            format!("idx must be >= 0, got {}", idx)
        ));
    }
}
```

**Best Practice:** Fail fast at plan time when possible (cheaper than failing mid-execution).

⸻

### 8️⃣ Registration and Usage

**Registration:**
```rust
let ctx = SessionContext::new();
let udf = make_regexp_extract_udf();
ctx.register_udf(udf);
```

**SQL Usage:**
```sql
SELECT regexp_extract(log_line, '(\d+\.\d+\.\d+\.\d+)', 1) AS ip
FROM logs
WHERE regexp_extract(log_line, 'ERROR', 0) != ''
```

**DataFrame API Usage:**
```rust
use datafusion_expr::{expr_fn::scalar_udf, lit, col};

let df = ctx.table("logs").await?
    .select(vec![
        scalar_udf(
            "regexp_extract",
            vec![col("log_line"), lit(r"(\d+\.\d+\.\d+\.\d+)"), lit(1)],
        ).alias("ip"),
    ])?;
```

⸻

### 9️⃣ Performance Comparison: Spark vs DataFusion UDFs

**Spark Python UDF:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def extract_domain(email):
    return email.split('@')[1] if '@' in email else ''

df.select(extract_domain(col("email")))
```

**Performance Characteristics:**
- Each row serialized to Python (Pickle)
- Python interpreter overhead per row
- Result serialized back to JVM
- Throughput: ~10-50 MB/s
- Memory: 2-3x data size (serialization buffers)

**DataFusion Rust UDF:**
```rust
let extract_domain = create_udf(
    "extract_domain",
    vec![DataType::Utf8],
    DataType::Utf8,
    Volatility::Immutable,
    Arc::new(|args| {
        let emails = args[0].as_array().as_any()
            .downcast_ref::<StringArray>()?;
        let mut builder = StringBuilder::with_capacity(emails.len());

        for i in 0..emails.len() {
            if emails.is_null(i) {
                builder.append_null();
            } else {
                let email = emails.value(i);
                let domain = email.split('@').nth(1).unwrap_or("");
                builder.append_value(domain);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }),
);
```

**Performance Characteristics:**
- Native Rust execution (no serialization)
- Vectorized processing (8192 rows/batch)
- Zero-copy Arrow arrays
- Throughput: ~400-800 MB/s
- Memory: ~1x data size

**Speedup: 10-80x for simple UDFs, 100-1000x for complex UDFs (regex, JSON parsing)**

⸻

### 🔟 Advanced: UDF + Arrow Compute Kernels

**Leverage Arrow's Optimized Kernels:**
```rust
use arrow::compute;

let my_udf = create_udf(
    "upper_and_trim",
    vec![DataType::Utf8],
    DataType::Utf8,
    Volatility::Immutable,
    Arc::new(|args| {
        let arr = args[0].as_array();

        // Use Arrow compute kernels (SIMD-optimized)
        let upper = compute::kernels::string::upper(&arr)?;
        let trimmed = compute::kernels::string::trim(&upper)?;

        Ok(ColumnarValue::Array(trimmed))
    }),
);

// Arrow kernels use SIMD internally → 5-10x faster than manual loops
```

⸻

### 1️⃣1️⃣ Testing UDFs

**Unit Tests:**
```rust
#[test]
fn test_udf_basic() {
    let input = StringArray::from(vec![Some("abc"), None, Some("xyz")]);
    let result = my_kernel(&input).unwrap();
    let output = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(output.value(0), "expected");
    assert!(output.is_null(1));
    assert_eq!(output.value(2), "expected");
}
```

**Integration Tests:**
```rust
#[tokio::test]
async fn test_udf_in_query() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_my_udf());

    let df = ctx.sql("SELECT my_udf(column) FROM table").await?;
    let batches = df.collect().await?;

    // Assert results
}
```

**Property-Based Tests (with proptest):**
```rust
proptest! {
    #[test]
    fn udf_null_propagation(s in prop::option::of(prop::string::string_regex(".*").unwrap())) {
        let input = ScalarValue::Utf8(s.clone());
        let result = my_udf(&[ColumnarValue::Scalar(input)])?;

        if s.is_none() {
            assert!(result.is_null());
        }
    }
}
```

⸻

### 🧩 TL;DR

A UDF extends a query engine with custom logic while maintaining vectorized execution.
In DataFusion, UDFs must operate on Arrow arrays (not individual rows) to achieve
10-100x performance vs. Spark Python UDFs through zero-copy, zero-serialization,
vectorized processing.

**Key Requirements:**
1. ✅ Vectorized implementation (process arrays, not rows)
2. ✅ Handle both `Scalar` and `Array` inputs (broadcasting)
3. ✅ Proper null propagation
4. ✅ Precompute expensive operations for scalar arguments
5. ✅ Use Arrow compute kernels when possible

**Result: Native performance for custom operations.**
