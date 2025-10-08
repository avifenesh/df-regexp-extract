## ColumnarValue

🧸 ColumnarValue explained like you're 6 years old

ColumnarValue is like the robot's way of understanding whether you're giving it ONE instruction for everyone, or DIFFERENT instructions for each person.

**Two ways to tell the robot what to do:**

**Way 1: Scalar (One for All)**
"Everyone paint your toy RED!"
- That's ONE color for EVERYONE
- The robot remembers: "Use RED for every single toy"
- Like having one paint bucket that everyone shares

**Way 2: Array (Different for Each)**
"Paint toy 1 RED, toy 2 BLUE, toy 3 GREEN..."
- That's DIFFERENT colors for each toy
- The robot needs a whole list of what color goes with what toy
- Like having a separate paint bucket for each toy

**Why does the robot need to know the difference?**

Because if you say "paint everything RED" (scalar), the robot can:
1. Open ONE paint can
2. Dip the brush ONCE
3. Paint all 1,000 toys with that same brush

But if you say "paint each toy a different color" (array), the robot needs to:
1. Open paint can #1 for toy #1
2. Open paint can #2 for toy #2
3. ... 1,000 times!

The first way is MUCH faster! That's why the robot needs to know: "Am I using ONE thing for everyone (Scalar), or DIFFERENT things for each (Array)?"

ColumnarValue is the robot's way of asking: "Is this instruction ONE-FOR-ALL or ONE-PER-TOY?"

⸻

🧠 ColumnarValue explained like you're a senior systems engineer

`ColumnarValue` is DataFusion's sum type that represents either a scalar constant or a columnar array, enabling efficient broadcasting semantics and constant folding optimizations in query execution.

### 1️⃣ Type Definition

```rust
pub enum ColumnarValue {
    /// A single constant value that applies to all rows
    Scalar(ScalarValue),

    /// A column of values (one per row)
    Array(ArrayRef),  // ArrayRef = Arc<dyn Array>
}
```

**ScalarValue Variants (subset):**
```rust
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    // ... 30+ more types
}
```

⸻

### 2️⃣ Why ColumnarValue Exists

**Problem Without ColumnarValue:**

Consider this query:
```sql
SELECT upper(name) FROM users WHERE id > 1000;
```

Without ColumnarValue abstraction:
```rust
// ❌ Inefficient: Must materialize scalar constant 1000 as an array
fn compare(column: &Int32Array, threshold: i32) -> BooleanArray {
    // Must create: [1000, 1000, 1000, ...] (1M times)
    let threshold_array = vec![threshold; column.len()];

    // Then compare element-wise
    column.iter().zip(threshold_array.iter())
        .map(|(a, b)| a > b)
        .collect()
}

// Memory: Unnecessary 4MB allocation for constant
```

**With ColumnarValue:**
```rust
// ✅ Efficient: Keep constant as scalar
fn compare(column: &ColumnarValue, threshold: &ColumnarValue) -> Result<ColumnarValue> {
    match (column, threshold) {
        (ColumnarValue::Array(arr), ColumnarValue::Scalar(ScalarValue::Int32(Some(val)))) => {
            // Directly compare array vs scalar (no materialization)
            let result = arr.iter()
                .map(|a| a > val)  // Reuse same val for all rows
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        // ... other cases
    }
}

// Memory: 0 bytes overhead for constant
```

⸻

### 3️⃣ The Four Combinations

Every binary operation must handle 4 cases:

```rust
fn binary_op(left: &ColumnarValue, right: &ColumnarValue) -> Result<ColumnarValue> {
    match (left, right) {
        // Case 1: Array op Array (element-wise)
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
            if l.len() != r.len() {
                return Err(DataFusionError::Execution("length mismatch".into()));
            }
            let result = element_wise_op(l, r)?;
            Ok(ColumnarValue::Array(result))
        }

        // Case 2: Array op Scalar (broadcast scalar)
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            let result = array_scalar_op(l, r)?;
            Ok(ColumnarValue::Array(result))
        }

        // Case 3: Scalar op Array (broadcast scalar)
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            let result = scalar_array_op(l, r)?;
            Ok(ColumnarValue::Array(result))
        }

        // Case 4: Scalar op Scalar (constant folding)
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            let result = scalar_scalar_op(l, r)?;
            Ok(ColumnarValue::Scalar(result))  // Return scalar, not array!
        }
    }
}
```

**Decision Table:**

| Left | Right | Output | Row Count | Memory |
|------|-------|--------|-----------|--------|
| Array(1000) | Array(1000) | Array(1000) | 1000 | 2× input |
| Array(1000) | Scalar | Array(1000) | 1000 | ~1× input |
| Scalar | Array(1000) | Array(1000) | 1000 | ~1× input |
| Scalar | Scalar | Scalar | 1 | Constant |

⸻

### 4️⃣ Constant Folding Optimization

**Key Insight: Scalar op Scalar → Don't build array!**

```rust
// ❌ BAD: Build array with single element
if all_scalar {
    let result = compute_once();
    let array = vec![result];  // Unnecessary allocation
    return Ok(ColumnarValue::Array(Arc::new(StringArray::from(array))));
}

// ✅ GOOD: Return scalar directly
if args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))) {
    let result = compute_once();
    return Ok(ColumnarValue::Scalar(result));  // No array allocation
}
```

**Query Optimizer Uses This:**
```sql
-- Query: SELECT upper('hello')
-- Optimizer sees: all arguments are literals

-- Plan-time execution:
upper('hello') → ColumnarValue::Scalar(ScalarValue::Utf8(Some("HELLO")))

-- Runtime: Zero execution cost (already computed at plan time!)
```

⸻

### 5️⃣ Length Determination Algorithm

```rust
fn determine_batch_size(args: &[ColumnarValue]) -> Result<usize> {
    let mut array_len: Option<usize> = None;

    for arg in args {
        if let ColumnarValue::Array(arr) = arg {
            let len = arr.len();

            match array_len {
                None => array_len = Some(len),
                Some(existing_len) => {
                    if existing_len != len {
                        return Err(DataFusionError::Execution(
                            format!(
                                "Array arguments must have same length: {} vs {}",
                                existing_len, len
                            )
                        ));
                    }
                }
            }
        }
    }

    Ok(array_len.unwrap_or(1))  // All scalars → batch size = 1
}
```

**Examples:**
```rust
// Example 1: All scalars
let args = vec![
    ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into()))),
    ColumnarValue::Scalar(ScalarValue::Int32(Some(42))),
];
assert_eq!(determine_batch_size(&args)?, 1);

// Example 2: One array
let args = vec![
    ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b", "c"]))),
    ColumnarValue::Scalar(ScalarValue::Int32(Some(42))),
];
assert_eq!(determine_batch_size(&args)?, 3);

// Example 3: Mismatched arrays (error)
let args = vec![
    ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b"]))),
    ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2, 3]))),
];
assert!(determine_batch_size(&args).is_err());
```

⸻

### 6️⃣ Extracting Values at Index

**Generic Value Extractor Pattern:**

```rust
/// Extract value at given index, handling both scalar and array
fn get_value_at<T>(cv: &ColumnarValue, index: usize) -> Result<Option<T>>
where
    T: Clone,
{
    match cv {
        // Scalar: return same value for all indices (broadcasting)
        ColumnarValue::Scalar(sv) => {
            if sv.is_null() {
                Ok(None)
            } else {
                Ok(Some(extract_from_scalar(sv)?))
            }
        }

        // Array: return value at specific index
        ColumnarValue::Array(arr) => {
            if arr.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(extract_from_array(arr, index)?))
            }
        }
    }
}
```

**Concrete Implementations:**

```rust
// String extractor
fn get_string_at(cv: &ColumnarValue, index: usize) -> Result<Option<String>> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(v)) => Ok(v.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(v)) => Ok(v.clone()),
        ColumnarValue::Array(arr) => {
            match arr.data_type() {
                DataType::Utf8 => {
                    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                    if str_arr.is_null(index) {
                        Ok(None)
                    } else {
                        Ok(Some(str_arr.value(index).to_string()))
                    }
                }
                DataType::LargeUtf8 => {
                    let str_arr = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    if str_arr.is_null(index) {
                        Ok(None)
                    } else {
                        Ok(Some(str_arr.value(index).to_string()))
                    }
                }
                _ => Err(DataFusionError::Execution("expected string type".into())),
            }
        }
        _ => Err(DataFusionError::Execution("invalid type".into())),
    }
}

// Int32 extractor
fn get_int32_at(cv: &ColumnarValue, index: usize) -> Result<Option<i32>> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Int32(v)) => Ok(*v),
        ColumnarValue::Array(arr) => {
            let int_arr = arr.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Execution("expected Int32".into()))?;

            if int_arr.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(int_arr.value(index)))
            }
        }
        _ => Err(DataFusionError::Execution("expected Int32".into())),
    }
}
```

**Usage in UDF:**
```rust
for i in 0..batch_size {
    let s = get_string_at(&args[0], i)?;  // Broadcasts if scalar
    let n = get_int32_at(&args[1], i)?;   // Broadcasts if scalar

    match (s, n) {
        (Some(s), Some(n)) => builder.append_value(process(s, n)),
        _ => builder.append_null(),  // NULL propagation
    }
}
```

⸻

### 7️⃣ Converting Between Scalar and Array

**Scalar → Array (Materialization):**
```rust
impl ColumnarValue {
    /// Convert scalar to array of given length
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Array(arr) => Ok(arr),
            ColumnarValue::Scalar(scalar) => {
                // Materialize scalar as array
                scalar.to_array(num_rows)
            }
        }
    }
}

// Usage:
let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into())));
let array = scalar.into_array(1000)?;  // Creates ["hello", "hello", ..., "hello"]
```

**Array → Scalar (Extract Single Value):**
```rust
impl ColumnarValue {
    /// If array has length 1, convert to scalar
    pub fn try_into_scalar(self) -> Result<ColumnarValue> {
        match self {
            ColumnarValue::Scalar(_) => Ok(self),
            ColumnarValue::Array(arr) => {
                if arr.len() == 1 {
                    let scalar = ScalarValue::try_from_array(&arr, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Err(DataFusionError::Execution(
                        format!("Cannot convert array of length {} to scalar", arr.len())
                    ))
                }
            }
        }
    }
}
```

⸻

### 8️⃣ Performance Implications

**Memory Comparison (1M rows):**

```rust
// Scenario: my_udf(column, 'constant_pattern')

// Without ColumnarValue (everything is arrays):
// - column: StringArray (1M strings, ~50 MB)
// - pattern: StringArray (1M copies of "constant_pattern", ~20 MB)
// Total: 70 MB

// With ColumnarValue (scalar for constants):
// - column: StringArray (1M strings, ~50 MB)
// - pattern: ScalarValue (1 string, ~20 bytes)
// Total: 50 MB
// Savings: 28%
```

**Computation Comparison:**

```rust
// Without scalar optimization:
for i in 0..1_000_000 {
    let pattern = pattern_array.value(i);  // Same every time
    let regex = Regex::new(pattern)?;      // Compile 1M times!
    results.push(regex.is_match(strings[i]));
}
// Time: ~5 seconds (regex compilation dominates)

// With scalar optimization:
let pattern = pattern_scalar.as_str();
let regex = Regex::new(pattern)?;  // Compile ONCE
for i in 0..1_000_000 {
    results.push(regex.is_match(strings[i]));  // Reuse compiled regex
}
// Time: ~10 milliseconds
// Speedup: 500×
```

⸻

### 9️⃣ NULL Handling

**Scalar NULL:**
```rust
let null_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(None));

// Fast path: if scalar is NULL, entire output is NULL
match &args[0] {
    ColumnarValue::Scalar(sv) if sv.is_null() => {
        // Don't iterate: return null array immediately
        let null_array = StringArray::new_null(batch_size);
        return Ok(ColumnarValue::Array(Arc::new(null_array)));
    }
    _ => { /* normal processing */ }
}
```

**Array NULL (Per-Row):**
```rust
for i in 0..batch_size {
    let value = match &args[0] {
        ColumnarValue::Scalar(sv) => {
            if sv.is_null() {
                None
            } else {
                Some(extract_scalar(sv)?)
            }
        }
        ColumnarValue::Array(arr) => {
            if arr.is_null(i) {
                None
            } else {
                Some(extract_array(arr, i)?)
            }
        }
    };

    if value.is_none() {
        builder.append_null();
    } else {
        builder.append_value(process(value.unwrap()));
    }
}
```

⸻

### 🔟 Complete UDF Template

**Production-Ready Pattern:**

```rust
use datafusion::logical_expr::{ColumnarValue, create_udf, Volatility};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use std::sync::Arc;

pub fn make_my_udf() -> datafusion::logical_expr::ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        // 1. Validate argument count
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "my_udf expects 2 arguments".into()
            ));
        }

        // 2. Determine batch size
        let batch_size = args.iter()
            .filter_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);

        // 3. Constant folding: if all scalars, return scalar
        if batch_size == 1 && args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))) {
            let result = compute_scalar_only(args)?;
            return Ok(ColumnarValue::Scalar(result));
        }

        // 4. Fast path: if any scalar argument is NULL, return null array
        for arg in args.iter() {
            if let ColumnarValue::Scalar(sv) = arg {
                if sv.is_null() {
                    let null_array = StringArray::new_null(batch_size);
                    return Ok(ColumnarValue::Array(Arc::new(null_array)));
                }
            }
        }

        // 5. Precompute expensive operations for scalar arguments
        let precomputed = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) => {
                Some(expensive_setup(pattern)?)
            }
            _ => None,
        };

        // 6. Build output array
        let mut builder = StringBuilder::with_capacity(batch_size);

        for i in 0..batch_size {
            // Extract values (broadcasts scalars automatically)
            let val0 = get_value_at(&args[0], i)?;
            let val1 = get_value_at(&args[1], i)?;

            // NULL propagation
            match (val0, val1) {
                (Some(v0), Some(v1)) => {
                    // Use precomputed value if available
                    let setup = if let Some(ref pc) = precomputed {
                        pc
                    } else {
                        &expensive_setup(v1)?
                    };

                    builder.append_value(compute(v0, setup));
                }
                _ => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    });

    create_udf(
        "my_udf",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        fun,
    )
}
```

⸻

### 1️⃣1️⃣ Testing ColumnarValue Logic

```rust
#[test]
fn test_all_four_combinations() {
    // Scalar × Scalar
    let result = my_udf(&[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
    ]).unwrap();
    assert!(matches!(result, ColumnarValue::Scalar(_)));

    // Array × Scalar
    let result = my_udf(&[
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b"]))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
    ]).unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(arr.len(), 2),
        _ => panic!("expected array"),
    }

    // Scalar × Array
    let result = my_udf(&[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into()))),
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2]))),
    ]).unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(arr.len(), 2),
        _ => panic!("expected array"),
    }

    // Array × Array
    let result = my_udf(&[
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b"]))),
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2]))),
    ]).unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(arr.len(), 2),
        _ => panic!("expected array"),
    }
}

#[test]
fn test_length_mismatch() {
    let result = my_udf(&[
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b"]))),
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2, 3]))),
    ]);
    assert!(result.is_err());
}
```

⸻

### 🧩 TL;DR

ColumnarValue is DataFusion's abstraction for handling both scalar constants and array columns,
enabling:
- **Broadcasting semantics** (scalar applies to all rows without duplication)
- **Constant folding** (scalar-only operations computed at plan time)
- **Memory efficiency** (20 bytes vs 20 MB for 1M-row constant)
- **Performance optimization** (1 expensive operation vs 1M operations)

**Critical pattern:** Always check for scalar arguments and precompute expensive operations
outside the row-processing loop. This single optimization accounts for 100-1000× speedup in
common production queries.

**UDF implementation checklist:**
1. ✅ Handle all 4 combinations (S×S, S×A, A×S, A×A)
2. ✅ Return scalar for S×S (constant folding)
3. ✅ Validate array lengths match
4. ✅ Precompute expensive scalar operations outside loop
5. ✅ Fast-path: scalar NULL → null array (no iteration)
