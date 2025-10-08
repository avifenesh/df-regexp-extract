## Null Propagation

🧸 Null Propagation explained like you're 6 years old

Null propagation is like the "missing ingredient" rule when baking cookies.

Imagine you want to make chocolate chip cookies. You need:
- Flour ✅
- Sugar ✅
- Chocolate chips ❌ (MISSING!)

What happens? You **can't make the cookies**! Even though you have flour and sugar, without chocolate chips, there's no cookie. The answer is "no cookie" (that's like NULL).

**Computer Example:**

The robot wants to know: "What's your friend's name + age?"
- Friend 1: "Alice" + 7 years = "Alice is 7"
- Friend 2: "Bob" + ??? (you don't know) = ??? (can't answer!)
- Friend 3: ??? (no name) + 8 years = ??? (can't answer!)

When something is missing (NULL), the whole answer becomes missing (NULL).

**The Rule:**
If ANY ingredient is missing → the whole recipe is missing
If ANY piece of info is missing → the whole answer is missing

This is called "null propagation" - the "missing-ness" spreads (propagates) to the final answer!

**Important exception:**

Some special operations can handle missing info:
- "Give me the name, or if it's missing, use 'Unknown'" → That's COALESCE
- "Is the name missing?" → That's IS NULL (can answer even if name is missing!)

But most operations follow the rule: missing input = missing output.

⸻

🧠 Null Propagation explained like you're a senior systems engineer

Null propagation is the implementation of SQL's three-valued logic (TRUE, FALSE, NULL) where NULL inputs to operations produce NULL outputs, ensuring semantic correctness while requiring explicit handling for performance optimization.

### 1️⃣ SQL Three-Valued Logic

**Truth Tables:**

**AND:**
| A | B | A AND B |
|---|---|---------|
| T | T | T |
| T | F | F |
| T | NULL | NULL |
| F | T | F |
| F | F | F |
| F | NULL | F |
| NULL | T | NULL |
| NULL | F | F |
| NULL | NULL | NULL |

**OR:**
| A | B | A OR B |
|---|---|--------|
| T | T | T |
| T | F | T |
| T | NULL | T |
| F | T | T |
| F | F | F |
| F | NULL | NULL |
| NULL | T | T |
| NULL | F | NULL |
| NULL | NULL | NULL |

**Scalar Operations:**
```sql
SELECT 1 + NULL;        → NULL
SELECT 'hello' || NULL; → NULL
SELECT NULL > 5;        → NULL
SELECT NULL = NULL;     → NULL (not TRUE!)
```

**Exceptions (Special Functions):**
```sql
SELECT COALESCE(NULL, 42);     → 42
SELECT NULL IS NULL;            → TRUE
SELECT NULL IS NOT NULL;        → FALSE
SELECT CASE WHEN NULL THEN 1 ELSE 2 END;  → 2
```

⸻

### 2️⃣ Implementation in DataFusion UDFs

**Pattern 1: Early NULL Check (Per-Row)**

```rust
fn my_udf(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let batch_size = determine_batch_size(args)?;
    let mut builder = StringBuilder::with_capacity(batch_size);

    for i in 0..batch_size {
        // Check if ANY argument is NULL at row i
        let any_null = args.iter().any(|arg| {
            match arg {
                ColumnarValue::Scalar(sv) => sv.is_null(),
                ColumnarValue::Array(arr) => arr.is_null(i),
            }
        });

        if any_null {
            builder.append_null();  // Propagate NULL
            continue;               // Skip computation
        }

        // All arguments are non-NULL: safe to compute
        let val0 = get_value_at(&args[0], i)?.unwrap();
        let val1 = get_value_at(&args[1], i)?.unwrap();
        builder.append_value(compute(val0, val1));
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}
```

**Pattern 2: Per-Argument NULL Check**

```rust
for i in 0..batch_size {
    let val0 = get_value_at(&args[0], i)?;  // Returns Option<T>
    let val1 = get_value_at(&args[1], i)?;
    let val2 = get_value_at(&args[2], i)?;

    match (val0, val1, val2) {
        (Some(v0), Some(v1), Some(v2)) => {
            // All present: compute
            builder.append_value(compute(v0, v1, v2));
        }
        _ => {
            // At least one NULL: propagate NULL
            builder.append_null();
        }
    }
}
```

**Pattern 3: Explicit NULL Flags (This Codebase)**

```rust:src/lib.rs
for i in 0..len {
    // Check each argument separately for NULL
    let null_str = match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v.is_none(),
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => a.as_any().downcast_ref::<StringArray>().unwrap().is_null(i),
            DataType::LargeUtf8 => a.as_any().downcast_ref::<GenericStringArray<i64>>().unwrap().is_null(i),
            _ => true,
        },
        _ => true,
    };

    let null_pat = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v.is_none(),
        ColumnarValue::Array(a) => {
            let sa = a.as_any().downcast_ref::<StringArray>().ok_or_else(||
                DataFusionError::Execution("pattern must be Utf8".into()))?;
            sa.is_null(i)
        }
        _ => true,
    };

    let null_idx = get_idx_at(args, i)?.is_none();

    // Propagate NULL if ANY argument is NULL
    if null_str || null_pat || null_idx {
        out.append_null();
        continue;
    }

    // All non-NULL: proceed with computation
    // ...
}
```

⸻

### 3️⃣ Performance Optimization: Early NULL Checks

**Critical Pattern: Check Nulls BEFORE Expensive Operations**

```rust
// ❌ BAD: Expensive operation even for NULLs
for i in 0..arr.len() {
    let s = if arr.is_null(i) { "" } else { arr.value(i) };

    // Regex compilation or matching on "" is wasted work!
    let regex = Regex::new(pattern)?;
    let result = regex.is_match(s);

    if arr.is_null(i) {
        builder.append_null();
    } else {
        builder.append_value(result.to_string());
    }
}

// ✅ GOOD: Skip expensive operation for NULLs
for i in 0..arr.len() {
    if arr.is_null(i) {
        builder.append_null();
        continue;  // Skip all expensive work
    }

    let s = arr.value(i);
    let regex = Regex::new(pattern)?;
    builder.append_value(regex.is_match(s).to_string());
}
```

**Performance Impact:**

| Operation | Cost per Row | Dataset (1M rows, 10% NULL) | Without Early Check | With Early Check | Speedup |
|-----------|--------------|------------------------------|---------------------|------------------|---------|
| **Regex match** | 100 ns | 1M calls | 100ms | 90ms | 11% |
| **Regex compile** | 5 μs | 1M calls | 5000ms | 4500ms | 11% |
| **JSON parse** | 1 μs | 1M calls | 1000ms | 900ms | 11% |
| **ML inference** | 10 ms | 1M calls | 2.8 hours | 2.5 hours | 11% |

**Insight:** For expensive operations, early NULL checks provide ~10% speedup per 10% NULL rate.

⸻

### 4️⃣ Scalar NULL Fast Path

**Optimization: If Scalar Argument is NULL, Entire Output is NULL**

```rust
// Fast path: scalar NULL input → null array output (no iteration needed)
for arg in args.iter() {
    if let ColumnarValue::Scalar(sv) = arg {
        if sv.is_null() {
            // Don't iterate 1M rows: create null array directly
            let null_array = StringArray::new_null(batch_size);
            return Ok(ColumnarValue::Array(Arc::new(null_array)));
        }
    }
}

// Normal path: may have per-row NULLs
for i in 0..batch_size {
    // ...
}
```

**StringArray::new_null Implementation:**
```rust
impl StringArray {
    pub fn new_null(length: usize) -> Self {
        // Efficient: only allocate null bitmap, no string data
        let null_buffer = Buffer::from(vec![0u8; (length + 7) / 8]);  // All zeros
        StringArray {
            offsets: vec![0; length + 1].into(),  // All zero-length spans
            values: vec![].into(),                 // Empty data buffer
            nulls: Some(NullBuffer::from(null_buffer)),
        }
    }
}
```

**Memory Savings:**
- **Without fast path:** Iterate 1M rows, build 1M null entries → 4MB offsets + 125KB bitmap
- **With fast path:** Direct null array creation → 4MB offsets + 125KB bitmap (same memory, but instant)
- **Time savings:** ~10ms iteration overhead eliminated

⸻

### 5️⃣ NULL vs Empty String (Semantic Correctness)

**Critical Distinction in Spark Compatibility:**

```rust
// Spark's regexp_extract behavior:
// - NULL input → NULL output (propagation)
// - No match → "" (empty string, NOT NULL)
// - Out-of-range group → "" (empty string, NOT NULL)

pub fn eval_regexp_extract_utf8(arr: &StringArray, regex: &Regex, idx: i32) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();

    for i in 0..arr.len() {
        // NULL propagation
        if arr.is_null(i) {
            builder.append_null();  // NULL input → NULL output
            continue;
        }

        let s = arr.value(i);  // Non-NULL input

        if let Some(caps) = regex.captures(s) {
            // Match found
            let out = caps.get(idx as usize).map_or("", |m| m.as_str());
            builder.append_value(out);  // May be "" if group out of range
        } else {
            // No match
            builder.append_value("");  // Empty string, NOT NULL
        }
    }

    Ok(Arc::new(builder.finish()))
}
```

**Test Cases:**
```rust
#[test]
fn test_null_vs_empty_string() {
    let arr = StringArray::from(vec![
        Some("abc-123"),  // Match → "abc"
        None,             // NULL → NULL
        Some("no-match"), // No match → ""
    ]);
    let re = Regex::new(r"(\w+)-(\d+)").unwrap();

    let result = eval_regexp_extract_utf8(&arr, &re, 1).unwrap();
    let output = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(output.value(0), "abc");    // Match
    assert!(output.is_null(1));             // NULL propagated
    assert_eq!(output.value(2), "");       // No match: empty string, not NULL!
    assert!(!output.is_null(2));            // Explicitly not NULL
}
```

⸻

### 6️⃣ Memory Layout: NULL Bitmap

**Arrow's Null Representation:**

```
StringArray: ["foo", NULL, "bar", NULL]

┌────────────────────────────────────────┐
│ Validity Bitmap (1 bit per value):    │
│ [1, 0, 1, 0]                           │
│  ↑  ↑  ↑  ↑                            │
│  │  │  │  └─ NULL (bit = 0)            │
│  │  │  └──── "bar" valid (bit = 1)     │
│  │  └─────── NULL (bit = 0)            │
│  └────────── "foo" valid (bit = 1)     │
│                                         │
│ Packed storage: 8 values per byte      │
│ [0b10100001]  (bit pattern)            │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Offsets: [0, 3, 3, 6, 6]               │
│           ↑  ↑  ↑  ↑  ↑                │
│           │  │  │  │  └─ End of NULL   │
│           │  │  │  └──── Start/end bar │
│           │  │  └─────── End of NULL   │
│           │  └────────── Start/end foo │
│           └───────────── Start         │
│                                         │
│ NULLs use zero-length spans (no data)  │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Data: [f, o, o, b, a, r]               │
│        0  1  2  3  4  5                │
│                                         │
│ NULLs store no actual bytes            │
└────────────────────────────────────────┘
```

**Checking if Value is NULL:**
```rust
// Fast: bitmap check only (1 bitwise AND + branch)
if arr.is_null(i) {
    // Don't access offsets or data
}

// Cost: ~0.1 ns (L1 cache hit)
```

**Accessing Value (Non-NULL):**
```rust
let s = arr.value(i);

// Steps:
// 1. Load offsets[i] and offsets[i+1] from memory
// 2. Compute span: start = offsets[i], end = offsets[i+1]
// 3. Return &data[start..end]

// Cost: ~5 ns (L1/L2 cache hit)
```

**Key Insight:** Null bitmap is separate from data → can check nulls without touching string data (cache-efficient).

⸻

### 7️⃣ Null Propagation in Arrow Compute Kernels

**Arrow Kernels Handle Nulls Automatically:**

```rust
use arrow::compute;

let a = Int32Array::from(vec![Some(1), None, Some(3)]);
let b = Int32Array::from(vec![Some(10), Some(20), None]);

// Add with automatic null propagation
let result = compute::add(&a, &b)?;
// Result: [Some(11), None, None]
//          ↑         ↑     ↑
//          1+10      NULL  NULL (either input NULL)

// Comparison with null propagation
let mask = compute::gt(&a, &lit_scalar(2))?;
// Result: [Some(false), None, Some(true)]
```

**Implementation (simplified):**
```rust
pub fn add(left: &Int32Array, right: &Int32Array) -> Result<Int32Array> {
    // Combine null bitmaps (OR operation)
    let nulls = match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(l), Some(r)) => Some(l.bitand(r)),  // Both must be valid
    };

    // Compute on values (even if NULL; will be masked out)
    let values: Vec<i32> = left.values()
        .iter()
        .zip(right.values().iter())
        .map(|(a, b)| a + b)
        .collect();

    // Construct with combined null bitmap
    Ok(Int32Array::new(values.into(), nulls))
}
```

⸻

### 8️⃣ Testing NULL Propagation

**Comprehensive Test Suite:**

```rust
#[test]
fn test_null_input_null_output() {
    // NULL input → NULL output
    let arr = StringArray::from(vec![None]);
    let result = my_kernel(&arr).unwrap();
    let output = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(output.is_null(0));
}

#[test]
fn test_mixed_nulls() {
    let arr = StringArray::from(vec![Some("a"), None, Some("b"), None]);
    let result = my_kernel(&arr).unwrap();
    let output = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert!(!output.is_null(0));
    assert!(output.is_null(1));
    assert!(!output.is_null(2));
    assert!(output.is_null(3));
}

#[test]
fn test_scalar_null() {
    // Scalar NULL → entire output NULL
    let udf = make_my_udf();
    let result = udf.invoke(&[
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b", "c"]))),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)),  // NULL scalar
    ]).unwrap();

    let arr = result.into_array(3).unwrap();
    let output = arr.as_any().downcast_ref::<StringArray>().unwrap();

    // All outputs should be NULL
    assert!(output.is_null(0));
    assert!(output.is_null(1));
    assert!(output.is_null(2));
}

#[test]
fn test_all_nulls() {
    let arr = StringArray::new_null(1000);
    let result = my_kernel(&arr).unwrap();
    let output = result.as_any().downcast_ref::<StringArray>().unwrap();

    for i in 0..1000 {
        assert!(output.is_null(i));
    }
}
```

**Property-Based Testing:**
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_null_propagation(
        values in prop::collection::vec(
            prop::option::of(prop::string::string_regex("[a-z]{1,10}").unwrap()),
            1..100
        )
    ) {
        let arr = StringArray::from(values.clone());
        let result = my_kernel(&arr).unwrap();
        let output = result.as_any().downcast_ref::<StringArray>().unwrap();

        // Property: NULL input → NULL output
        for (i, val) in values.iter().enumerate() {
            if val.is_none() {
                prop_assert!(output.is_null(i), "NULL input at index {} did not produce NULL output", i);
            }
        }
    }
}
```

⸻

### 9️⃣ Common Pitfalls

**Pitfall 1: Forgetting to Check Scalars**
```rust
// ❌ BUG: Only checks array nulls, misses scalar nulls
for i in 0..len {
    if let ColumnarValue::Array(arr) = &args[0] {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
    }
    // BUG: If args[0] is Scalar(NULL), this code doesn't handle it!
}

// ✅ FIX: Check both scalars and arrays
for i in 0..len {
    let is_null = match &args[0] {
        ColumnarValue::Scalar(sv) => sv.is_null(),
        ColumnarValue::Array(arr) => arr.is_null(i),
    };

    if is_null {
        builder.append_null();
        continue;
    }
}
```

**Pitfall 2: Confusing NULL with Empty String**
```rust
// ❌ BUG: Treats no-match as NULL (wrong!)
if let Some(caps) = regex.captures(s) {
    builder.append_value(caps.get(1).unwrap().as_str());
} else {
    builder.append_null();  // WRONG: should be append_value("")
}

// ✅ FIX: No match → empty string
if let Some(caps) = regex.captures(s) {
    builder.append_value(caps.get(1).unwrap_or(""));
} else {
    builder.append_value("");  // Correct: empty string, not NULL
}
```

**Pitfall 3: Not Leveraging Null Bitmap Optimization**
```rust
// ❌ SLOW: Access value before checking null
for i in 0..arr.len() {
    let s = arr.value(i);  // Loads data from memory
    if arr.is_null(i) {
        builder.append_null();
        continue;
    }
    builder.append_value(process(s));
}

// ✅ FAST: Check null before accessing value
for i in 0..arr.len() {
    if arr.is_null(i) {  // Bitmap check only (fast!)
        builder.append_null();
        continue;
    }
    let s = arr.value(i);  // Only load if non-null
    builder.append_value(process(s));
}
```

⸻

### 🧩 TL;DR

Null propagation ensures semantic correctness by implementing SQL's three-valued logic:
NULL input → NULL output (with exceptions for special functions like COALESCE, IS NULL).

**Performance optimization requires:**
1. ✅ **Early null checks** before expensive operations (10-30% speedup on real data)
2. ✅ **Scalar NULL fast path** (entire output NULL without iteration)
3. ✅ **Bitmap-first access** (check `is_null()` before `value()`)
4. ✅ **Correct NULL vs empty string** (semantic correctness for Spark compatibility)

**Critical distinction:**
- **NULL:** Missing/unknown value (propagates through operations)
- **Empty string:** Known value that happens to be zero-length (does NOT propagate)

Example: `regexp_extract("no-match", pattern, 1)` returns `""` (empty string), not NULL.

**Implementation pattern:**
```rust
if arr.is_null(i) { builder.append_null(); continue; }  // NULL propagation
let s = arr.value(i);  // Safe: guaranteed non-null
if let Some(result) = try_compute(s) {
    builder.append_value(result);
} else {
    builder.append_value("");  // Empty string for "no result", NOT NULL
}
```
