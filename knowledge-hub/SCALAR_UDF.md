## ScalarUDF

🧸 ScalarUDF explained like you're 6 years old

A ScalarUDF is like a recipe card you give to your robot helper!

The recipe card has everything the robot needs to know:
- **Name:** "Make_Smoothie" (what to call this trick)
- **Ingredients needed:** banana + milk (what things it needs)
- **What you get:** smoothie (what comes out)
- **Instructions:** "Blend them together!" (how to do it)

Once you give the robot this recipe card, you can say "Hey robot, make_smoothie!" and it knows exactly what to do!

**The special thing about a ScalarUDF:**

It works on ONE item at a time (logically):
- Give it banana #1 → get smoothie #1
- Give it banana #2 → get smoothie #2
- Give it banana #3 → get smoothie #3

But the CLEVER part? The robot actually makes ALL the smoothies at once in a big batch! It's like having a giant blender instead of a tiny one - WAY faster!

**Compare to other types:**

- **ScalarUDF:** One thing → One result (do this for EACH toy)
- **AggregateUDF:** Many things → One result (count ALL the toys)
- **WindowUDF:** Many things → Many results (rank toys by size)

The recipe card (ScalarUDF) tells the robot:
1. What it's called
2. What ingredients it needs
3. What you'll get back
4. How to make it super fast!

⸻

🧠 ScalarUDF explained like you're a senior systems engineer

`ScalarUDF` is DataFusion's metadata container for scalar user-defined functions, encapsulating function identity, type signature, volatility characteristics, and the implementation, enabling query planner integration and optimization.

### 1️⃣ Structure and Components

**Type Definition (DataFusion 50.x):**
```rust
pub struct ScalarUDF {
    /// Function name for SQL and catalog lookup
    name: String,

    /// Type signature: input types, variadic, coercion rules
    signature: Signature,

    /// Return type (can be dynamic based on input types)
    return_type: Arc<dyn ReturnTypeFunction>,

    /// Volatility: Immutable | Stable | Volatile
    volatility: Volatility,

    /// Implementation: the actual function
    fun: ScalarFunctionImplementation,
}

pub type ScalarFunctionImplementation = Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;
```

**Creation via `create_udf` Helper:**
```rust
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

let udf = create_udf(
    "my_function",                              // name
    vec![DataType::Utf8, DataType::Int32],     // input types
    DataType::Utf8,                             // return type
    Volatility::Immutable,                      // deterministic
    Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        // Implementation
        todo!()
    }),
);
```

⸻

### 2️⃣ Signature: Type System Integration

**Signature Variants:**

**1. Exact Signature (Fixed Types):**
```rust
Signature::exact(
    vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
    Volatility::Immutable,
)

// SQL: regexp_extract(str: Utf8, pattern: Utf8, idx: Int32) → Utf8
// Rejects: regexp_extract(str, pattern, 1.5)  ← Float64 not Int32
```

**2. Variadic Signature (Variable Arguments):**
```rust
Signature::variadic(
    vec![DataType::Float64],
    Volatility::Immutable,
)

// SQL: my_sum(a, b, c, ...)  all Float64
// Accepts: my_sum(1.0, 2.0, 3.0, 4.0, ...)
```

**3. One-Of Signature (Union of Signatures):**
```rust
Signature::one_of(
    vec![
        TypeSignature::Exact(vec![DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8]),
    ],
    Volatility::Immutable,
)

// Accepts either Utf8 OR LargeUtf8
```

**4. Any Signature (Arbitrary Types, Manual Validation):**
```rust
Signature::any(/* number of args */, Volatility::Immutable)

// UDF validates types at runtime
```

⸻

### 3️⃣ Volatility: Optimization Enabler

**Volatility Levels:**

| Level | Meaning | Optimization Impact | Examples |
|-------|---------|---------------------|----------|
| **Immutable** | Pure function: f(x) always produces same output for same input | • Constant folding<br>• CSE (Common Subexpression Elimination)<br>• Cross-query caching<br>• Predicate pushdown | `upper()`, `abs()`, `regexp_extract()` |
| **Stable** | Same within query execution, may differ between queries | • CSE within query<br>• No cross-query cache | `now()`, `current_user()`, `random_seed()` |
| **Volatile** | Different every call | • No optimization<br>• Execute every row | `random()`, `uuid()`, `now_micros()` |

**Immutable Example:**
```sql
-- Query optimizer recognizes upper('hello') is constant
SELECT upper('hello'), upper('hello'), col FROM table;

-- Optimized to:
SELECT 'HELLO', 'HELLO', col FROM table;
-- upper() executed 0 times (constant folded at plan time)
```

**Stable Example:**
```sql
-- now() evaluated once per query
SELECT now(), col FROM table WHERE created_at < now();

-- Execution:
-- 1. Evaluate now() → '2024-10-08 10:30:00' (once)
-- 2. Use same value throughout query
```

**Volatile Example:**
```sql
-- random() must be evaluated for each row
SELECT random(), col FROM table;

-- Cannot optimize or cache (different value per row required)
```

⸻

### 4️⃣ Return Type Function

**Static Return Type:**
```rust
// Simple case: always returns same type
create_udf(
    "upper",
    vec![DataType::Utf8],
    DataType::Utf8,  // Static return type
    Volatility::Immutable,
    fun,
)
```

**Dynamic Return Type:**
```rust
// Return type depends on input types
let return_type = Arc::new(|arg_types: &[DataType]| -> Result<Arc<DataType>> {
    match &arg_types[0] {
        DataType::Utf8 => Ok(Arc::new(DataType::Utf8)),
        DataType::LargeUtf8 => Ok(Arc::new(DataType::LargeUtf8)),
        _ => Err(DataFusionError::Plan("expected string type".into())),
    }
});

let udf = ScalarUDF::new(
    "my_func",
    Signature::any(1, Volatility::Immutable),
    return_type,  // Dynamic
    fun,
);
```

⸻

### 5️⃣ Registration and Invocation

**Registration in SessionContext:**
```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();
let udf = make_my_udf();

// Register UDF (makes it available in SQL and DataFrame API)
ctx.register_udf(udf);
```

**SQL Invocation:**
```sql
-- After registration, use in SQL
SELECT my_function(column, 'pattern', 1) FROM table;

-- Can be used in WHERE, JOIN, GROUP BY, etc.
SELECT * FROM table WHERE my_function(column, 'test', 0) != '';
```

**DataFrame API Invocation:**
```rust
use datafusion::prelude::*;
use datafusion_expr::{expr_fn::scalar_udf, lit, col};

// Get DataFrame
let df = ctx.table("my_table").await?;

// Use UDF via scalar_udf function
let df = df.select(vec![
    scalar_udf(
        "my_function",  // Function name (string)
        vec![
            col("column"),      // First argument
            lit("pattern"),     // Second argument (literal)
            lit(1),             // Third argument
        ],
    ).alias("result"),
])?;

// Execute
let results = df.collect().await?;
```

**Alternative: Direct UDF Object Usage:**
```rust
// Get registered UDF
let udf = ctx.udf("my_function")?;

// Call directly (for programmatic use)
let result = udf.call(vec![
    ColumnarValue::Array(Arc::new(strings)),
    ColumnarValue::Scalar(ScalarValue::Utf8(Some("pattern".into()))),
    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
])?;
```

⸻

### 6️⃣ Query Planner Integration

**Logical Planning Phase:**

```
SQL: SELECT my_udf(col, 'const') FROM table

↓ Parse

LogicalPlan:
  Projection: my_udf(col, 'const')
    TableScan: table

↓ Analysis (type checking)

1. Lookup UDF by name: "my_udf"
2. Check signature: my_udf(Utf8, Utf8) ← matches signature?
3. Get return type: Utf8
4. Validate: OK

↓ Optimization (rule-based)

Optimizer sees:
- my_udf is Immutable
- Second argument is literal 'const'
- Can precompute expensive setup (e.g., regex compilation)

↓ Physical Planning

PhysicalPlan:
  ProjectionExec: my_udf(col, 'const')
    ParquetExec: table

↓ Execution

For each RecordBatch:
  my_udf.call([batch.column("col"), Scalar('const')])
```

**Optimizer Rules Applied:**

1. **Constant Folding:**
```sql
-- Query: SELECT my_udf('hello', 'world')
-- Optimizer detects: all arguments are literals, UDF is Immutable
-- Executes at plan time: result = 'computed_value'
-- Physical plan: SELECT 'computed_value' FROM table  (no UDF execution!)
```

2. **Common Subexpression Elimination:**
```sql
-- Query: SELECT my_udf(col, 'pat'), my_udf(col, 'pat'), col FROM table
-- Optimizer detects: same UDF called twice with same arguments
-- Physical plan: call UDF once, reuse result twice
```

3. **Predicate Pushdown:**
```sql
-- Query: SELECT * FROM table WHERE my_udf(col, 'pattern') = 'value'
-- If UDF is Immutable and table is Parquet:
-- Push predicate into Parquet scan (row group pruning)
```

⸻

### 7️⃣ Error Handling

**Plan-Time Errors (Preferred):**
```rust
// Check during logical planning (before execution starts)
pub fn make_my_udf() -> ScalarUDF {
    create_udf(
        "my_udf",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int32],  // Strict signature
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            // Signature already validated by planner
            // Only handle runtime errors here
            compute(args)
        }),
    )
}

// SQL: SELECT my_udf(col, 'pat', 'not_an_int') FROM table
// Error at plan time: ^^^^^^^^^^^
// "Cannot coerce 'not_an_int' (Utf8) to Int32"
```

**Runtime Errors (Unavoidable):**
```rust
Arc::new(|args| {
    // Runtime validation (can't be checked at plan time)
    let idx = get_int_at(&args[2], 0)?;

    if idx < 0 {
        return Err(DataFusionError::Execution(
            format!("idx must be >= 0, got {}", idx)
        ));
    }

    compute(args)
})

// SQL: SELECT my_udf(col, 'pat', column_with_negatives) FROM table
// Error during execution: idx = -1 encountered
```

**Error Propagation:**
```rust
// UDF errors propagate up the execution stack
// Query fails with error message
// Partial results are discarded (transactions)
```

⸻

### 8️⃣ ScalarUDF vs Other UDF Types

| Type | Input → Output | Use Case | Example |
|------|----------------|----------|---------|
| **ScalarUDF** | N rows → N rows | Row-level transformation | `upper(name)`, `regexp_extract(log, pat, 1)` |
| **AggregateUDF** | N rows → 1 value (per group) | Aggregation | `custom_median(values)`, `array_agg(items)` |
| **WindowUDF** | N rows → N rows (with context) | Window functions | `custom_rank()`, `moving_avg(value, 3)` |
| **TableUDF** | Table → Table | Table generation | `range(1, 100)`, `explode(array_col)` |

**ScalarUDF Characteristics:**
- ✅ Vectorized (processes batches of 8192 rows)
- ✅ Stateless (each row independent)
- ✅ Parallelizable (across partitions)
- ✅ Optimizable (constant folding, CSE)

⸻

### 9️⃣ Testing ScalarUDF

**Unit Tests (Direct Invocation):**
```rust
#[test]
fn test_udf_registration() {
    let udf = make_my_udf();

    assert_eq!(udf.name(), "my_function");
    assert_eq!(udf.signature().volatility, Volatility::Immutable);
}

#[test]
fn test_udf_invocation() {
    let udf = make_my_udf();

    let input = vec![
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("pattern".into()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
    ];

    let result = udf.call(input).unwrap();
    // Assert result
}
```

**Integration Tests (via SQL):**
```rust
#[tokio::test]
async fn test_udf_in_query() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_my_udf());

    // Create test table
    let schema = Arc::new(Schema::new(vec![
        Field::new("col", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["test1", "test2"]))],
    ).unwrap();
    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("test_table", Arc::new(provider)).unwrap();

    // Execute query
    let df = ctx.sql("SELECT my_function(col, 'pattern', 1) FROM test_table").await.unwrap();
    let results = df.collect().await.unwrap();

    // Validate results
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);
}
```

**Error Handling Tests:**
```rust
#[tokio::test]
async fn test_udf_type_mismatch() {
    let ctx = SessionContext::new();
    ctx.register_udf(make_my_udf());

    // Should fail: wrong types
    let result = ctx.sql("SELECT my_function(123, 'pat', 1) FROM table").await;
    assert!(result.is_err());
}

#[test]
fn test_udf_runtime_error() {
    let udf = make_my_udf();

    let input = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".into()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("pattern".into()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),  // Invalid idx
    ];

    let result = udf.call(input);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("idx must be >= 0"));
}
```

⸻

### 🔟 Performance Considerations

**Function Call Overhead:**
```rust
// ScalarUDF uses Arc<dyn Fn> for implementation
// Overhead: ~2ns per batch (not per row!)

// 1M rows, 8192 rows/batch:
// Batches: 1M / 8192 ≈ 122 batches
// Overhead: 122 × 2ns = 244ns total
// Negligible compared to computation (milliseconds)
```

**Inlining Barrier:**
```rust
// UDF boundary prevents LLVM inlining across UDF calls
// Trade-off: Flexibility vs. 5-10% performance

// Mitigation: Keep hot computation in kernel functions (can be inlined)
pub fn kernel_hot_path(arr: &StringArray) -> Result<ArrayRef> {
    // This can be inlined by LLVM
}

pub fn make_udf() -> ScalarUDF {
    create_udf(/* ... */, Arc::new(|args| {
        let arr = extract_array(args)?;
        kernel_hot_path(&arr)  // Call inlinable kernel
    }))
}
```

**Cloning and Arc:**
```rust
// ScalarUDF uses Arc internally (cheap to clone)
let udf = make_my_udf();
let udf_clone = udf.clone();  // Only increments reference count (O(1))

// Can safely share across threads
std::thread::spawn(move || {
    udf_clone.call(...);  // Thread-safe
});
```

⸻

### 🧩 TL;DR

ScalarUDF is DataFusion's metadata wrapper for scalar functions, containing:
- **Identity:** Name for SQL/catalog lookup
- **Type System:** Signature for argument validation and coercion
- **Optimization:** Volatility enables constant folding, CSE, predicate pushdown
- **Implementation:** Arc<dyn Fn> for thread-safe, vectorized execution

**Key properties:**
- ✅ Registered in SessionContext (SQL + DataFrame API)
- ✅ Query planner validates types at plan time
- ✅ Optimizer applies transformations based on volatility
- ✅ Executed in batches (8192 rows) with ~2ns overhead per batch

**Best practices:**
1. Use `Volatility::Immutable` for pure functions (enables optimization)
2. Validate types via signature (avoid runtime type errors)
3. Keep hot computation in kernel functions (enables inlining)
4. Test both direct invocation and SQL integration
5. Provide clear error messages for runtime failures

**Performance:** Overhead is negligible (~0.001% for typical UDFs); the actual computation dominates.
