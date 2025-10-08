## DataFusion

🧸 DataFusion explained like you're 6 years old

Imagine you have a super-smart robot helper that can answer questions about your toy collection really fast.

You can ask it things like:
- "Show me all my red cars!"
- "How many stuffed animals do I have?"
- "Which toys are bigger than my hand?"

The robot is really smart because:
1. It uses those special LEGO trays we talked about (Arrow!) to find things quickly
2. It doesn't need to search through every single toy one at a time — it can look at whole groups at once
3. It's built with super-strong metal parts (that's Rust!) so it never gets tired or needs breaks

Other robots might need to stop and rest (that's like Java's garbage collection), but this robot just keeps working non-stop!

The best part? You can teach this robot NEW tricks (those are UDFs - User Defined Functions), and once it learns them, it can do them super fast for ALL your toys at once, not one toy at a time.

That's DataFusion — a super-fast, never-tired robot that organizes and searches through data using Arrow's special organization system.

⸻

🧠 DataFusion explained like you're a senior systems engineer

Apache DataFusion is an embeddable, extensible query execution framework written in Rust that operates natively on Apache Arrow columnar data.

### 1️⃣ Architecture Overview

**Modular Design:**
```
SQL/DataFrame API
      ↓
Logical Plan (optimizer: rule-based + cost-based)
      ↓
Physical Plan (executor: Volcano-style push model)
      ↓
RecordBatch streams (Arrow in-memory format)
```

**Key Components:**
- **Logical Plan:** AST representation with 100+ optimizer rules (predicate pushdown, projection pruning, constant folding, etc.)
- **Physical Plan:** Executable operators implementing `ExecutionPlan` trait
- **Execution Runtime:** Tokio-based async streaming execution
- **Extension Points:** 10+ APIs for custom UDFs, table providers, optimizers, file formats

⸻

### 2️⃣ Execution Model

**Vectorized Processing:**
- Processes data in batches (default: 8192 rows per `RecordBatch`)
- Each operator yields `Stream<Item = RecordBatch>`
- Operators are chained via async iterators (zero-copy between stages)

**Example Execution Flow:**
```rust
// Physical plan for: SELECT sum(price) FROM sales WHERE region = 'US'
PhysicalPlan:
  AggregateExec (sum)
    FilterExec (region = 'US')
      ParquetExec (scan sales.parquet)

// Execution:
ParquetExec → RecordBatch(8192 rows)
  → FilterExec processes entire batch via SIMD predicate
  → AggregateExec accumulates via vectorized sum kernel
```

⸻

### 3️⃣ Performance Characteristics

**Why 2-3x Faster Than Spark:**

| Aspect | Spark | DataFusion | Impact |
|--------|-------|------------|--------|
| **Language** | JVM (Scala/Java) | Rust | No GC pauses |
| **Execution** | Row-at-a-time (Python UDFs) | Vectorized (8K rows/batch) | ~10x throughput |
| **Memory** | JVM heap + off-heap | Zero-copy Arrow | 50% less memory |
| **Startup** | JIT warmup (~10s) | Native binary (instant) | Better for short queries |
| **Serialization** | Java serialization / Pickle | None (Arrow native) | 10x less CPU |

**Concrete Example:**
```rust
// DataFusion: Vectorized filter (processes 8192 rows at once)
let filtered = arrow::compute::filter(&batch.column(0), &predicate_mask)?;
// Uses SIMD: processes 8 values per instruction (AVX2)

// Spark: Row-at-a-time in Python UDF
for row in partition:
    if row["region"] == "US":  # Python interpreter overhead per row
        output.append(row)
```

⸻

### 4️⃣ Query Optimization

**Logical Optimization (Rule-Based):**
- **Predicate Pushdown:** Move filters close to scans
- **Projection Pushdown:** Only read required columns (critical for Parquet)
- **Constant Folding:** Evaluate expressions at plan time
- **Common Subexpression Elimination:** Reuse computed values

**Example:**
```sql
SELECT upper(name) FROM users WHERE region = 'US' AND 1 = 1
```

Optimized to:
```sql
-- 1 = 1 eliminated (constant folding)
-- upper(name) pushed after filter (reduces work)
SELECT upper(name) FROM users WHERE region = 'US'
```

**Physical Optimization (Cost-Based):**
- Join order selection
- Hash join vs. sort-merge join
- Partition pruning for partitioned tables

⸻

### 5️⃣ Extensibility

**Custom UDFs:**
```rust
let udf = create_udf(
    "my_function",
    vec![DataType::Int32],
    DataType::Int32,
    Volatility::Immutable,
    Arc::new(|args: &[ColumnarValue]| {
        // Vectorized implementation on Arrow arrays
        Ok(ColumnarValue::Array(compute(args)?))
    }),
);
ctx.register_udf(udf);
```

**Custom Table Providers:**
```rust
impl TableProvider for MyCustomSource {
    async fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // Implement custom data source (REST API, database, etc.)
    }
}
ctx.register_table("my_source", Arc::new(MyCustomSource::new()));
```

⸻

### 6️⃣ Memory Management

**Zero-Copy Pipeline:**
```
Parquet File → Arrow RecordBatch (mmap)
  ↓ (shared Arc pointer)
Filter → RecordBatch (slice, no copy)
  ↓ (shared Arc pointer)
Aggregate → RecordBatch (new allocation only for results)
```

**Memory Pressure Handling:**
- Spill-to-disk for sorts/joins exceeding memory limits
- Backpressure propagation via async streams
- Per-operator memory budgets

⸻

### 7️⃣ Comparison: DataFusion vs Spark

**When DataFusion Wins:**
- Single-node analytics (< 1TB)
- Embedded use cases (in-app analytics)
- Low-latency queries (< 1s)
- Custom query engines (data warehouses, time-series DBs)

**When Spark Wins:**
- Multi-petabyte distributed datasets
- Mature ecosystem (MLlib, GraphX, connectors)
- Complex DAG workflows with shuffles

**Hybrid Approach (Ballista):**
DataFusion + distributed scheduler = distributed query engine
- Uses DataFusion as the execution engine per node
- Adds shuffle/partition coordination

⸻

### 8️⃣ Integration with Arrow

**Native Arrow Operations:**
```rust
// DataFusion operator directly manipulates Arrow arrays
impl ExecutionPlan for FilterExec {
    fn execute(&self, ...) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;

        Ok(Box::pin(stream.map(move |batch| {
            let batch = batch?;

            // Evaluate predicate (returns BooleanArray)
            let predicate = self.predicate.evaluate(&batch)?;

            // Arrow compute kernel: vectorized filter
            let filtered = arrow::compute::filter_record_batch(&batch, &predicate)?;

            Ok(filtered)
        })))
    }
}
```

⸻

### 9️⃣ Real-World Use Cases

**1. InfluxDB (Time-Series Database):**
- Uses DataFusion for SQL query layer
- Custom table providers for time-series storage
- 10x query performance vs. previous implementation

**2. Delta Lake (Databricks):**
- `delta-rs` uses DataFusion for query execution
- Native Rust implementation of Delta protocol

**3. Cube.js (Analytics API):**
- Embedded DataFusion for sub-second OLAP queries
- Replaces in-process PostgreSQL

**4. AWS (Internal):**
- Multiple AWS services use DataFusion for log analytics
- Billions of events/day processed

⸻

### 🔟 Key Takeaways for Performance Engineers

**Memory Hierarchy Awareness:**
```
L1 cache (32 KB): Single RecordBatch (~8K rows × 10 columns × 8 bytes = 640 KB)
L2 cache (256 KB): Fits 2-3 small RecordBatches
L3 cache (8 MB): Fits ~12 RecordBatches

DataFusion's batch size (8192 rows) is tuned to fit L2/L3 cache.
```

**Profiling Hot Paths:**
```bash
# Profile DataFusion query
perf record -g ./target/release/query-runner
perf report

# Common hot spots:
# 1. Arrow compute kernels (filter, take, etc.)
# 2. Parquet decoding
# 3. Hash table operations (joins, aggregates)
# 4. UDF implementations
```

**Optimization Opportunities:**
1. **Kernel Selection:** Use Arrow compute kernels (SIMD-optimized) over custom loops
2. **Batch Size Tuning:** Smaller batches = more cache hits, larger = less overhead
3. **Predicate Ordering:** Put cheap filters first (null checks before regex)
4. **Memory Pooling:** Reuse Arrow builders across batches

⸻

### 🧩 TL;DR

DataFusion is a Rust-native, vectorized query engine that operates on Arrow columnar data,
providing 2-3x Spark performance for single-node workloads through zero-GC execution,
SIMD vectorization, and zero-copy memory management, while remaining embeddable and extensible.

⸻

**Why it matters for a Spark accelerator company:**
- Drop-in replacement for Spark execution engine
- Same Arrow memory format = zero data conversion
- Native Rust = no JVM overhead
- Extensible UDFs = customer-specific functions
- Result: 2-3x faster, 50% less memory, instant startup
