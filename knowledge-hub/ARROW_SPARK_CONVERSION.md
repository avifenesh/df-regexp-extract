## Arrow ↔ Spark Data Conversion

🧸 Arrow ↔ Spark explained like you're 6 years old

Imagine Spark organizes your toy collection like a **list of kids**:
- Kid 1: Red car, age 5, name "Alice"
- Kid 2: Blue block, age 6, name "Bob"
- Kid 3: Yellow ball, age 5, name "Charlie"

This is called "row-by-row" - each kid gets a bag with all their stuff mixed together.

But Arrow wants to organize by **type of thing**:
- All toys: [Red car, Blue block, Yellow ball]
- All ages: [5, 6, 5]
- All names: ["Alice", "Bob", "Charlie"]

**How does Arrow convert Spark's bags into trays?**

It's like sorting your toys! You:
1. Open Kid 1's bag → take out the toy → put in toy tray
2. Open Kid 1's bag → take out the age → put in age tray
3. Open Kid 1's bag → take out the name → put in name tray
4. Repeat for Kid 2, Kid 3...

Now all the toys are together, all the ages are together, all the names are together!

**The clever part:** Once Arrow has the trays organized, the computer can look at ALL the toys super fast (they're all next to each other), instead of opening each kid's bag one by one!

⸻

🧠 Arrow ↔ Spark explained like you're a senior systems engineer

Arrow and Spark use different in-memory data representations. Converting between them requires transposing data from row-oriented (Spark) to columnar (Arrow) or vice versa. This conversion is critical for interoperability but has performance implications.

### 1️⃣ Spark's Internal Representation (Pre-Arrow)

**InternalRow (Tungsten Binary Format):**
Spark uses a custom row-based binary format called Tungsten:

```
RecordBatch of 3 rows:
┌──────────────────────────────────────────┐
│ Row 0: [1, "Alice", 25.5]                │
│   ├─ Int32: 1                            │
│   ├─ String: "Alice" (offset+length)     │
│   └─ Float64: 25.5                       │
├──────────────────────────────────────────┤
│ Row 1: [2, "Bob", 30.0]                  │
│   ├─ Int32: 2                            │
│   ├─ String: "Bob" (offset+length)       │
│   └─ Float64: 30.0                       │
├──────────────────────────────────────────┤
│ Row 2: [3, "Charlie", 22.8]              │
│   ├─ Int32: 3                            │
│   ├─ String: "Charlie" (offset+length)   │
│   └─ Float64: 22.8                       │
└──────────────────────────────────────────┘

Memory Layout (simplified):
[row0_header | 1 | "Alice" | 25.5]
[row1_header | 2 | "Bob"   | 30.0]
[row2_header | 3 | "Charlie" | 22.8]

Each row is a contiguous memory block
```

**Characteristics:**
- Optimized for: Row-at-a-time processing (filtering, projections)
- Cache-unfriendly: Accessing single column requires scanning all rows
- Variable-width encoding: Strings and complex types inline

### 2️⃣ Arrow's Columnar Representation

**RecordBatch:**
```
Same 3 rows in Arrow format:
┌──────────────────────────────────────────┐
│ Column 0 (id: Int32):                    │
│   Values: [1, 2, 3]                      │
│   Nulls:  [1, 1, 1] (all valid)          │
├──────────────────────────────────────────┤
│ Column 1 (name: Utf8):                   │
│   Offsets: [0, 5, 8, 15]                 │
│   Data:    "AliceBobCharlie"             │
│   Nulls:   [1, 1, 1]                     │
├──────────────────────────────────────────┤
│ Column 2 (score: Float64):               │
│   Values: [25.5, 30.0, 22.8]             │
│   Nulls:  [1, 1, 1]                      │
└──────────────────────────────────────────┘

Memory Layout:
Column 0: [1, 2, 3]  (contiguous)
Column 1: [0,5,8,15] "AliceBobCharlie"  (offsets + data)
Column 2: [25.5, 30.0, 22.8]  (contiguous)

All values of same column are adjacent
```

**Characteristics:**
- Optimized for: Columnar operations (aggregations, scans)
- Cache-friendly: Single column access is sequential
- SIMD-friendly: Vectorized operations on contiguous data

### 3️⃣ Spark → Arrow Conversion (Row to Columnar)

**Process: Transposition**

**Step 1: Allocate Columnar Buffers**
```rust
// For 1000 rows with schema: (Int32, Utf8, Float64)
let mut id_builder = Int32Builder::with_capacity(1000);
let mut name_builder = StringBuilder::with_capacity(1000);
let mut score_builder = Float64Builder::with_capacity(1000);
```

**Step 2: Iterate Rows and Extract Columns**
```scala
// Spark side (Scala)
def sparkToArrow(iterator: Iterator[InternalRow], schema: StructType): RecordBatch = {
  val allocator = new RootAllocator()
  val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)

  iterator.foreach { row =>
    // Extract each column value from row
    val id = row.getInt(0)
    val name = row.getString(1)
    val score = row.getDouble(2)

    // Append to Arrow column vectors
    vectorSchemaRoot.getVector("id").setSafe(rowIndex, id)
    vectorSchemaRoot.getVector("name").setSafe(rowIndex, name.getBytes)
    vectorSchemaRoot.getVector("score").setSafe(rowIndex, score)

    rowIndex += 1
  }

  vectorSchemaRoot.setRowCount(rowIndex)
  vectorSchemaRoot
}
```

**Visualization:**
```
Input (Spark Rows):
Row 0: [1, "Alice", 25.5]
Row 1: [2, "Bob", 30.0]
Row 2: [3, "Charlie", 22.8]

Transposition Process:
┌─────────────────────────────────────┐
│ For each row:                       │
│   Extract col[0] → append to Vec[0] │
│   Extract col[1] → append to Vec[1] │
│   Extract col[2] → append to Vec[2] │
└─────────────────────────────────────┘

Output (Arrow Columns):
Column 0: [1, 2, 3]
Column 1: ["Alice", "Bob", "Charlie"]
Column 2: [25.5, 30.0, 22.8]
```

**Performance Cost:**
- **Time:** O(rows × columns) - must touch every value
- **Memory:** 2× (source rows + destination columns exist simultaneously)
- **Cache:** Row iteration is cache-unfriendly (scattered access)

**Typical Conversion Speed:**
- **Simple types (Int32, Float64):** ~500 MB/s
- **Strings (UTF8):** ~200 MB/s (due to encoding validation)
- **Complex types (structs, arrays):** ~100 MB/s

### 4️⃣ Arrow → Spark Conversion (Columnar to Row)

**Process: Reassembly**

**Step 1: Extract Column Vectors**
```scala
def arrowToSpark(recordBatch: RecordBatch): Iterator[InternalRow] = {
  val idVector = recordBatch.getVector("id").asInstanceOf[IntVector]
  val nameVector = recordBatch.getVector("name").asInstanceOf[VarCharVector]
  val scoreVector = recordBatch.getVector("score").asInstanceOf[Float8Vector]

  (0 until recordBatch.getRowCount).iterator.map { i =>
    // Extract values from each column at index i
    val id = if (idVector.isNull(i)) null else idVector.get(i)
    val name = if (nameVector.isNull(i)) null else nameVector.get(i)
    val score = if (scoreVector.isNull(i)) null else scoreVector.get(i)

    // Create Spark InternalRow
    InternalRow(id, UTF8String.fromString(name), score)
  }
}
```

**Visualization:**
```
Input (Arrow Columns):
Column 0 (id):    [1, 2, 3]
Column 1 (name):  ["Alice", "Bob", "Charlie"]
Column 2 (score): [25.5, 30.0, 22.8]

Reassembly Process:
┌─────────────────────────────────────┐
│ For each row index i:               │
│   Read col[0][i] → row[0]           │
│   Read col[1][i] → row[1]           │
│   Read col[2][i] → row[2]           │
│   Create InternalRow(row)           │
└─────────────────────────────────────┘

Output (Spark Rows):
Row 0: [1, "Alice", 25.5]
Row 1: [2, "Bob", 30.0]
Row 2: [3, "Charlie", 22.8]
```

**Performance Cost:**
Similar to Spark → Arrow, but may be faster if Spark can avoid full materialization (lazy iteration).

### 5️⃣ Spark Arrow Integration (PySpark)

**When Spark Uses Arrow:**

**1. Pandas UDF (Python → JVM):**
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("int")
def pandas_multiply(series: pd.Series) -> pd.Series:
    return series * 2

# Spark → Arrow → Pandas → Python UDF → Pandas → Arrow → Spark
df.select(pandas_multiply(col("value")))
```

**Data Flow:**
```
┌─────────────────────────────────────────────────────────┐
│ 1. Spark InternalRow (JVM)                              │
│    [Row 0, Row 1, ..., Row N]                           │
├─────────────────────────────────────────────────────────┤
│ 2. Convert to Arrow (JVM)                               │
│    ┌────────────────────────────────────────┐           │
│    │ Column-oriented Arrow RecordBatch      │           │
│    └────────────────────────────────────────┘           │
├─────────────────────────────────────────────────────────┤
│ 3. Send Arrow bytes to Python process                   │
│    (Zero-copy via shared memory or socket)              │
├─────────────────────────────────────────────────────────┤
│ 4. Python deserializes Arrow → Pandas DataFrame         │
│    pd.DataFrame with columnar data                      │
├─────────────────────────────────────────────────────────┤
│ 5. Python UDF executes on Pandas                        │
│    result = series * 2                                  │
├─────────────────────────────────────────────────────────┤
│ 6. Pandas → Arrow (Python)                              │
│    Convert result back to Arrow RecordBatch             │
├─────────────────────────────────────────────────────────┤
│ 7. Send Arrow bytes back to JVM                         │
│    (Zero-copy via shared memory or socket)              │
├─────────────────────────────────────────────────────────┤
│ 8. Arrow → Spark InternalRow (JVM)                      │
│    Reassemble rows for further processing               │
└─────────────────────────────────────────────────────────┘
```

**Performance Comparison:**

| UDF Type | Serialization | Throughput | Speedup |
|----------|---------------|------------|---------|
| **Python UDF (Pickle)** | Row-by-row Pickle | 10 MB/s | 1× |
| **Pandas UDF (Arrow)** | Columnar Arrow | 100-500 MB/s | **10-50×** |
| **Scala UDF** | None (JVM native) | 1000 MB/s | 100× |

**2. toPandas() / createDataFrame():**
```python
# Spark → Arrow → Pandas
pandas_df = spark_df.toPandas()  # Uses Arrow for efficient transfer

# Pandas → Arrow → Spark
spark_df = spark.createDataFrame(pandas_df)  # Uses Arrow for efficient transfer
```

**Without Arrow (Legacy):**
- **Spark → Pandas:** Pickle serialization (~10 MB/s)
- **Pandas → Spark:** Row-by-row conversion (~20 MB/s)

**With Arrow (Spark 2.3+):**
- **Spark → Pandas:** Arrow columnar transfer (~500 MB/s)
- **Pandas → Spark:** Arrow columnar transfer (~500 MB/s)
- **Speedup: 25-50×**

### 6️⃣ Zero-Copy Optimization

**Key Insight:** Arrow enables zero-copy when data is already in Arrow format.

**Scenario 1: DataFusion reads Parquet → Arrow (zero-copy)**
```
Parquet File (columnar on disk)
    ↓ mmap() or read()
Arrow RecordBatch (columnar in memory)
    ↓ Zero-copy! (same format)
DataFusion processes directly
```

**Scenario 2: Spark reads Parquet → InternalRow (copy required)**
```
Parquet File (columnar on disk)
    ↓ Decompress + decode
Temporary columnar buffers
    ↓ Transpose (COPY!)
Spark InternalRow (row-oriented in memory)
    ↓ Process
```

**Scenario 3: Spark → Arrow → DataFusion**
```
Spark InternalRow
    ↓ Convert to Arrow (COPY!)
Arrow RecordBatch
    ↓ Share via IPC (zero-copy!)
DataFusion processes
    ↓ Return result Arrow RecordBatch
    ↓ Convert back to InternalRow (COPY!)
Spark continues
```

**Cost Summary:**
- **Spark → Arrow:** 1 copy (transposition)
- **Arrow IPC transfer:** 0 copies (shared memory or mmap)
- **Arrow → Spark:** 1 copy (transposition)
- **Total: 2 copies per round-trip**

### 7️⃣ Optimizing Conversions

**Strategy 1: Batch Size**
```python
# Bad: Many small batches (high conversion overhead)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")

# Good: Larger batches (amortize conversion cost)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

**Strategy 2: Columnar Operations**
```python
# Bad: Forces row-at-a-time conversion
@pandas_udf("int")
def slow_udf(x):
    return x.apply(lambda v: complex_operation(v))  # Row-at-a-time

# Good: Vectorized operations (stays columnar)
@pandas_udf("int")
def fast_udf(x):
    return x * 2  # Vectorized (NumPy/Pandas optimized)
```

**Strategy 3: Avoid Round-Trips**
```python
# Bad: Multiple Spark ↔ Python conversions
df1 = spark_df.toPandas()
df2 = process1(df1)
spark_df2 = spark.createDataFrame(df2)
df3 = spark_df2.toPandas()
df4 = process2(df3)

# Good: Stay in one environment
df1 = spark_df.toPandas()
df2 = process1(df1)
df3 = process2(df2)
spark_df2 = spark.createDataFrame(df3)
```

### 8️⃣ Real-World Performance Analysis

**Example: 10M Row Dataset (3 columns: Int32, String, Float64)**

**Conversion Costs:**

| Operation | Time | Throughput | Bottleneck |
|-----------|------|------------|------------|
| **Spark InternalRow iteration** | 20 ms | 15 GB/s | Memory bandwidth |
| **Spark → Arrow conversion** | 200 ms | 1.5 GB/s | Transposition |
| **Arrow columnar scan** | 10 ms | 30 GB/s | Memory bandwidth |
| **Arrow → Spark conversion** | 200 ms | 1.5 GB/s | Transposition |

**Total Overhead:** 400ms for conversion, 30ms for actual processing

**When Conversion is Worth It:**
- If subsequent operations benefit from columnar format (aggregations, scans)
- If sharing data with non-JVM systems (Python, Rust)
- If using vectorized operations (SIMD)

**When to Avoid Conversion:**
- Simple row-at-a-time operations (filtering, projections)
- Small datasets (< 1000 rows)
- Operations staying in JVM (Scala UDFs)

### 9️⃣ Arrow Flight: Efficient Network Transfer

**Without Arrow Flight:**
```
Spark Driver                Spark Executor
    │                            │
    │  Serialize to bytes        │
    │  (Protocol Buffers/Pickle) │
    ├───────────────────────────>│
    │                            │
    │  Network transfer (slow)   │
    │                            │
    │  Deserialize bytes         │
    │                            │
```

**With Arrow Flight (gRPC + Arrow IPC):**
```
System A (Spark/DataFusion)    System B (DataFusion/Python)
    │                               │
    │  Arrow RecordBatch            │
    │  (already columnar)           │
    ├──────────────────────────────>│
    │  Send via Arrow Flight        │
    │  (zero-copy IPC)              │
    │                               │
    │  No deserialization needed!   │
    │  Direct memory mapping        │
```

**Performance:**
- **Traditional RPC:** ~100 MB/s (serialization bottleneck)
- **Arrow Flight:** ~10 GB/s (network bottleneck, not CPU)
- **Speedup: 100×**

### 🔟 Code Example: Spark → Arrow Conversion

**Scala (Spark side):**
```scala
import org.apache.arrow.vector._
import org.apache.spark.sql.catalyst.InternalRow

def convertPartitionToArrow(
    iterator: Iterator[InternalRow],
    schema: StructType
): RecordBatch = {
  val allocator = new RootAllocator(Long.MaxValue)
  val arrowSchema = ArrowUtils.toArrowSchema(schema)
  val root = VectorSchemaRoot.create(arrowSchema, allocator)

  val vectors = root.getFieldVectors
  var rowCount = 0

  iterator.foreach { row =>
    // For each column, extract value from row and append to vector
    (0 until schema.length).foreach { colIndex =>
      val vector = vectors.get(colIndex)

      if (row.isNullAt(colIndex)) {
        vector.setNull(rowCount)
      } else {
        schema.fields(colIndex).dataType match {
          case IntegerType =>
            vector.asInstanceOf[IntVector].setSafe(rowCount, row.getInt(colIndex))
          case StringType =>
            val str = row.getString(colIndex)
            vector.asInstanceOf[VarCharVector].setSafe(rowCount, str.getBytes)
          case DoubleType =>
            vector.asInstanceOf[Float8Vector].setSafe(rowCount, row.getDouble(colIndex))
          // ... handle other types
        }
      }
    }

    rowCount += 1
  }

  root.setRowCount(rowCount)
  root
}
```

**Rust (DataFusion side - receiving Arrow):**
```rust
use arrow::ipc::reader::StreamReader;
use datafusion::prelude::*;

// Read Arrow IPC stream (from Spark)
let reader = StreamReader::try_new(ipc_bytes)?;

for batch_result in reader {
    let batch = batch_result?;

    // Process directly as Arrow RecordBatch
    // No conversion needed!
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    // DataFusion operates natively on Arrow
    let result = df.select(vec![col("id") * lit(2)]).collect().await?;
}
```

### 🧩 TL;DR

**Conversion Process:**
- **Spark → Arrow:** Transpose rows to columns (O(rows × columns), ~200 MB/s)
- **Arrow → Spark:** Reassemble columns to rows (O(rows × columns), ~200 MB/s)

**Performance Impact:**
- **Pandas UDF with Arrow:** 10-50× faster than Pickle
- **toPandas() with Arrow:** 25-50× faster than legacy
- **Overhead:** 2 copies per round-trip (Spark ↔ Arrow ↔ Spark)

**When Conversion is Worth It:**
- ✅ Columnar operations (aggregations, scans)
- ✅ Python/Rust interop (Pandas UDFs, external systems)
- ✅ Network transfer (Arrow Flight)
- ✅ SIMD vectorization benefits

**When to Avoid:**
- ❌ Simple row operations (filtering, projections in Scala)
- ❌ Small datasets (< 1000 rows)
- ❌ Staying in JVM (no Python/external systems)

**Key Optimization:**
Use larger batch sizes (`spark.sql.execution.arrow.maxRecordsPerBatch`) to amortize transposition cost.

**This Codebase Context:**
If building a Spark accelerator with DataFusion:
1. Accept Spark data via Arrow IPC (avoid InternalRow conversion)
2. Process natively in DataFusion (zero conversion)
3. Return results via Arrow IPC
4. Spark converts back to InternalRow (1 conversion vs 2)
5. **Result: 50% less conversion overhead + 2-3× faster processing**
