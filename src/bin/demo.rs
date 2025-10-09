use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use df_regexp_extract::make_regexp_extract_udf;
use std::sync::Arc;
use tokio;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let udf = make_regexp_extract_udf();
    ctx.register_udf(udf.clone());

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("abc-123"),
            Some("zzz"),
            Some("foo-42"),
            None,
        ])) as _],
    )?;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col;
    use datafusion::prelude::lit;

    ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;

    let df = ctx.table("t").await?.select(vec![
        col("s"),
        udf.call(vec![col("s"), lit("(\\w+)-(\\d+)"), lit(1_i32)])
            .alias("g1"),
        udf.call(vec![col("s"), lit("(\\w+)-(\\d+)"), lit(0_i32)])
            .alias("full"),
    ])?;

    let results: Vec<datafusion::arrow::record_batch::RecordBatch> = df.collect().await?;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    let formatted = pretty_format_batches(&results)?;
    println!("{formatted}");
    Ok(())
}
