use std::fs::File;

use anyhow::Result;
use arrow::array::AsArray;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::{prelude::*, sql::SQLContext};

const PQ_FILE: &str = "assets/sample.parquet";

#[tokio::main]
async fn main() -> Result<()> {
    read_with_parquet(PQ_FILE)?;
    read_with_datafusion(PQ_FILE).await?;
    read_with_polars(PQ_FILE)?;
    Ok(())
}

fn read_with_parquet(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;
    for record_batch in reader {
        let record_batch = record_batch?;
        let emails = record_batch.column(0).as_string::<i32>();
        for email in emails {
            println!("{:?}", email);
        }
    }
    Ok(())
}

async fn read_with_datafusion(file: &str) -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("stats", file, ParquetReadOptions::default())
        .await?;
    let ret = ctx
        .sql("SELECT email::text email, name::text name FROM stats limit 3")
        .await?
        .collect()
        .await?;
    for batch in ret {
        let emails = batch.column(0).as_string::<i32>();
        let names = batch.column(1).as_string::<i32>();

        for (email, name) in emails.iter().zip(names.iter()) {
            println!("{:?} {:?}", email, name);
        }
    }
    Ok(())
}

fn read_with_polars(file: &str) -> Result<()> {
    /* let mut file = File::open(file)?;
    let df = ParquetReader::new(&mut file).finish()?;
    println!("{:?}", df); */

    let df = LazyFrame::scan_parquet(file, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("stats", df);
    let df = ctx.execute("SELECT email, name FROM stats")?.collect()?;
    println!("{:?}", df);
    Ok(())
}
