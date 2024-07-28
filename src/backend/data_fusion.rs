use std::ops::Deref;

use anyhow::Ok;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, SessionConfig, SessionContext};

use crate::{
    cli::{ConnectOpts, DatasetConn},
    Backend, ReplDisplay,
};

pub struct DataFusionBackend(SessionContext);

impl Backend for DataFusionBackend {
    type DataFrame = datafusion::dataframe::DataFrame;
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_conn_str) => {
                println!("Postgres connection is not supported yet");
            }
            DatasetConn::Csv(file_opts) => {
                let csv_opts = CsvReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_csv(&opts.name, &file_opts.filename, csv_opts)
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(file_opts) => {
                let json_opts = NdJsonReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_json(&opts.name, &file_opts.filename, json_opts)
                    .await?;
            }
        }
        Ok(())
    }
    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let sql = "select table_name, table_type from information_schema.tables where table_schema = 'public'";
        let df = self.sql(sql).await?;
        Ok(df)
    }
    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql(format!("select * from {}", name).as_str())
            .await?;
        let df = df.describe().await?;
        Ok(df)
    }
    async fn schema(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(format!("DESCRIBE {}", name).as_str()).await?;
        Ok(df)
    }
    async fn head(&self, name: &str, n: usize) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql(format!("SELECT * FROM {} LIMIT {}", name, n).as_str())
            .await?;
        Ok(df)
    }
    async fn sql(&self, query: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(query).await?;
        Ok(df)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        Self(SessionContext::new_with_config(config))
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let barches = self.collect().await?;
        let data = pretty_format_batches(&barches)?;
        Ok(data.to_string())
    }
}
