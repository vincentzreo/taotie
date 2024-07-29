use std::{ops::Deref, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::pretty_format_batches,
};
use datafusion::{
    functions_aggregate::{count::count, expr_fn::avg, median::median, stddev::stddev, sum::sum},
    prelude::{
        case, col, is_null, lit, max, min, CsvReadOptions, DataFrame, NdJsonReadOptions,
        SessionConfig, SessionContext,
    },
};

use crate::{
    cli::{ConnectOpts, DatasetConn},
    Backend, ReplDisplay,
};

pub struct DataFusionBackend(SessionContext);

impl Backend for DataFusionBackend {
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
    async fn list(&self) -> anyhow::Result<impl ReplDisplay> {
        let sql = "select table_name, table_type from information_schema.tables where table_schema = 'public'";
        let df = self.sql(sql).await?;
        Ok(df)
    }
    async fn describe(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self
            .0
            .sql(format!("select * from {}", name).as_str())
            .await?;
        let ddf = DescribeDataFrame::new(df);
        let describe_record_batch = ddf.to_record_batch().await?;
        Ok(describe_record_batch)
    }
    async fn schema(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(format!("DESCRIBE {}", name).as_str()).await?;
        Ok(df)
    }
    async fn head(&self, name: &str, n: usize) -> anyhow::Result<impl ReplDisplay> {
        let df = self
            .0
            .sql(format!("SELECT * FROM {} LIMIT {}", name, n).as_str())
            .await?;
        Ok(df)
    }
    async fn sql(&self, query: &str) -> anyhow::Result<impl ReplDisplay> {
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

impl ReplDisplay for RecordBatch {
    async fn display(self) -> anyhow::Result<String> {
        let data = pretty_format_batches(&[self])?;
        Ok(data.to_string())
    }
}

struct DescribeDataFrame {
    df: DataFrame,
    functions: &'static [&'static str],
    scheme: SchemaRef,
}

impl DescribeDataFrame {
    fn new(df: DataFrame) -> Self {
        let original_schema_fields = df.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));
        Self {
            df,
            functions: &["count", "null_count", "mean", "std", "min", "max", "median"],
            scheme: Arc::new(Schema::new(describe_schemas)),
        }
    }
    fn count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        // count aggregation
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn null_count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn mean(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn stddev(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| stddev(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn min(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| min(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn max(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| max(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn median(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| median(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    async fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        let original_schema_fields = self.df.schema().fields().iter();
        // first column with function names
        let mut describe_col_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            self.functions
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>(),
        ))];
        let batchs = vec![
            // count aggregation
            self.count(),
            // null_count aggregation
            self.null_count(),
            // mean aggregation
            self.mean(),
            // std aggregation
            self.stddev(),
            // min aggregation
            self.min(),
            // max aggregation
            self.max(),
            // median aggregation
            self.median(),
        ];
        for field in original_schema_fields {
            let mut array_data = vec![];
            for result in batchs.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_data.push(array_ref);
            }
            describe_col_vec.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }
        let batch = RecordBatch::try_new(self.scheme.clone(), describe_col_vec)?;

        Ok(batch)
    }
}
