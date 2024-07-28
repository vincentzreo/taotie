use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

use crate::{CmdExector, ReplContext, ReplMsg};

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(FileOpts),
    Parquet(String),
    NdJson(FileOpts),
}

#[derive(Debug, Clone)]
pub struct FileOpts {
    pub filename: String,
    pub ext: String,
    pub compression: FileCompressionType,
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = verify_conn_str, help = "Connection string to the dataset, could be postgres or local file(csv, parquet, json)")]
    pub conn: DatasetConn,

    #[arg(short, long, help = "if database, the name of the table")]
    pub table: Option<String>,

    #[arg(short, long, help = "Name of the dataset")]
    pub name: String,
}

impl ConnectOpts {
    pub fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}
pub fn connect(
    args: ArgMatches,
    ctx: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("export conn")
        .to_owned();
    let table = args.get_one::<String>("table").map(|t| t.to_owned());
    let name = args
        .get_one::<String>("name")
        .expect("export name")
        .to_owned();
    let (msg, rx) = ReplMsg::new(ConnectOpts::new(conn, table, name));
    Ok(ctx.send(msg, rx))
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    let conn_str = s.to_string();
    if s.starts_with("postgres://") {
        return Ok(DatasetConn::Postgres(conn_str));
    }
    if s.ends_with(".parquet") {
        return Ok(DatasetConn::Parquet(s.to_string()));
    }

    let exts = conn_str.split('.').rev().collect::<Vec<_>>();
    let len = exts.len();
    let mut exts = exts.into_iter().take(len - 1);
    let ext1 = exts.next();
    let ext2 = exts.next();
    match (ext1, ext2) {
        (Some(ext1), Some(ext2)) => {
            let compression = match ext1 {
                "gz" => FileCompressionType::GZIP,
                "bz2" => FileCompressionType::BZIP2,
                "xz" => FileCompressionType::XZ,
                "zst" => FileCompressionType::ZSTD,
                v => return Err(format!("Unsupported compression type: {}", v)),
            };
            let opts = FileOpts {
                filename: s.to_string(),
                ext: ext2.to_string(),
                compression,
            };
            match ext2 {
                "csv" => Ok(DatasetConn::Csv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DatasetConn::NdJson(opts)),
                v => Err(format!("Unsupported file type: {}", v)),
            }
        }
        (Some(ext1), None) => {
            let opts = FileOpts {
                filename: s.to_string(),
                ext: ext1.to_string(),
                compression: FileCompressionType::UNCOMPRESSED,
            };
            match ext1 {
                "csv" => Ok(DatasetConn::Csv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DatasetConn::NdJson(opts)),
                v => Err(format!("Unsupported file extension: {}", v)),
            }
        }
        _ => Err(format!("Invalid file path: {}", conn_str)),
    }
}

impl CmdExector for ConnectOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;

        Ok(format!("Connected to dataset: {}", self.name))
    }
}
