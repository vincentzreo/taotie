use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::ReplCommand;

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(String),
    Parquet(String),
    Json(String),
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

impl From<ConnectOpts> for ReplCommand {
    fn from(opts: ConnectOpts) -> Self {
        ReplCommand::Connect(opts)
    }
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
    let cmd = ConnectOpts::new(conn, table, name).into();
    ctx.send(cmd);
    Ok(None)
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    if s.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(s.to_string()))
    } else if s.ends_with(".csv") {
        Ok(DatasetConn::Csv(s.to_string()))
    } else if s.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(s.to_string()))
    } else if s.ends_with(".json") {
        Ok(DatasetConn::Json(s.to_string()))
    } else {
        Err("Invalid connection string".to_string())
    }
}
