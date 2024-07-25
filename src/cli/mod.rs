mod connect;
mod describe;
mod head;
mod list;
mod schema;
mod sql;

use enum_dispatch::enum_dispatch;

pub use connect::{connect, ConnectOpts, DatasetConn};
pub use describe::{describe, DescribeOpts};
pub use head::{head, HeadOpts};
pub use list::{list, ListOpts};
pub use schema::{schema, SchemaOpts};
pub use sql::{sql, SqlOpts};

use clap::Parser;

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExector)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "schema", about = "Describe the schema of dataset")]
    Schema(SchemaOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "Show the first few rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "Run a SQL query on a dataset")]
    Sql(SqlOpts),
}
