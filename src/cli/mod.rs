mod connect;
mod describe;
mod head;
mod list;
mod sql;

use describe::DescribeOpts;
use head::HeadOpts;

pub use connect::connect;
pub use describe::describe;
pub use head::head;
pub use list::list;
pub use sql::sql;

use clap::Parser;
use connect::ConnectOpts;
use sql::SqlOpts;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List,
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "Show the first few rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "Run a SQL query on a dataset")]
    Sql(SqlOpts),
}
