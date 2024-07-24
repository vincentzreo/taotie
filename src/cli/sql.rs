use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::ReplCommand;

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(short, long, help = "SQL query to run")]
    pub query: String,
}

impl From<SqlOpts> for ReplCommand {
    fn from(opts: SqlOpts) -> Self {
        ReplCommand::Sql(opts)
    }
}

impl SqlOpts {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    let query = args
        .get_one::<String>("query")
        .expect("export query")
        .to_owned();
    let cmd = SqlOpts::new(query).into();
    ctx.send(cmd);
    Ok(None)
}
