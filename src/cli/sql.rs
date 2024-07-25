use clap::{ArgMatches, Parser};

use crate::{CmdExector, ReplContext, ReplDisplay, ReplMsg};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(help = "SQL query to run")]
    pub query: String,
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
    let (msg, rx) = ReplMsg::new(SqlOpts::new(query));
    Ok(ctx.send(msg, rx))
}

impl CmdExector for SqlOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.query).await?;

        df.display().await
    }
}
