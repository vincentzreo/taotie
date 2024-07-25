use clap::{ArgMatches, Parser};

use crate::{CmdExector, ReplContext, ReplDisplay, ReplMsg};

#[derive(Debug, Parser)]
pub struct SchemaOpts {
    #[arg(help = "Name of the dataset")]
    pub name: String,
}

pub fn schema(args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("export name")
        .to_owned();
    let (msg, rx) = ReplMsg::new(SchemaOpts::new(name));
    Ok(ctx.send(msg, rx))
}

impl SchemaOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl CmdExector for SchemaOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.schema(&self.name).await?;

        df.display().await
    }
}
