use clap::{ArgMatches, Parser};

use crate::{CmdExector, ReplContext, ReplDisplay, ReplMsg};

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(help = "Name of the dataset")]
    pub name: String,
}

pub fn describe(
    args: ArgMatches,
    ctx: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("export name")
        .to_owned();
    let (msg, rx) = ReplMsg::new(DescribeOpts::new(name));
    Ok(ctx.send(msg, rx))
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl CmdExector for DescribeOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;

        df.display().await
    }
}
