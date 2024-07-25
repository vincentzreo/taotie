use clap::{ArgMatches, Parser};

use crate::{CmdExector, ReplContext, ReplDisplay, ReplMsg};

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(help = "Name of the dataset")]
    pub name: String,

    #[arg(short, long, help = "Number of rows to show")]
    pub n: Option<usize>,
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

pub fn head(args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("export name")
        .to_owned();
    let n = args.get_one::<usize>("n").copied();
    let (msg, rx) = ReplMsg::new(HeadOpts::new(name, n));
    Ok(ctx.send(msg, rx))
}

impl CmdExector for HeadOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(&self.name, self.n.unwrap_or(5)).await?;

        df.display().await
    }
}
