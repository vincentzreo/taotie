use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::ReplCommand;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(short, long, help = "Name of the dataset")]
    pub name: String,

    #[arg(short, long, help = "Number of rows to show")]
    pub n: Option<usize>,
}

impl From<HeadOpts> for ReplCommand {
    fn from(opts: HeadOpts) -> Self {
        ReplCommand::Head(opts)
    }
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
    let cmd = HeadOpts::new(name, n).into();
    ctx.send(cmd);
    Ok(None)
}
