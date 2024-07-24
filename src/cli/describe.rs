use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::ReplCommand;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(short, long, help = "Name of the dataset")]
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
    let cmd = DescribeOpts::new(name).into();
    ctx.send(cmd);
    Ok(None)
}

impl From<DescribeOpts> for ReplCommand {
    fn from(opts: DescribeOpts) -> Self {
        ReplCommand::Describe(opts)
    }
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
