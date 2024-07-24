use clap::ArgMatches;

use crate::ReplContext;

use super::ReplCommand;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    ctx.send(ReplCommand::List);
    Ok(None)
}
