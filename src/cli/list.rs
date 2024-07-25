use clap::{ArgMatches, Parser};

use crate::{CmdExector, ReplContext, ReplDisplay, ReplMsg};

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    let (msg, rx) = ReplMsg::new(ListOpts);
    Ok(ctx.send(msg, rx))
}

impl CmdExector for ListOpts {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;

        df.display().await
    }
}
