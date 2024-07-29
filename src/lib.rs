mod backend;
mod cli;
use std::{ops::Deref, process, thread};

use backend::DataFusionBackend;
use cli::{connect, describe, head, list, schema, sql, ConnectOpts};
use cli::{DescribeOpts, HeadOpts, ListOpts, SchemaOpts, SqlOpts};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;

pub use cli::ReplCommand;
use tokio::runtime::Runtime;

#[enum_dispatch]
trait CmdExector {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

trait Backend {
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<impl ReplDisplay>;
    async fn schema(&self, name: &str) -> anyhow::Result<impl ReplDisplay>;
    async fn describe(&self, name: &str) -> anyhow::Result<impl ReplDisplay>;
    async fn head(&self, name: &str, n: usize) -> anyhow::Result<impl ReplDisplay>;
    async fn sql(&self, query: &str) -> anyhow::Result<impl ReplDisplay>;
}

trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    cmd: ReplCommand,
    tx: oneshot::Sender<String>,
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

pub type ReplCallbBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbBacks {
    let mut callbacks = CallBackMap::new();

    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("list".to_string(), list);
    callbacks.insert("schema".to_string(), schema);
    callbacks.insert("describe".to_string(), describe);
    callbacks.insert("head".to_string(), head);
    callbacks.insert("sql".to_string(), sql);

    callbacks
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();

        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        let mut backend = DataFusionBackend::new();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        let ret = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(ret)?;
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                    }
                }
            })
            .unwrap();
        Self { tx }
    }

    pub fn send(&self, msg: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(msg) {
            eprintln!("Repl Send Error: {}", e);
            process::exit(1);
        }
        rx.recv().ok()
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}
