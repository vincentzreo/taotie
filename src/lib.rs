mod cli;
use std::{ops::Deref, process, thread};

use cli::{connect, describe, head, list, sql};
use crossbeam_channel as mpsc;
use reedline_repl_rs::CallBackMap;

pub use cli::ReplCommand;

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplCommand>,
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplCommand>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

pub type ReplCallbBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbBacks {
    let mut callbacks = CallBackMap::new();

    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("list".to_string(), list);
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
        let (tx, rx) = mpsc::unbounded();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    println!("!!! cmd: {:?}", cmd);
                }
            })
            .unwrap();
        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        if let Err(e) = self.tx.send(cmd) {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}
