use std::sync::Arc;

use lab::{self, lab1, lab2};
use log::info;

use tribbler::config::Config;
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

// Copied & Modified from cmd/bins_run.rs
#[derive(Debug, Clone)]
pub enum ProcessType {
    Back,
    Keep,
}

#[allow(unused_must_use)]
pub async fn run_srv(
    t: ProcessType,
    idx: usize,
    config: Arc<Config>,
    tx: Option<std::sync::mpsc::Sender<bool>>,
    shutdown: Option<tokio::sync::mpsc::Receiver<()>>,
) {
    match t {
        ProcessType::Back => {
            let cfg = config.back_config(idx, Box::new(MemStorage::default()), tx, shutdown);
            info!("starting backend on {}", cfg.addr);
            lab1::serve_back(cfg).await;
        }
        ProcessType::Keep => {
            let cfg = config.keeper_config(idx, tx, shutdown).unwrap();
            info!("starting keeper on {}", cfg.addr());
            lab2::serve_keeper(cfg).await;
        }
    };
}
