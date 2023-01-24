use std::{collections::HashMap, sync::Arc, time::Duration};

use clap::{ArgMatches, Command, Parser};
use cmd::{
    lab3_back_cmds::{app_commands, repl},
    lab3_run,
};
use log::{info, LevelFilter};
use tokio::{join, sync::mpsc::Sender, task::JoinHandle};
use tribbler::{
    addr,
    config::{Config, DEFAULT_CONFIG_LOCATION},
    err::{TribResult, TribblerError},
};

#[derive(Parser, Debug)]
#[clap(name = "lab3-keep")]
struct Options {
    /// log level to use when starting the backends
    #[clap(short, long, default_value = "INFO")]
    log_level: LevelFilter,
    /// bin configuration file
    #[clap(short, long, default_value = DEFAULT_CONFIG_LOCATION)]
    config: String,
    /// addresses to send ready notifications to
    #[clap(short, long)]
    ready_addrs: Vec<String>,

    #[clap(long, default_value = "10")]
    recv_timeout: u64,
}

#[allow(unused_variables)]
#[tokio::main]
async fn main() -> TribResult<()> {
    let args = Options::parse();
    let config = Arc::new(Config::read(Some(&args.config))?);
    let _ = env_logger::builder()
        .default_format()
        .filter_level(args.log_level)
        .try_init();
    let pt = lab3_run::ProcessType::Keep;
    let mut map: HashMap<String, (Sender<()>, JoinHandle<()>)> = HashMap::new();
    // automatically start one backend
    if let Ok((shutdown, handle)) = start(config.backs[0].clone(), config.clone()).await {
        map.insert(config.backs[0].clone(), (shutdown, handle));
    }

    let app = Command::new("lab3-back").subcommands(app_commands());

    loop {
        match repl(&app) {
            Ok(subcmd) => match match_cmds(subcmd.subcommand(), config.clone(), &mut map).await {
                true => continue,
                false => break,
            },
            Err(_) => continue,
        }
    }
    Ok(())
}

pub async fn match_cmds(
    subcmd: Option<(&str, &ArgMatches)>,
    config: Arc<Config>,
    map: &mut HashMap<String, (Sender<()>, JoinHandle<()>)>,
) -> bool {
    match subcmd {
        Some(("start", v)) => {
            // call start with the address from cmdline
            let addr = v.get_one::<String>("backend").unwrap();
            // append to handle / shutdown vectors
            if let Ok((shutdown, handle)) = start(addr.to_string(), config).await {
                map.insert(addr.to_string(), (shutdown, handle));
            }
            true
        }
        Some(("stop", v)) => {
            // call kill with the address from cmdline
            let addr = v.get_one::<String>("backend").unwrap();
            // and match it to the correct handle / shutdown
            if map.contains_key(addr) {
                let (shutdown, handle) = map.remove(addr).unwrap();
                let _ = kill(shutdown, handle).await;
            }
            true
        }
        Some (("start-n", v)) => {
            let num = v.get_one::<String>("num backend").unwrap();
            let num = num.parse::<usize>().unwrap();
            let _ = start_n(num, config, map).await;
            true
        }
        Some (("stop-n", v)) => {
            let num = v.get_one::<String>("num backend").unwrap();
            let num = num.parse::<usize>().unwrap();
            let _ = stop_n(num, map).await;
            true
        }
        Some(("exit", _)) => false,
        _other => {
            println!("Command not found");
            true
        }
    }
}

// Assuming signals are implemented correctly - otherwise, this will hang
pub async fn kill(
    shutdown: tokio::sync::mpsc::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
) -> TribResult<()> {
    shutdown.send(()).await?;
    let result = join!(handle);
    assert!(result.0.is_ok());

    Ok(())
}

// start a back on addr if it's not already running
pub async fn start(
    addr: String,
    config: Arc<Config>,
) -> TribResult<(tokio::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>)> {
    let pt = lab3_run::ProcessType::Back;
    let (shutdown, rx) = tokio::sync::mpsc::channel(1);
    let (tx, rdy) = std::sync::mpsc::channel();

    // back index in the config
    let idx = config
        .backs
        .iter()
        .position(|x| *x == addr)
        .unwrap_or(usize::MAX);

    if idx == usize::MAX {
        log::error!("Backend {} not found in config", addr);
        return Err(Box::new(TribblerError::Unknown(
            "Backend not found in config".to_string(),
        )));
    }

    if addr::check(&addr)? {
        let handle = tokio::spawn(lab3_run::run_srv(
            pt,
            idx,
            config.clone(),
            Some(tx.clone()),
            Some(rx),
        ));
        if rdy.recv_timeout(Duration::from_secs(3)).is_err() {
            info!(
                "Failed to start {}: timeout during wait for ready signal",
                addr
            );
        }
        Ok((shutdown, handle))
    } else {
        log::error!("Backend {} already running", addr);
        return Err(Box::new(TribblerError::Unknown(format!(
            "{} is already running",
            addr
        ))));
    }
}

// start num backends
pub async fn start_n(
    num: usize,
    config: Arc<Config>,
    map: &mut HashMap<String, (Sender<()>, JoinHandle<()>)>,
) -> TribResult<()> {
    let avail_backends = config.backs.clone().into_iter().filter(|item| !map.contains_key(item)).collect::<Vec<_>>();

    if num > avail_backends.len() {
        log::error!("Not enough backends available to start {}", num);
        return Ok(());
    }

    for addr in avail_backends.into_iter().take(num) {
        if let Ok((shutdown, handle)) = start(addr.clone(), config.clone()).await {
            map.insert(addr.clone(), (shutdown.clone(), handle));
        }
    }

    Ok(())
}

// stop num backends
pub async fn stop_n(
    num: usize,
    map: &mut HashMap<String, (Sender<()>, JoinHandle<()>)>,
) -> TribResult<()> {
    let backends = map.keys().cloned().collect::<Vec<_>>();

    if num > backends.len() {
        log::error!("Not enough backends to stop {}", num);
        return Ok(());
    }

    for addr in backends.into_iter().take(num) {
        if map.contains_key(&addr) {
            let (shutdown, handle) = map.remove(&addr).unwrap();
            let _ = kill(shutdown, handle).await;
        }
    }

    Ok(())
}