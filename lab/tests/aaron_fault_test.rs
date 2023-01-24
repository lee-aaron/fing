use std::{collections::HashMap, sync::Arc, time::Duration};

use lab::{lab1, lab2};
use log::{info, LevelFilter};
use tokio::{join, sync::mpsc::Sender, task::JoinHandle};
use tribbler::{addr, config::Config, err::TribResult, storage::MemStorage, trib::Server};

// set up num backends and update the map of live backends
async fn setup_back(
    num: usize,
    config: Arc<Config>,
    map: &mut HashMap<String, (Sender<()>, JoinHandle<()>)>,
) -> TribResult<()> {
    let avail_backends = config
        .backs
        .clone()
        .into_iter()
        .filter(|item| !map.contains_key(item))
        .collect::<Vec<_>>();

    if num > avail_backends.len() {
        return Err("Not enough backends available".into());
    }

    for addr in avail_backends.into_iter().take(num) {
        if let Ok((shutdown, handle)) = start(addr.clone(), config.clone()).await {
            map.insert(addr.clone(), (shutdown.clone(), handle));
        }
    }

    Ok(())
}

// set up num keepers and update the map of live keepers
async fn setup_keeper(
    num: usize,
    config: Arc<Config>,
    map: &mut HashMap<String, (Sender<()>, JoinHandle<()>)>,
) -> TribResult<()> {
    let avail_keeper = config
        .keepers
        .clone()
        .into_iter()
        .filter(|item| !map.contains_key(item))
        .collect::<Vec<_>>();

    if num > avail_keeper.len() {
        log::error!("Not enough keepers available to start {}", num);
        return Ok(());
    }

    for addr in avail_keeper.into_iter().take(num) {
        if let Ok((shutdown, handle)) = start_keep(addr.clone(), config.clone()).await {
            map.insert(addr.clone(), (shutdown.clone(), handle));
        }
    }
    Ok(())
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

// start a keeper on addr if it's not already running
pub async fn start_keep(
    addr: String,
    config: Arc<Config>,
) -> TribResult<(tokio::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>)> {
    let pt = ProcessType::Keep;
    let (shutdown, rx) = tokio::sync::mpsc::channel(1);
    let (tx, rdy) = std::sync::mpsc::channel();

    // keeper index in the config
    let idx = config
        .keepers
        .iter()
        .position(|x| *x == addr)
        .unwrap_or(usize::MAX);

    if idx == usize::MAX {
        log::error!("Keeper {} not found in config", addr);
        return Err("Keeper not found in config".into());
    }

    if addr::check(&addr)? {
        let handle = tokio::spawn(run_srv(pt, idx, config.clone(), Some(tx.clone()), Some(rx)));
        if rdy.recv_timeout(Duration::from_secs(3)).is_err() {
            info!(
                "Failed to start {}: timeout during wait for ready signal",
                addr
            );
        }
        Ok((shutdown, handle))
    } else {
        log::error!("Keeper {} already running", addr);
        return Err(format!("{} is already running", addr).into());
    }
}

// start a back on addr if it's not already running
pub async fn start(
    addr: String,
    config: Arc<Config>,
) -> TribResult<(tokio::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>)> {
    let pt = ProcessType::Back;
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
        return Err("Backend not found in config".into());
    }

    if addr::check(&addr)? {
        let handle = tokio::spawn(run_srv(pt, idx, config.clone(), Some(tx.clone()), Some(rx)));
        if rdy.recv_timeout(Duration::from_secs(3)).is_err() {
            info!(
                "Failed to start {}: timeout during wait for ready signal",
                addr
            );
        }
        Ok((shutdown, handle))
    } else {
        log::error!("Backend {} already running", addr);
        return Err(format!("{} is already running", addr).into());
    }
}

// Copied & Modified from cmd/bins_run.rs
#[derive(Debug, Clone)]
pub enum ProcessType {
    Back,
    Keep,
}

#[allow(unused_must_use)]
async fn run_srv(
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

// Copied & modified from cmd/trib_front.rs
async fn setup_front(
    num_front: u32,
    config: Arc<Config>,
) -> TribResult<Vec<Box<dyn Server + Send + Sync>>> {
    let mut servers = Vec::new();
    for _ in 0..num_front {
        let bc = lab2::new_bin_client(config.backs.clone()).await?;
        let server = lab2::new_front(bc).await?;
        servers.push(server);
    }

    Ok(servers)
}

async fn setup(
    num_back: usize,
    num_keeper: usize,
    config: Arc<Config>,
) -> TribResult<HashMap<String, (tokio::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>)>> {
    let _ = env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .try_init();

    let mut map = HashMap::new();

    let _ = setup_back(num_back, config.clone(), &mut map).await?;
    let _ = setup_keeper(num_keeper, config.clone(), &mut map).await?;

    Ok(map)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_node_restart() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // 34003 - 34006 should be online
    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    // test insert a, stop 34006, start 34006, get a
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    // keeper is migrating 34006 so front should return from 34007
    // get a
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_backend_keeper_down() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 2, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // test insert a, stop 34006, start 34006, get a
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;
    let _ = map.get_mut("127.0.0.1:34008").unwrap().0.send(()).await;

    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    // keeper is migrating 34006 so front should return from 34007
    // get a
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_node_events() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 2, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // test insert a, stop 34006, start 34006, get a
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    // keeper is migrating 34006 so front should return from 34007
    // get a
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_node_leave() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34001".to_string(),
            "127.0.0.1:34002".to_string(),
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec![
            "127.0.0.1:34008".to_string(),
            "127.0.0.1:34009".to_string(),
            "127.0.0.1:34010".to_string(),
        ],
    });

    let mut map = setup(4, 2, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // start 34005
    if let Ok((shutdown, handle)) = start("127.0.0.1:34005".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34005".to_string(), (shutdown.clone(), handle));
    }
    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    // test insert a, stop 34006, start 34006, get a
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;
    // stop 34001
    let _ = map.get_mut("127.0.0.1:34001").unwrap().0.send(()).await;
    // stop 34002
    let _ = map.get_mut("127.0.0.1:34002").unwrap().0.send(()).await;
    // stop 34003
    let _ = map.get_mut("127.0.0.1:34003").unwrap().0.send(()).await;

    // should return from 34004
    // get a
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec![
            "127.0.0.1:34008".to_string(),
            "127.0.0.1:34009".to_string(),
            "127.0.0.1:34010".to_string(),
        ],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let mut fronts = setup_front(2, config.clone()).await?;
    let front = fronts.pop().unwrap();

    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    // start 34006
    if let Ok((shutdown, handle)) = start("127.0.0.1:34006".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34006".to_string(), (shutdown.clone(), handle));
    }

    // try concurrent follow
    let mut handles = vec![];
    let mut r = vec![];
    for _ in 0..5 {
        let backs = config.backs.clone();
        let jh = tokio::spawn(async move {
            let front = lab2::new_front(lab2::new_bin_client(backs).await?).await?;
            if let Err(e) = front.follow("alice", "bob").await {
                return Err(e);
            };
            Ok(())
        });
        handles.push(jh);
    }
    for handle in handles {
        let res = handle.await;
        assert!(res.is_ok());

        r.push(res?);
    }

    assert!(r.iter().filter(|&x| x.is_ok()).count() == 1);

    let res = front.following("alice").await?;
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follow() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
            "127.0.0.1:34007".to_string(),
        ],
        keepers: vec![
            "127.0.0.1:34008".to_string(),
            "127.0.0.1:34009".to_string(),
            "127.0.0.1:34010".to_string(),
        ],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let mut fronts = setup_front(2, config.clone()).await?;
    let front = fronts.pop().unwrap();

    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    // stop 34006
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = map.get_mut("127.0.0.1:34003").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = map.get_mut("127.0.0.1:34004").unwrap().0.send(()).await;

    // wait a few seconds
    tokio::time::sleep(Duration::from_secs(2)).await;

    // follow
    front.follow("alice", "bob").await?;

    Ok(())
}
