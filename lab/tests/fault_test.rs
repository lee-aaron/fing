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
async fn test_fail_after_write() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // Wait for system stablize
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Write content
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // Immediately stop server
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // Read before replication happens
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(4)).await;
    // Check again
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_after_fail() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // Wait for system stablize
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Stop server
    let _ = map.get_mut("127.0.0.1:34006").unwrap().0.send(()).await;

    // Immediately write content to stopped server
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // Read before replication happens
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(4)).await;
    // Check again
    let post = front.tribs("a").await?;
    assert_eq!(post.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow_with_fail() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
            "127.0.0.1:34006".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(4, 1, config.clone()).await?;
    let fronts = setup_front(6, config.clone()).await?;
    let front1 = fronts.get(0).unwrap();
    let front2 = fronts.get(1).unwrap();
    let front3 = fronts.get(2).unwrap();
    let front4 = fronts.get(3).unwrap();
    let front5 = fronts.get(4).unwrap();
    let front6 = fronts.get(5).unwrap();

    // Wait for system stablize
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Stop server
    let stop_handle = map.get_mut("127.0.0.1:34006").unwrap().0.send(());

    // And concurrent follows
    front1.sign_up("a").await?;
    front1.sign_up("b").await?;
    let (r1, r2, _) = join!(front1.follow("a", "b"), front2.follow("a","b"), stop_handle);
    let (r3, r4) = join!(front3.follow("a","b"), front4.follow("a", "b"));

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (r5, r6) = join!(front5.follow("a","b"), front6.follow("a", "b"));
    let result = [r1, r2, r3, r4, r5, r6];

    // Only one follow should succeed
    assert!(result.iter().filter(|r| r.is_ok()).count() <= 1);
    assert!(front1.is_following("a", "b").await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_below_three_server() -> TribResult<()> {
    let config = Arc::new(Config {
        backs: vec![
            "127.0.0.1:34003".to_string(),
            "127.0.0.1:34004".to_string(),
            "127.0.0.1:34005".to_string(),
        ],
        keepers: vec!["127.0.0.1:34008".to_string(), "127.0.0.1:34009".to_string()],
    });

    let mut map = setup(3, 1, config.clone()).await?;
    let fronts = setup_front(1, config.clone()).await?;
    let front = fronts.get(0).unwrap();

    // Wait for system stablize
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Write to first server
    front.sign_up("a").await?;
    front.post("a", "testing", 0).await?;

    // Stop server to make it below 2
    map.get_mut("127.0.0.1:34003").unwrap().0.send(()).await?;

    // Post a trib
    front.post("a", "testing2", 0).await?;

    // Wait for stable
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Write some more
    front.post("a", "testing3", 0).await?;

    let tribs = front.tribs("a").await?;
    assert!(tribs.len() == 3);

    // Shutdown one more
    map.get_mut("127.0.0.1:34005").unwrap().0.send(()).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;

    let tribs = front.tribs("a").await?;
    assert!(tribs.len() == 3);

    // Restart one server
    if let Ok((shutdown, handle)) = start("127.0.0.1:34003".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34003".to_string(), (shutdown.clone(), handle));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    let tribs = front.tribs("a").await?;
    assert!(tribs.len() == 3);

    // Restart one more
    if let Ok((shutdown, handle)) = start("127.0.0.1:34005".to_string(), config.clone()).await {
        map.insert("127.0.0.1:34005".to_string(), (shutdown.clone(), handle));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    let tribs = front.tribs("a").await?;
    assert!(tribs.len() == 3);

    Ok(())
}