use std::{
    sync::{
        mpsc::{self, Sender},
        Arc,
    },
    time::Duration,
};

use lab::{self, lab1, lab2};
use log::{info, warn, LevelFilter};

use tokio::{join, sync::Mutex};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};
use tribbler::{
    addr::{self},
    config::Config,
    trib::{Server, MIN_LIST_USER},
};

const CONFIG_PATH: &str = "../bins.json";
const RECV_TIMEOUT: u64 = 5;

// Copied & Modified from cmd/bins_run.rs
#[derive(Debug, Clone)]
pub enum ProcessType {
    Back,
    Keep,
}

pub async fn setup_backends(t: ProcessType) -> TribResult<()> {
    let config = Arc::new(Config::read(Some(&CONFIG_PATH))?);

    let (tx, rdy) = mpsc::channel();

    let mut handles = vec![];
    let it = match t {
        ProcessType::Back => &config.backs,
        ProcessType::Keep => &config.keepers,
    };
    for (i, srv) in it.iter().enumerate() {
        if addr::check(srv)? {
            handles.push(tokio::spawn(run_srv(
                t.clone(),
                i,
                config.clone(),
                Some(tx.clone()),
            )));
        }
    }
    let proc_name = match t {
        ProcessType::Back => "backend",
        ProcessType::Keep => "keeper",
    };
    if handles.is_empty() {
        warn!("no {}s found for this host", proc_name);
        return Ok(());
    }
    info!("Waiting for ready signal from {}...", proc_name);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ready = rdy.recv_timeout(Duration::from_secs(RECV_TIMEOUT))?;
    if !ready {
        panic!("failed to start")
    }
    info!("Ready to serve! {}", proc_name);
    Ok(())
}

#[allow(unused_must_use)]
async fn run_srv(t: ProcessType, idx: usize, config: Arc<Config>, tx: Option<Sender<bool>>) {
    match t {
        ProcessType::Back => {
            let cfg = config.back_config(idx, Box::new(MemStorage::default()), tx, None);
            info!("starting backend on {}", cfg.addr);
            lab1::serve_back(cfg).await;
        }
        ProcessType::Keep => {
            let cfg = config.keeper_config(idx, tx, None).unwrap();
            info!("starting keeper on {}", cfg.addr());
            lab2::serve_keeper(cfg).await;
        }
    };
}

// Copied & modified from cmd/trib_front.rs
async fn setup_front(num_front: u32) -> TribResult<Vec<Box<dyn Server + Send + Sync>>> {
    let config = Config::read(Some(&CONFIG_PATH))?;

    let mut servers = Vec::new();
    for _ in 0..num_front {
        let bc = lab2::new_bin_client(config.clone().backs).await?;
        let server = lab2::new_front(bc).await?;
        servers.push(server);
    }

    Ok(servers)
}

async fn setup(num_front: u32) -> TribResult<Vec<Box<dyn Server + Send + Sync>>> {
    let _ = env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .try_init();
    setup_backends(ProcessType::Back).await?;
    setup_backends(ProcessType::Keep).await?;
    Ok(setup_front(num_front).await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_signup() -> TribResult<()> {
    let num_front = 1;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    front_ends.get(0).unwrap().sign_up("harry").await?;

    let list_users = front_ends.get(0).unwrap().list_users().await?;
    assert_eq!(1, list_users.len());
    assert_eq!("harry", list_users.get(0).unwrap());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_signups() -> TribResult<()> {
    let num_front = 3;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let mut handles = vec![];
    for front in front_ends.iter() {
        let handle = front.sign_up("harry");
        handles.push(handle);
    }

    // Wait for front ends to finish
    for handle in handles.into_iter() {
        let _ = handle.await;
    }

    let list_users = front_ends.get(0).unwrap().list_users().await?;
    assert_eq!(1, list_users.len());
    assert_eq!("harry", list_users.get(0).unwrap());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_list_user_limit() -> TribResult<()> {
    let num_front = 1;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    for i in 0..MIN_LIST_USER + 5 {
        front_ends
            .get(0)
            .unwrap()
            .sign_up(&format!("user{}", i))
            .await?;
    }

    let list_users = front_ends.get(0).unwrap().list_users().await?;
    assert_eq!(MIN_LIST_USER, list_users.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follow() -> TribResult<()> {
    let num_front = 1;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let front = front_ends.get(0).unwrap();
    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    let list_users = front_ends.get(0).unwrap().list_users().await?;
    assert_eq!(2, list_users.len());

    front.follow("alice", "bob").await?;

    assert_eq!(true, front.is_following("alice", "bob").await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow() -> TribResult<()> {
    let num_front = 6;
    let mut front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let front = front_ends.pop().unwrap();
    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    // Concurrent follows
    let mut handles = vec![];
    let ok_count = Arc::new(Mutex::new(0));
    for front in front_ends.into_iter() {
        let count = ok_count.clone();
        let handle = tokio::spawn(async move {
            if front.follow("alice", "bob").await.is_ok() {
                let mut ok_c = count.lock().await;
                *ok_c += 1;
            }
        });
        handles.push(handle);
    }

    // Join and check error count
    for h in handles {
        match join!(h) {
            (Ok(_),) => (),
            (Err(e),) => {
                warn!("A front_end failed to join: {}", e);
            }
        };
    }

    assert!(*ok_count.lock().await <= 1);

    assert!(front.is_following("alice", "bob").await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_unfollow() -> TribResult<()> {
    let num_front = 6;
    let mut front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let front = front_ends.pop().unwrap();
    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    // Concurrent follows
    let mut handles = vec![];
    let ok_count = Arc::new(Mutex::new(0));
    for front in front_ends.into_iter() {
        let count = ok_count.clone();
        let handle = tokio::spawn(async move {
            if front.unfollow("alice", "bob").await.is_ok() {
                let mut ok_c = count.lock().await;
                *ok_c += 1;
            }
        });
        handles.push(handle);
    }

    // Join and check error count
    for h in handles {
        match join!(h) {
            (Ok(_),) => (),
            (Err(e),) => {
                warn!("A front_end failed to join: {}", e);
            }
        };
    }

    assert!(*ok_count.lock().await <= 1);

    assert!(!front.is_following("alice", "bob").await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow_with_unfollow() -> TribResult<()> {
    let num_front = 6;
    let mut front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let front = front_ends.pop().unwrap();
    front.sign_up("alice").await?;
    front.sign_up("bob").await?;

    front.follow("alice", "bob").await?;
    front.unfollow("alice", "bob").await?;

    // Concurrent follows
    let mut handles = vec![];
    let ok_count = Arc::new(Mutex::new(0));
    for front in front_ends.into_iter() {
        let count = ok_count.clone();
        let handle = tokio::spawn(async move {
            if front.follow("alice", "bob").await.is_ok() {
                let mut ok_c = count.lock().await;
                *ok_c += 1;
            }
        });
        handles.push(handle);
    }

    // Join and check error count
    for h in handles {
        match join!(h) {
            (Ok(_),) => (),
            (Err(e),) => {
                warn!("A front_end failed to join: {}", e);
            }
        };
    }

    assert!(*ok_count.lock().await <= 1);

    assert!(front.is_following("alice", "bob").await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_follow_limit() -> TribResult<()> {
    let num_front = 2;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    // The test bellow takes a long time
    // Maybe modify MAX_FOLLOWING to some small number when testing

    // let front = front_ends.get(0).unwrap();

    // for i in 0..MAX_FOLLOWING+2 {
    //     front.sign_up(&format!("user{}", i)).await?;
    // }

    // // Follow MAX_FOLLOWING users
    // for i in 1..MAX_FOLLOWING+1 {
    //     let result = front.follow("user0", &format!("user{}", i)).await;
    //     assert!(result.is_ok());
    // }

    // // Follow one more
    // let result = front.follow("user0", &format!("user{}", MAX_FOLLOWING+1)).await;
    // assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_home_correctness() -> TribResult<()> {
    let num_front = 1;
    let front_ends = setup(num_front).await?;
    assert_eq!(front_ends.len(), num_front as usize);

    let front = front_ends.get(0).unwrap();

    front.sign_up("aaaaaa").await?;
    front.sign_up("zzzzzz").await?;
    front.follow("zzzzzz", "aaaaaa").await?;

    // user a posts with clock 5
    front.post("aaaaaa", "trib a", 0).await?;

    // user a posts with clock 10
    front.post("aaaaaa", "trib b", 5).await?;

    // user b posts with clock 15
    front.post("zzzzzz", "trib c", 10).await?;

    let tribs = front.home("zzzzzz").await?;
    assert!(tribs.len() == 3);
    assert!(tribs.windows(2).all(|t| t[0].clock <= t[1].clock));

    Ok(())
}

pub fn main() {
    env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .init();
}
