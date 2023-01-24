use lab::{self, lab1, lab2};
use log::LevelFilter;
use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;
use tribbler::{config::BackConfig, err::TribResult, storage::{MemStorage, self}, trib::Server};

async fn setup(
    addr: &str,
) -> TribResult<(Box<dyn Server + Send + Sync>, JoinHandle<TribResult<()>>)> {
    let _ = env_logger::builder()
        .filter_level(LevelFilter::Info)
        .try_init();

    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();

    let config = BackConfig {
        addr: addr.to_string(),
        ready: Some(tx.clone()),
        shutdown: None,
        storage: Box::new(MemStorage::new()),
    };

    let handle = spawn_back(config);

    let ready = rx.recv_timeout(Duration::from_secs(1))?;
    if !ready {
        panic!("failed to start")
    }

    let front = lab2::new_front(lab2::new_bin_client(vec![addr.to_string()]).await?).await?;

    Ok((front, handle))
}

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab1::serve_back(cfg))
}

async fn populate(server: &Box<dyn Server + Send + Sync>) -> TribResult<()> {
    // populate server
    server.sign_up("alice").await?;
    server.sign_up("bob").await?;
    server.sign_up("charlie").await?;
    server.sign_up("dave").await?;
    server.sign_up("eve").await?;
    server.sign_up("frank").await?;
    server.sign_up("george").await?;
    server.sign_up("harry").await?;
    server.sign_up("iris").await?;
    server.sign_up("joe").await?;
    server.sign_up("kate").await?;
    server.sign_up("lisa").await?;
    server.sign_up("mike").await?;
    server.sign_up("nancy").await?;
    server.sign_up("olivia").await?;
    server.sign_up("paul").await?;
    server.sign_up("quinn").await?;
    server.sign_up("richard").await?;
    server.sign_up("sarah").await?;
    // should be 19 people

    server.post("alice", "Hello, world.", 0).await?;
    server.post("bob", "Just tribble it.", 0).await?;
    server.post("charlie", "Testing.", 0).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow() -> TribResult<()> {
    let addr = "127.0.0.1:8000";
    let (front, _srv) = setup(addr).await?;

    populate(&front).await?;

    let front = Arc::new(front);
    let mut handles = vec![];
    let mut r = vec![];
    for _ in 0..5 {
        let jh = tokio::spawn(async move {
            let front =
                lab2::new_front(lab2::new_bin_client(vec![addr.to_string()]).await?).await?;
            if let Err(e) = front.follow("alice", "charlie").await {
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
async fn test_concurrent_unfollow() -> TribResult<()> {
    let addr = "127.0.0.1:8001";
    let (front, _srv) = setup(addr).await?;

    populate(&front).await?;

    front.follow("alice", "bob").await?;

    let front = Arc::new(front);
    let mut handles = vec![];
    let mut r = vec![];
    for _ in 0..5 {
        let jh = tokio::spawn(async move {
            let front =
                lab2::new_front(lab2::new_bin_client(vec![addr.to_string()]).await?).await?;
            if let Err(e) = front.unfollow("alice", "bob").await {
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

    let r = front.following("alice").await?;
    assert_eq!(r.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow_unfollow() -> TribResult<()> {
    let addr = "127.0.0.1:8002";
    let (front, _srv) = setup(addr).await?;

    populate(&front).await?;

    front.follow("alice", "bob").await?;
    front.unfollow("alice", "bob").await?;

    let front = Arc::new(front);
    let mut handles = vec![];
    let mut r = vec![];
    for _ in 0..5 {
        let jh = tokio::spawn(async move {
            let front =
                lab2::new_front(lab2::new_bin_client(vec![addr.to_string()]).await?).await?;
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

    let r = front.following("alice").await?;
    assert_eq!(r.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiops() -> TribResult<()> {
    let addr = "127.0.0.1:8003";
    let (front, _srv) = setup(addr).await?;

    populate(&front).await?;

    let front = Arc::new(front);
    let mut r = vec![];
    for _ in 0..5 {
        let front = lab2::new_front(lab2::new_bin_client(vec![addr.to_string()]).await?).await?;
        if let Err(e) = front.follow("alice", "bob").await {
            r.push(Err(e));
        };
        if let Err(e) = front.unfollow("alice", "bob").await {
            r.push(Err(e));
        }
        r.push(Ok(()));
    }

    assert!(r.iter().filter(|&x| x.is_err()).count() == 0);

    let r = front.following("alice").await?;
    assert_eq!(r.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_bin_lists() -> TribResult<()> {
    let addr = "127.0.0.1:8004";
    
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();

    let config = BackConfig {
        addr: addr.to_string(),
        ready: Some(tx.clone()),
        shutdown: None,
        storage: Box::new(MemStorage::new()),
    };

    spawn_back(config);

    let ready = rx.recv_timeout(Duration::from_secs(1))?;
    if !ready {
        panic!("failed to start")
    }

    let storage = lab2::new_bin_client(vec![addr.to_string()]).await?;

    let bin = storage.bin("alice").await?;
    bin.list_append(&storage::KeyValue {
        key: "bob".to_string(),
        value: "A".to_string(),
    }).await?;

    bin.list_append(&storage::KeyValue {
        key: "bob".to_string(),
        value: "B".to_string(),
    }).await?;


    let list = bin.list_get("bob").await?.0;
    assert_eq!(list.len(), 2);
    assert_eq!(list[0], "A");
    assert_eq!(list[1], "B");

    let bin = storage.bin("charlie").await?;
    bin.list_append(&storage::KeyValue {
        key: "bob".to_string(),
        value: "A".to_string(),
    }).await?;

    bin.list_append(&storage::KeyValue {
        key: "bob".to_string(),
        value: "B".to_string(),
    }).await?;


    let list = bin.list_get("bob").await?.0;
    assert_eq!(list.len(), 2);
    assert_eq!(list[0], "A");
    assert_eq!(list[1], "B");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_bin_values() -> TribResult<()> {
    let addr = "127.0.0.1:8005";

    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();

    let config = BackConfig {
        addr: addr.to_string(),
        ready: Some(tx.clone()),
        shutdown: None,
        storage: Box::new(MemStorage::new()),
    };

    spawn_back(config);

    let ready = rx.recv_timeout(Duration::from_secs(1))?;
    if !ready {
        panic!("failed to start")
    }

    let storage = lab2::new_bin_client(vec![addr.to_string()]).await?;

    // test key string methods (get, set, etc) on two different bins
    let bin = storage.bin("alice").await?;
    bin.set(&storage::KeyValue {
        key: "a".to_string(),
        value: "b".to_string(),
    }).await?;
    let value = bin.get("a").await?;
    assert_eq!(value, Some("b".to_string()));

    let bin = storage.bin("charlie").await?;
    let value = bin.get("a").await?;
    assert_eq!(value, None);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_keys() -> TribResult<()> {
    let addr = "127.0.0.1:8006";

    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();

    let config = BackConfig {
        addr: addr.to_string(),
        ready: Some(tx.clone()),
        shutdown: None,
        storage: Box::new(MemStorage::new()),
    };

    spawn_back(config);

    let ready = rx.recv_timeout(Duration::from_secs(1))?;
    if !ready {
        panic!("failed to start")
    }

    let storage = lab2::new_bin_client(vec![addr.to_string()]).await?;

    let bin = storage.bin("alice").await?;
    bin.set(&storage::KeyValue {
        key: "a".to_string(),
        value: "b".to_string(),
    }).await?;
    bin.set(&storage::KeyValue {
        key: "aa".to_string(),
        value: "b".to_string(),
    }).await?;
    bin.set(&storage::KeyValue {
        key: "aaa".to_string(),
        value: "b".to_string(),
    }).await?;
    let value = bin.keys(&storage::Pattern{
        prefix: "".to_string(),
        suffix: "".to_string()
    }).await?.0;
    assert_eq!(value.len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tribs() -> TribResult<()> {
    let addr = "127.0.0.1:8007";
    let (front, _srv) = setup(addr).await?;

    populate(&front).await?;

    let front = Arc::new(front);

    for _ in 0..200 {
        let _ = front.post("alice", "tribbler", 0).await;
    }

    let tribs = front.tribs("alice").await?;
    assert!(tribs.len() == tribbler::trib::MAX_TRIB_FETCH);

    Ok(())
}