use std::cmp::max;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::join;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Uri};
use tribbler::err::TribResult;
use tribbler::storage;
use tribbler::storage::Storage;

use crate::keeper::trib_keeper_client::TribKeeperClient;
use crate::keeper::BackStatus;
use crate::keeper::{self, trib_keeper_server::TribKeeper};
use crate::lab1;

pub enum Status {
    READY,
    DEAD,
    JOINING,
    LEAVING,
}

impl Status {
    pub fn get_code(&self) -> i32 {
        match *self {
            Status::READY => 0,
            Status::DEAD => 1,
            Status::JOINING => 2,
            Status::LEAVING => 3,
        }
    }
}

pub struct Keeper {
    pub keeper_rpc: KeeperRPC,
    pub keeper_daemon: KeeperDaemon,
}

impl Keeper {
    pub async fn new(backs: Vec<String>, addrs: Vec<String>, this: usize, id: u128) -> Self {
        // Initialize backend status as dead at the start
        let backend_status = Arc::new(RwLock::new(Box::new(HashMap::new())));
        for addrs in backs.iter() {
            backend_status.write().await.insert(
                addrs.clone(),
                BackStatus {
                    status: Status::DEAD.get_code(),
                    version: 0,
                },
            );
        }

        // Instantiate keeper daemon module
        let keeper_daemon = KeeperDaemon::new(backs, addrs, this, id, backend_status.clone());

        // Instantiate keeper RPC module
        let keeper_rpc = KeeperRPC::new(backend_status.clone());

        Keeper {
            keeper_rpc,
            keeper_daemon,
        }
    }
}

pub struct KeeperDaemon {
    backs: Vec<String>,
    addrs: Vec<String>,
    this: usize,
    id: u128,
    conn_keeper: HashMap<String, TribKeeperClient<Channel>>,
    conn_backend: HashMap<String, Box<dyn Storage>>,
    backend_status: Arc<RwLock<Box<HashMap<String, BackStatus>>>>,
    back_start: usize,
    back_end: usize,
}

impl KeeperDaemon {
    pub fn new(
        backs: Vec<String>,
        addrs: Vec<String>,
        this: usize,
        id: u128,
        backend_status: Arc<RwLock<Box<HashMap<String, BackStatus>>>>,
    ) -> Self {
        KeeperDaemon {
            backs,
            addrs,
            this,
            id,
            conn_keeper: HashMap::new(),
            conn_backend: HashMap::new(),
            back_start: 0,
            back_end: 0,
            backend_status,
        }
    }

    // Update live keepers and poll backend_status
    pub async fn update_keepers(&mut self) -> TribResult<()> {
        // Check liveness of keepers by polling backend status
        let addrs = self.addrs.clone();
        for (_idx, keeper) in addrs.iter().enumerate() {
            if self.conn_keeper.contains_key(keeper) {
                // Previously live keeper
                let channel = self.conn_keeper.get_mut(keeper).unwrap();
                let result = channel.get_backend_status(keeper::Empty {}).await;
                if result.is_err() {
                    log::info!(
                        "{}: cannot connect to keeper {}",
                        self.addrs[self.this],
                        keeper
                    );
                    self.conn_keeper.remove(keeper);
                } else {
                    // Update backend status
                    self.update_backend_status(&result.unwrap().get_ref().back_status)
                        .await;
                }
            } else {
                // Previously dead keeper
                let uri = match keeper.contains("http://") {
                    true => keeper.parse::<Uri>().unwrap(),
                    false => Uri::builder()
                        .scheme("http")
                        .authority(keeper.to_string())
                        .path_and_query("/")
                        .build()?,
                };
                let channel = Channel::builder(uri).connect_lazy();
                let mut conn = TribKeeperClient::new(channel.clone());
                let result = conn.get_backend_status(keeper::Empty {}).await;
                if result.is_ok() {
                    // Update backend status
                    self.update_backend_status(&result.unwrap().get_ref().back_status)
                        .await;
                    self.conn_keeper.insert((*keeper).clone(), conn);
                }
            }
        }

        Ok(())
    }

    // Helper function to update back_status with version
    pub async fn update_backend_status(&mut self, back_status: &HashMap<String, BackStatus>) {
        let mut write_handle = self.backend_status.write().await;
        for addr in back_status.keys() {
            // Compare version number
            let new_back_status = back_status.get(addr).unwrap();
            let cur_version = write_handle.get(addr).unwrap().version;
            if new_back_status.version > cur_version {
                write_handle.insert((*addr).clone(), (*new_back_status).clone());
            }
        }
    }

    // Update live backends and sync clock
    pub async fn update_backends(&mut self) -> TribResult<()> {
        let backends = &self.backs[self.back_start..self.back_end].to_vec();
        let mut clock: u64 = 0;

        // for each backend this keeper is responsible for
        for back in backends {
            // backend has just been transfered to us, establish new connection
            let read_handle = self.backend_status.read().await;
            if !self.conn_backend.contains_key(back)
                && read_handle.get(back).unwrap().status == Status::READY.get_code()
            {
                let uri = match back.contains("http://") {
                    true => back.parse::<Uri>().unwrap(),
                    false => Uri::builder()
                        .scheme("http")
                        .authority(back.to_string())
                        .path_and_query("/")
                        .build()?,
                };
                let client = lab1::new_client(&uri.to_string()).await?;
                self.conn_backend.insert(back.clone(), client);
            }
            drop(read_handle);

            // then update status and sync clock
            if self.conn_backend.contains_key(back) {
                // Previously live backend
                let conn = self.conn_backend.get(back).unwrap();
                let clk = conn.clock(0).await;
                if clk.is_err() {
                    // couldn't reach backend
                    log::info!(
                        "{}: cannot connect to backend {}",
                        self.addrs[self.this],
                        back,
                    );
                    self.conn_backend.remove(back);
                    self.set_back_status(back.clone(), Status::LEAVING).await;
                } else {
                    clock = max(clk.unwrap(), clock);
                }
            } else {
                // Previously dead backend
                let uri = match back.contains("http://") {
                    true => back.parse::<Uri>().unwrap(),
                    false => Uri::builder()
                        .scheme("http")
                        .authority(back.to_string())
                        .path_and_query("/")
                        .build()?,
                };
                let client = lab1::new_client(&uri.to_string()).await?;
                let clk = client.clock(0).await;
                if let Err(_e) = clk {
                    // couldn't reach backend
                    log::info!(
                        "{}: cannot connect to backend {}",
                        self.addrs[self.this],
                        back
                    );
                } else {
                    // backend is alive
                    clock = max(clk.unwrap(), clock);
                    self.conn_backend.insert(back.clone(), client);
                    self.set_back_status(back.clone(), Status::JOINING).await;
                }
            }
        }

        // sync backend clock
        for back in self.conn_backend.values() {
            let _ = back.clock(clock).await;
        }

        Ok(())
    }

    // Helper function to update single back_status
    pub async fn set_back_status(&mut self, addr: String, status: Status) {
        let mut write_handle = self.backend_status.write().await;
        let version = write_handle.get(&addr).unwrap().version + 1;
        let status = status.get_code();
        write_handle.insert(addr.clone(), BackStatus { status, version });
    }

    // update the set of backends managed by this keeper
    pub async fn update_backends_range(&mut self) -> TribResult<()> {
        // updates the backends this keeper is responsible for
        // calculate number of alive keepers and repartition backends
        let num_backends = self.backs.len() / self.conn_keeper.len();
        // evenly split backs among keepers by matching keeper this to index in array of alive keepers
        let keeper_index = self
            .addrs
            .iter()
            .filter(|k| self.conn_keeper.contains_key(*k))
            .enumerate()
            .find(|(_, addr)| (*addr).eq(&self.addrs[self.this]));
        println!(
            "keepers: {} index: {:?}",
            self.conn_keeper.len(),
            keeper_index
        );

        if let Some((idx, _)) = keeper_index {
            self.back_start = idx * num_backends;
            self.back_end = self.back_start + num_backends;

            // let last keeper take up the remaining backends
            if idx == self.conn_keeper.len() - 1 {
                self.back_end = self.backs.len();
            }
        }

        log::info!(
            "Keeper {} - new range [{}-{}]",
            self.addrs[self.this],
            self.back_start,
            self.back_end,
        );

        // remove from conn_back backends that are no longer ours
        let new_backs = self.backs[self.back_start..self.back_end].to_vec();
        let keys: Vec<String> = self.conn_backend.keys().map(|k| k.clone()).collect();
        for key in keys {
            if !new_backs.contains(&key) {
                log::info!(
                    "Keeper {} - remove connection to back {}",
                    self.addrs[self.this],
                    key,
                );
                self.conn_backend.remove(&key);
            }
        }

        Ok(())
    }

    pub async fn make_consistent(&mut self) -> TribResult<()> {
        // deal with leaving and joining backends by making necessary replications
        let backends: Vec<String> = self.backs.iter().map(|s| s.clone()).collect();
        let mut join_node = vec![];
        let mut leave_node = vec![];
        let mut non_dead_backends = vec![];
        for (idx, back) in backends.iter().enumerate() {
            let read_handle = self.backend_status.read().await;
            let back_status = read_handle.get(back).unwrap();
            let status = back_status.status;
            log::info!(
                "Keeper {} - status {} s:{} v:{}",
                self.addrs[self.this],
                back,
                status,
                back_status.version,
            );

            // If backend is in this keeper's range
            if idx >= self.back_start && idx < self.back_end {
                if status == Status::JOINING.get_code() {
                    join_node.push(back);
                } else if status == Status::LEAVING.get_code() {
                    leave_node.push(back);
                }
            }

            // Record non-dead node for later
            if status != Status::DEAD.get_code() {
                non_dead_backends.push(back);
            }
        }

        for node in join_node {
            let _ = self.node_join(&node, &non_dead_backends).await;
            self.set_back_status(node.to_string(), Status::READY).await;
        }

        for node in leave_node {
            let _ = self.node_leave(&node, &non_dead_backends).await;
            self.set_back_status(node.to_string(), Status::DEAD).await;
        }

        Ok(())
    }

    // Helper function for replicating data
    pub async fn copy_backends(&self, addr_from: &String, addr_to: &String) -> TribResult<()> {
        // Avoid copying to self
        if addr_from == addr_to {
            return Ok(());
        }

        log::info!(
            "Keeper {} - copying data from {} to {}",
            self.addrs[self.this],
            addr_from,
            addr_to
        );

        // New connections every time
        let uri = match addr_from.contains("http://") {
            true => addr_from.parse::<Uri>().unwrap(),
            false => Uri::builder()
                .scheme("http")
                .authority(addr_from.to_string())
                .path_and_query("/")
                .build()?,
        };
        let from_backend = &(lab1::new_client(&uri.to_string()).await?);
        let uri = match addr_to.contains("http://") {
            true => addr_to.parse::<Uri>().unwrap(),
            false => Uri::builder()
                .scheme("http")
                .authority(addr_to.to_string())
                .path_and_query("/")
                .build()?,
        };
        let to_backend = &(lab1::new_client(&uri.to_string()).await?);

        // handle list keys
        let mut keys = from_backend
            .list_keys(&storage::Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?;
        for key in keys.0 {
            let from_handle = from_backend.list_get(&key);
            let to_handle = to_backend.list_get(&key);
            let (from_val, to_val) = join!(from_handle, to_handle);
            let from_val = from_val?.0;
            let to_val = to_val?.0;
            for v in from_val {
                // Only copy non-existing data
                if !to_val.contains(&v) {
                    to_backend
                        .list_append(&storage::KeyValue {
                            key: key.clone(),
                            value: v.clone(),
                        })
                        .await?;
                }
            }
        }

        // handle key
        let pattern = storage::Pattern {
            prefix: "".to_string(),
            suffix: "".to_string(),
        };
        keys = from_backend.keys(&pattern).await?;
        for key in keys.0 {
            let value = from_backend.get(&key).await?;
            if value.is_some() {
                to_backend
                    .set(&storage::KeyValue {
                        key: key.clone(),
                        value: value.unwrap().clone(),
                    })
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn node_join(
        &self,
        addr: &String,
        non_dead_backends: &Vec<&String>,
    ) -> TribResult<()> {
        // predecessor is the alive backend before this backend
        let joining_node = non_dead_backends
            .iter()
            .position(|x| (*x).eq(addr))
            .unwrap();
        let successor = (joining_node + 1) % non_dead_backends.len();

        log::info!("Keeper {} - node join {}", self.addrs[self.this], addr);

        // copy data O->A->C
        // B joins between A and C, copy C to B
        let _ = self
            .copy_backends(
                &non_dead_backends[successor],
                &non_dead_backends[joining_node],
            )
            .await?;
        Ok(())
    }

    pub async fn node_leave(
        &self,
        addr: &String,
        non_dead_backends: &Vec<&String>,
    ) -> TribResult<()> {
        if non_dead_backends.len() < 3 {
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                "Not enough backends",
            )));
        }

        log::info!("Keeper {} - node leave {}", self.addrs[self.this], addr);

        // predecessor is the alive backend before this backend
        let index = non_dead_backends
            .iter()
            .position(|x| (*x).eq(addr))
            .unwrap();
        let live_nodes: Vec<&String> = non_dead_backends
            .iter()
            .filter(|s| !(**s).eq(addr))
            .map(|s| *s)
            .collect();
        let index = index % live_nodes.len();
        let predecessor = index.checked_sub(1).unwrap_or(live_nodes.len() - 1);

        let successor_1 = (predecessor + 1) % live_nodes.len();
        let successor_2 = (predecessor + 2) % live_nodes.len();
        let successor_3 = (predecessor + 3) % live_nodes.len();

        // copy data O->A->B->C->D
        // A crashes, copy O to B, B to C and C to D
        let p_to_s1 = self.copy_backends(&live_nodes[predecessor], &live_nodes[successor_1]);
        let s1_to_s2 = self.copy_backends(&live_nodes[successor_1], &live_nodes[successor_2]);
        let s2_to_s3 = self.copy_backends(&live_nodes[successor_2], &live_nodes[successor_3]);
        let _ = join!(p_to_s1, s1_to_s2, s2_to_s3);

        Ok(())
    }

    // Called every fixed time interval
    pub async fn serve(mut self) {
        loop {
            // Contact keepers and poll backend status from all keepers
            let _ = self.update_keepers().await;

            // Sync clock, check backend liveness, and update backend status
            let _ = self.update_backends().await;

            // Update keeper range for backends
            let _ = self.update_backends_range().await;

            // Contact backends and do necessary replication
            let _ = self.make_consistent().await;

            // Sleep for fixed amount of time
            thread::sleep(Duration::from_secs(1));
        }
    }
}

pub struct KeeperRPC {
    backend_status: Arc<RwLock<Box<HashMap<String, BackStatus>>>>,
}

impl KeeperRPC {
    pub fn new(backend_status: Arc<RwLock<Box<HashMap<String, BackStatus>>>>) -> Self {
        KeeperRPC { backend_status }
    }
}

#[async_trait::async_trait]
impl TribKeeper for KeeperRPC {
    // just return response as heartbeat signal
    async fn heartbeat(
        &self,
        _request: tonic::Request<keeper::Empty>,
    ) -> Result<tonic::Response<keeper::Empty>, tonic::Status> {
        Ok(tonic::Response::new(keeper::Empty {}))
    }

    // gets backend status that this keeper is responsible for
    async fn get_backend_status(
        &self,
        _request: tonic::Request<keeper::Empty>,
    ) -> Result<tonic::Response<keeper::Dictionary>, tonic::Status> {
        let lock = &*(self.backend_status.as_ref());
        let status: HashMap<String, BackStatus> = lock.read().await.as_ref().clone();

        Ok(tonic::Response::new(keeper::Dictionary {
            back_status: status,
        }))
    }
}
