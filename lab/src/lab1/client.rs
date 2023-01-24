use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Uri};
use tribbler::{err::TribResult, rpc::trib_storage_client::TribStorageClient, storage::*};

pub struct StorageClient {
    pub addr: String,
    pub rpc_con: Mutex<TribStorageClient<Channel>>,
}

impl StorageClient {
    pub async fn new(addr: &str) -> TribResult<Self> {
        let addr = addr.to_string();
        let uri = match addr.contains("http://") {
            true => addr.parse::<Uri>().unwrap(),
            false => {
                if let Ok(u) = Uri::builder()
                    .scheme("http")
                    .authority(addr.to_string())
                    .path_and_query("/")
                    .build()
                {
                    u
                } else {
                    addr.parse::<Uri>().unwrap()
                }
            }
        };
        let channel = Channel::builder(uri).connect_lazy();
        let client = TribStorageClient::new(channel.clone());
        let rpc_con = Mutex::new(client);
        Ok(StorageClient { addr, rpc_con })
    }
}

#[async_trait]
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let r = self
            .rpc_con
            .lock()
            .await
            .get(tribbler::rpc::Key {
                key: key.to_string(),
            })
            .await?;
        match r.into_inner().value {
            value => {
                if value != "" {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let r = self
            .rpc_con
            .lock()
            .await
            .set(tribbler::rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let r = self
            .rpc_con
            .lock()
            .await
            .keys(tribbler::rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        match r.into_inner().list {
            value => Ok(tribbler::storage::List(value)),
        }
    }
}

#[async_trait]
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let r = self
            .rpc_con
            .lock()
            .await
            .list_get(tribbler::rpc::Key {
                key: key.to_string(),
            })
            .await?;
        match r.into_inner().list {
            value => Ok(tribbler::storage::List(value)),
        }
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let r = self
            .rpc_con
            .lock()
            .await
            .list_append(tribbler::rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let r = self
            .rpc_con
            .lock()
            .await
            .list_remove(tribbler::rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().removed {
            value => Ok(value),
        }
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let r = self
            .rpc_con
            .lock()
            .await
            .list_keys(tribbler::rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        match r.into_inner().list {
            value => Ok(tribbler::storage::List(value)),
        }
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let r = self
            .rpc_con
            .lock()
            .await
            .clock(tribbler::rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        match r.into_inner().timestamp {
            value => Ok(value),
        }
    }
}
