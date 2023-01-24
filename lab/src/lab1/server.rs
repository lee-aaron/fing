use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use tribbler::{rpc::trib_storage_server::TribStorage, storage::*};

pub struct StorageServer {
    pub addr: String,
    pub storage: Mutex<Box<dyn Storage>>,
}

impl StorageServer {
    pub fn new(addr: String, storage: Mutex<Box<dyn Storage>>) -> Self {
        StorageServer { addr, storage }
    }
}

#[async_trait]
impl TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<tribbler::rpc::Key>,
    ) -> Result<tonic::Response<tribbler::rpc::Value>, tonic::Status> {
        let key = request.into_inner();
        let result = self.storage.lock().await.get(&key.key).await;
        if let Ok(value) = result {
            match value {
                Some(value) => Ok(Response::new(tribbler::rpc::Value { value })),
                None => Ok(Response::new(tribbler::rpc::Value {
                    value: "".to_string(),
                })),
            }
        } else {
            Err(Status::aborted("Unknown error in get"))
        }
    }
    async fn set(
        &self,
        request: tonic::Request<tribbler::rpc::KeyValue>,
    ) -> Result<tonic::Response<tribbler::rpc::Bool>, tonic::Status> {
        let key_value = request.into_inner();
        let result = self
            .storage
            .lock()
            .await
            .set(&KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        if let Ok(value) = result {
            Ok(Response::new(tribbler::rpc::Bool { value }))
        } else {
            Err(Status::aborted("Unknown error in set"))
        }
    }
    async fn keys(
        &self,
        request: tonic::Request<tribbler::rpc::Pattern>,
    ) -> Result<tonic::Response<tribbler::rpc::StringList>, tonic::Status> {
        let pattern = request.into_inner();
        let result = self
            .storage
            .lock()
            .await
            .keys(&Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await;
        if let Ok(list) = result {
            Ok(Response::new(tribbler::rpc::StringList { list: list.0 }))
        } else {
            Err(Status::aborted("Unknown error in keys"))
        }
    }
    async fn list_get(
        &self,
        request: tonic::Request<tribbler::rpc::Key>,
    ) -> Result<tonic::Response<tribbler::rpc::StringList>, tonic::Status> {
        let key = request.into_inner();
        let result = self.storage.lock().await.list_get(&key.key).await;
        if let Ok(list) = result {
            Ok(Response::new(tribbler::rpc::StringList { list: list.0 }))
        } else {
            Err(Status::aborted("Unknown error in list_get"))
        }
    }
    async fn list_append(
        &self,
        request: tonic::Request<tribbler::rpc::KeyValue>,
    ) -> Result<tonic::Response<tribbler::rpc::Bool>, tonic::Status> {
        let key_value = request.into_inner();
        let result = self
            .storage
            .lock()
            .await
            .list_append(&KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        if let Ok(value) = result {
            Ok(Response::new(tribbler::rpc::Bool { value }))
        } else {
            Err(Status::aborted("Unknown error in list_append"))
        }
    }
    async fn list_remove(
        &self,
        request: tonic::Request<tribbler::rpc::KeyValue>,
    ) -> Result<tonic::Response<tribbler::rpc::ListRemoveResponse>, tonic::Status> {
        let key_value = request.into_inner();
        let result = self
            .storage
            .lock()
            .await
            .list_remove(&KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        if let Ok(value) = result {
            Ok(Response::new(tribbler::rpc::ListRemoveResponse {
                removed: value,
            }))
        } else {
            Err(Status::aborted("Unknown error in list_remove"))
        }
    }
    async fn list_keys(
        &self,
        request: tonic::Request<tribbler::rpc::Pattern>,
    ) -> Result<tonic::Response<tribbler::rpc::StringList>, tonic::Status> {
        let pattern = request.into_inner();
        let result = self
            .storage
            .lock()
            .await
            .list_keys(&Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await;
        if let Ok(list) = result {
            Ok(Response::new(tribbler::rpc::StringList { list: list.0 }))
        } else {
            Err(Status::aborted("Unknown error in list_keys"))
        }
    }
    async fn clock(
        &self,
        request: tonic::Request<tribbler::rpc::Clock>,
    ) -> Result<tonic::Response<tribbler::rpc::Clock>, tonic::Status> {
        let clock = request.into_inner();
        let result = self.storage.lock().await.clock(clock.timestamp).await;
        if let Ok(result) = result {
            Ok(Response::new(tribbler::rpc::Clock { timestamp: result }))
        } else {
            Err(Status::aborted("Unknown error in list_remove"))
        }
    }
}
