use async_trait::async_trait;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
use tribbler::{colon::escape, err::TribblerError, storage::List};
use tribbler::{
    err::TribResult,
    storage::{self, BinStorage, Storage},
};

use crate::lab1;

pub struct BinStorageClient {
    pub backs: Vec<String>,
}

impl BinStorageClient {
    pub fn new(backs: Vec<String>) -> Self {
        // Record backend servers
        BinStorageClient { backs }
    }
}

#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        // Hash to backend server
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let bin_idx = (hasher.finish() % self.backs.len() as u64) as usize;

        log::info!(
            "BinStorageClient bin: {:?} w/ name {}",
            &self.backs[bin_idx],
            name
        );

        // Establish connection and return - add scheme
        let bins = self.get_live_servers(bin_idx).await?;

        let wrapper = BinWrapper {
            storages: bins,
            name: name.to_string(),
        };
        Ok(Box::new(wrapper))
    }
}

impl BinStorageClient {
    async fn get_live_servers(&self, bin_idx: usize) -> TribResult<Vec<Box<dyn Storage>>> {
        // given starting index, iteratively check for up to three servers. if index comes back to itself, return what you have
        let mut idx = bin_idx;
        let mut count = 0;
        let mut live_backends = vec![];
        while count < 3 {
            let addr = &self.backs[idx];
            let client = lab1::new_client(addr.as_str()).await?;
            if let Ok(_r) = client.clock(0).await {
                log::info!("{} is live", addr);
                live_backends.push(client);
                count += 1;
            }
            idx = (idx + 1) % self.backs.len();

            if idx == bin_idx {
                break;
            }
        }
        Ok(live_backends)
    }
}

// change keys to append colons etc. and call lab1 client functions
struct BinWrapper {
    pub storages: Vec<Box<dyn Storage>>,
    pub name: String,
}

#[async_trait::async_trait]
impl storage::KeyString for BinWrapper {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let key = format!("{}::{}", escape(self.name.as_str()), key);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(storage.get(&key).await);
        }
        let ok_values = r
            .iter()
            .filter(|x| x.is_ok())
            .map(|x| x.as_ref().unwrap().clone())
            .collect::<Vec<Option<String>>>();
        if ok_values.len() > 0 {
            // if any of the values are Some return that one
            let ret = ok_values.iter().find(|x| x.is_some());
            if ret.is_some() {
                return Ok(ret.unwrap().clone());
            }
            Ok(None)
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Get failed"))))
        }
    }
    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let key = format!("{}::{}", escape(self.name.as_str()), kv.key);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(
                storage
                    .set(&storage::KeyValue {
                        key: key.clone(),
                        value: kv.value.clone(),
                    })
                    .await,
            );
        }
        let ok_count = r.iter().filter(|x| x.is_ok()).count();
        if ok_count == self.storages.len() {
            Ok(*r[0].as_ref().unwrap())
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Set failed"))))
        }
    }
    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let prefix = format!("{}::{}", escape(self.name.as_str()), p.prefix);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(
                storage
                    .keys(&storage::Pattern {
                        prefix: prefix.clone(),
                        suffix: p.suffix.clone(),
                    })
                    .await,
            );
        }
        let ok_values = r
            .iter()
            .filter(|x| x.is_ok())
            .map(|x| x.as_ref().unwrap().clone())
            .collect::<Vec<List>>();
        if ok_values.len() > 0 {
            let ret = ok_values.iter().find(|x| !x.0.is_empty());
            if ret.is_some() {
                let mut result = ret.unwrap().clone().0;
                for i in 0..result.len() {
                    result[i] = result[i]
                        .strip_prefix(&format!("{}::", self.name))
                        .unwrap()
                        .to_string();
                }
                return Ok(List(result));
            }
            Ok(List(Vec::new()))
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Keys failed"))))
        }
    }
}

#[async_trait::async_trait]
impl storage::KeyList for BinWrapper {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let key = format!("{}::{}", escape(self.name.as_str()), key);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(storage.list_get(&key).await);
        }
        let ok_values = r
            .iter()
            .filter(|x| x.is_ok())
            .map(|x| x.as_ref().unwrap().clone())
            .collect::<Vec<List>>();
        if ok_values.len() > 0 {
            let ret = ok_values.iter().find(|x| x.0.len() > 0);
            if ret.is_some() {
                return Ok(ret.unwrap().clone());
            }
            Ok(List(Vec::new()))
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Get failed"))))
        }
    }
    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let key = format!("{}::{}", escape(self.name.as_str()), kv.key);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(
                storage
                    .list_append(&storage::KeyValue {
                        key: key.clone(),
                        value: kv.value.clone(),
                    })
                    .await,
            );
        }
        let ok_count = r.iter().filter(|x| x.is_ok()).count();
        if ok_count == self.storages.len() {
            Ok(*r[0].as_ref().unwrap())
        } else {
            Err(Box::new(TribblerError::Unknown(format!(
                "List Append Failed"
            ))))
        }
    }
    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let key = format!("{}::{}", escape(self.name.as_str()), kv.key);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(
                storage
                    .list_remove(&storage::KeyValue {
                        key: key.clone(),
                        value: kv.value.clone(),
                    })
                    .await,
            );
        }
        let ok_count = r.iter().filter(|x| x.is_ok()).count();
        if ok_count == self.storages.len() {
            Ok(*r[0].as_ref().unwrap())
        } else {
            Err(Box::new(TribblerError::Unknown(format!(
                "List Remove Failed"
            ))))
        }
    }
    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let prefix = format!("{}::{}", escape(self.name.as_str()), p.prefix);
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(
                storage
                    .list_keys(&storage::Pattern {
                        prefix: prefix.clone(),
                        suffix: p.suffix.clone(),
                    })
                    .await,
            );
        }
        let ok_values = r
            .iter()
            .filter(|x| x.is_ok())
            .map(|x| x.as_ref().unwrap().clone())
            .collect::<Vec<List>>();
        if ok_values.len() > 0 {
            let ret = ok_values.iter().find(|x| !x.0.is_empty());
            if ret.is_some() {
                let mut result = ret.unwrap().clone().0;
                for i in 0..result.len() {
                    result[i] = result[i]
                        .strip_prefix(&format!("{}::", self.name))
                        .unwrap()
                        .to_string();
                }
                return Ok(List(result));
            }
            Ok(List(Vec::new()))
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Keys failed"))))
        }
    }
}

#[async_trait::async_trait]
impl Storage for BinWrapper {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut r = vec![];
        for storage in self.storages.iter() {
            r.push(storage.clock(at_least).await);
        }
        let ok_count = r.iter().filter(|x| x.is_ok()).count();
        if ok_count == self.storages.len() {
            Ok(*r[0].as_ref().unwrap())
        } else {
            Err(Box::new(TribblerError::Unknown(format!("Clock failed"))))
        }
    }
}
