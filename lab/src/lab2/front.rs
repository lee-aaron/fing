use std::{
    cmp::{max, min},
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tribbler::{
    colon::escape,
    err::{TribResult, TribblerError},
    storage::{BinStorage, KeyValue},
    trib::{
        is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
    },
};

use super::metadata::FrontMetadata;

pub struct FrontClient {
    pub bin_storage: Mutex<Box<dyn BinStorage>>,
    pub metadata: FrontMetadata,
}

impl FrontClient {
    pub fn new(bin_storage: Box<dyn BinStorage>) -> Self {
        FrontClient {
            bin_storage: Mutex::new(bin_storage),
            metadata: FrontMetadata::new(),
        }
    }

    pub fn construct_key(&self, user: &str, key: &str) -> String {
        let user_e = escape(user);
        let key_e = escape(key);

        format!("{}::{}", user_e, key_e)
    }
}

impl FrontClient {
    pub fn str_to_trib(&self, user: &str, trib: &str) -> TribResult<Trib> {
        let trib_content: Vec<&str> = trib.split("::").collect();
        if trib_content.len() != 3 {
            return Err(Box::new(TribblerError::Unknown(format!(
                "Trib content malformatted"
            ))));
        }
        let clock: u64 = match trib_content[0].parse() {
            Ok(t) => t,
            Err(_) => {
                return Err(Box::new(TribblerError::Unknown(format!(
                    "Trib clock malformatted"
                ))))
            }
        };
        let time: u64 = match trib_content[1].parse() {
            Ok(t) => t,
            Err(_) => {
                return Err(Box::new(TribblerError::Unknown(format!(
                    "Trib time malformatted"
                ))))
            }
        };
        Ok(Trib {
            user: user.to_string(),
            message: trib_content[2].to_string(),
            time,
            clock,
        })
    }
    pub fn trib_to_str(&self, trib: &Trib) -> TribResult<String> {
        Ok(format!(
            "{}::{}::{}::{}",
            trib.clock.to_string(),
            trib.time.to_string(),
            trib.user.to_string(),
            trib.message.to_string()
        ))
    }

    pub fn first_in_follow_log(&self, clock: u64, follow_log: Vec<String>, op: &str) -> bool {
        let mut first_follow = true;
        let mut last_follow = false;

        for l in follow_log.iter() {
            let parts: Vec<&str> = l.split("::").collect();
            if !parts[0].eq(op) {
                first_follow = true;
            } else if parts[0].eq(op) {
                if parts[1].eq(&clock.to_string()) {
                    last_follow = first_follow;
                }
                first_follow = false;
            }
        }

        return follow_log.len() == 0 || last_follow;
    }

    pub fn distinct_follows(&self, follows: Vec<String>) -> HashSet<String> {
        let mut distinct_follows: HashSet<String> = HashSet::new();
        for follow in follows.into_iter() {
            if distinct_follows.len() > MAX_FOLLOWING {
                break;
            } else if !distinct_follows.contains(&follow) {
                distinct_follows.insert(follow);
            }
        }

        return distinct_follows;
    }
}

#[async_trait]
impl Server for FrontClient {
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        println!("Call sign_up");
        // Check user name
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }

        // Check name exist from backend
        let user_key = self.metadata.get_user_exist_key();
        let storage = self.bin_storage.lock().await;
        let bin = storage.as_ref().bin(user).await?;
        if bin.get(&user_key).await?.is_some() {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }

        // Commit user creation by writing to backend
        let user_exist = KeyValue {
            key: user_key,
            value: "true".to_string(),
        };
        let success = bin.set(&user_exist).await?;
        if !success {
            return Err(Box::new(TribblerError::RpcError(format!(
                "Failed to create user \"{}\"",
                user.to_string()
            ))));
        }
        println!("Create new user in backend: {}", user);

        // Check whether we need to add more users
        let bin = storage.as_ref().bin(&self.metadata.default_server).await?;
        let all_users_key = self.metadata.get_all_users_key();
        let user_list = bin.list_get(&all_users_key).await?.0;

        let distinct_users: HashSet<String> = user_list.into_iter().collect();
        if distinct_users.len() < MIN_LIST_USER {
            // Add name to all users list on the backend
            let new_user = KeyValue {
                key: all_users_key,
                value: user.to_string(),
            };
            println!("Add new user to list: {}", user);
            let _ = bin.list_append(&new_user).await?;
        }

        Ok(())
    }

    async fn list_users(&self) -> TribResult<Vec<String>> {
        println!("Call list_users");
        // Get all users from default server
        let storage = self.bin_storage.lock().await;
        let bin = storage.as_ref().bin(&self.metadata.default_server).await?;
        let user_list_key = self.metadata.get_all_users_key();
        let users = bin.list_get(&user_list_key).await?.0;

        // Eliminate users that don't exist and duplicates
        // Possible that add to list succeeded when sign-up while set failed
        let distinct_users: HashSet<String> = users.into_iter().collect();
        let result: Vec<String> = distinct_users.into_iter().collect();

        // Sort by alphabetical order
        let mut sorted = result[..min(MIN_LIST_USER, result.len())].to_vec();
        sorted.sort();
        println!("List users: {:?}", sorted);

        Ok(sorted)
    }

    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        println!("Call post");
        // Check user name
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }
        // Check post is valid
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        // Check post author exists
        let bin = self.bin_storage.lock().await.bin(who).await?;
        let exist_key = self.metadata.get_user_exist_key();
        if !bin.get(&exist_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        // Get current backend clock, and compare with given clock, choose the larger one
        let bin_clock: u64 = match bin.clock(0).await {
            Ok(clk) => clk,
            Err(_) => {
                return Err(Box::new(TribblerError::RpcError(format!(
                    "Failed to read backend clock"
                ))))
            }
        };
        let new_clock = max(bin_clock, clock + 1);

        // Commit tribble
        let tribs_key = self.metadata.get_user_tribs_key();
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(time) => time,
            Err(_) => {
                return Err(Box::new(TribblerError::RpcError(format!(
                    "Failed to read current system time"
                ))))
            }
        };
        let trib_content = format!(
            "{}::{}::{}",
            new_clock.to_string(),
            timestamp.as_secs().to_string(),
            escape(post)
        );
        println!("{} post trib: {}", who.to_string(), trib_content);

        let trib = KeyValue {
            key: tribs_key.to_string(),
            value: trib_content,
        };
        bin.list_append(&trib).await?;

        Ok(())
    }

    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        println!("Call tribs");
        // Check user name
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        // Get bin
        let bin = self.bin_storage.lock().await.bin(user).await?;

        // Check user exists
        let exist_key = self.metadata.get_user_exist_key();
        if !bin.get(&exist_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        // Get all tribs
        let tribs_key = self.metadata.get_user_tribs_key();
        let tribs = bin.list_get(&tribs_key).await?.0;

        // Construct all tribs and sort them
        let mut sorted_tribs: Vec<Arc<Trib>> = Vec::new();
        for trib in tribs.into_iter() {
            let new_trib = self.str_to_trib(&user, &trib)?;
            sorted_tribs.push(Arc::new(new_trib));
        }

        // Sort tribs
        sorted_tribs.sort_by(|a, b| {
            Ord::cmp(&a.clock, &b.clock)
                .then(Ord::cmp(&a.time, &b.time))
                .then(Ord::cmp(&a.user, &b.user))
                .then(Ord::cmp(&a.message, &b.message))
        });

        // Eliminate tribs over limit
        let mut to_remove_count = 0;
        if sorted_tribs.len() > MAX_TRIB_FETCH {
            to_remove_count = sorted_tribs.len() - MAX_TRIB_FETCH;
        }
        for i in 0..to_remove_count {
            let trib_to_remove = sorted_tribs.get(sorted_tribs.len() - i - 1).unwrap();
            let to_remove = KeyValue {
                key: user.to_string(),
                value: self.trib_to_str(trib_to_remove)?,
            };
            // Ignore error as it's not critical
            let _ = bin.list_remove(&to_remove).await?;
        }

        // trim sorted tribs to max trib fetch
        sorted_tribs.truncate(MAX_TRIB_FETCH);

        Ok(sorted_tribs)
    }

    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        println!("Call follow");

        // Check user name and who != whom
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        } else if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        } else if who.eq(whom) {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        // Check users exist
        let storage = self.bin_storage.lock().await;
        let user_key = self.metadata.get_user_exist_key();
        let bin = storage.as_ref().bin(whom).await?;
        if !bin.get(&user_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let user_key = self.metadata.get_user_exist_key();
        let bin = storage.as_ref().bin(who).await?;
        if !bin.get(&user_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        // Get clock for future op
        let server_clock = bin.clock(0).await?;
        println!("Follow with clock: {}", server_clock);

        // Get follow list and test exisistence
        let follow_key = self.metadata.get_user_follows_key();
        let follow_list = self.distinct_follows(bin.list_get(&follow_key).await?.0);
        println!("Follow list: {:?}", follow_list);
        if follow_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        } else if follow_list.len() >= MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }
        println!("Follow list len: {}", follow_list.len());

        // Uncomment this line2 to better test concurrent follow
        // std::thread::sleep(std::time::Duration::from_secs(1));

        // Concurrent procedures
        // Append to follow list
        let kv = KeyValue {
            key: follow_key,
            value: whom.to_string(),
        };
        let _ = bin.list_append(&kv).await?;
        println!("{} follows {}", who, whom);

        // Append to follow log with clock
        let follow_log_key = self.metadata.get_user_follow_logs_key(whom);
        let kv = KeyValue {
            key: follow_log_key.clone(),
            value: format!("f::{}", server_clock.to_string()),
        };
        let _ = bin.list_append(&kv).await?;

        // Get follow log with clock, check this is the first follow after an unfollow or nothing
        let follow_log = bin.list_get(&follow_log_key).await?.0;
        println!("Follow log: {:?}", follow_log);
        if self.first_in_follow_log(server_clock, follow_log, "f") {
            // First in follow, check limit exceed
            let follow_key = self.metadata.get_user_follows_key();
            println!("Follow with clock {} succeeded", server_clock);
            let followed = bin.list_get(&follow_key).await?.0;
            // Only collect followed users up to the point of this follow
            let mut distinct_follow: HashSet<String> = HashSet::new();
            for followed_user in followed {
                if followed_user.eq(whom) {
                    break;
                } else {
                    distinct_follow.insert(followed_user);
                }
            }
            if distinct_follow.len() >= MAX_FOLLOWING {
                // Remove user and report error, ok to fail
                let kv = KeyValue {
                    key: follow_key,
                    value: whom.to_string(),
                };
                let _ = bin.list_remove(&kv).await?;
                return Err(Box::new(TribblerError::FollowingTooMany));
            }
        } else {
            // Not first in follow, clean up, ok to fail
            println!("Follow with clock {} failed", server_clock);
            let _ = bin.list_remove(&kv).await?;

            // Report already following
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        Ok(())
    }
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        println!("Call unfollow");
        // Check user name
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        } else if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        }

        // Check users exist
        let storage = self.bin_storage.lock().await;
        let user_key = self.metadata.get_user_exist_key();
        let bin = storage.as_ref().bin(whom).await?;
        if !bin.get(&user_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let user_key = self.metadata.get_user_exist_key();
        let bin = storage.as_ref().bin(who).await?;
        if !bin.get(&user_key).await?.is_some() {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        // Check is following
        let follow_key = self.metadata.get_user_follows_key();
        let follows = self.distinct_follows(bin.list_get(&follow_key).await?.0);
        if !follows.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        // Get clock for future op
        let server_clock = bin.clock(0).await?;
        println!("Unfollow with clock: {}", server_clock);

        // Append to follow log
        let follow_log_key = self.metadata.get_user_follow_logs_key(whom);
        let log_kv = KeyValue {
            key: follow_log_key,
            value: format!("u::{}", server_clock.to_string()),
        };
        let _ = bin.list_append(&log_kv).await?;

        // Remove from follow list - it's atomic
        let follow_key = self.metadata.get_user_follows_key();
        let kv = KeyValue {
            key: follow_key.to_string(),
            value: whom.to_string(),
        };
        println!("{} unfollows {}", who, whom);

        if bin.list_remove(&kv).await? == 0 {
            // Not the first unfollow, do clean up, ok to fail
            let _ = bin.list_remove(&log_kv).await?;
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        Ok(())
    }
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        println!("Call is_following");
        // Check user name
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        } else if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        }

        // Check follow list
        let storage = self.bin_storage.lock().await;
        let bin = storage.as_ref().bin(who).await?;
        let follow_key = self.metadata.get_user_follows_key();
        let follows = bin.list_get(&follow_key).await?.0;
        let follows = self.distinct_follows(follows);
        if follows.contains(&whom.to_string()) {
            println!("{} is following: {}", who, whom);
            Ok(true)
        } else {
            println!("{} is not following: {}", who, whom);
            Ok(false)
        }
    }
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        println!("Call following");
        // Check user name
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }

        // Get follow list
        let storage = self.bin_storage.lock().await;
        let bin = storage.as_ref().bin(who).await?;
        let follow_key = self.metadata.get_user_follows_key();
        let follows = bin.list_get(&follow_key).await?.0;
        let follows = self.distinct_follows(follows);
        let mut follows: Vec<String> = follows.into_iter().collect();

        // Sort by alphabetical order
        follows.sort();

        println!("{} following: {:?}", who, follows);
        Ok(follows)
    }

    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        println!("Call home");
        // Check user name
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }

        // Get follow list
        let storage = self.bin_storage.lock().await;
        let bin = storage.as_ref().bin(user).await?;
        let follow_key = self.metadata.get_user_follows_key();
        let follows = bin.list_get(&follow_key).await?.0;
        let distinct_follows: HashSet<String> = follows.iter().map(|v| v.to_string()).collect();
        let trib_key = self.metadata.get_user_tribs_key();
        let my_tribs = bin.list_get(&trib_key).await?.0;

        // Gather tribs from follows
        let mut tribs: Vec<Arc<Trib>> = Vec::new();
        for follow in distinct_follows.iter() {
            let user_bin = storage.as_ref().bin(follow).await?;
            let user_tribs = user_bin
                .list_get(&self.metadata.get_user_tribs_key())
                .await?
                .0;
            let mut follow_tribs: Vec<Arc<Trib>> = Vec::new();
            for trib in user_tribs.iter() {
                follow_tribs.push(Arc::new(self.str_to_trib(follow, trib)?));
            }
            tribs.append(&mut follow_tribs);
        }

        // Add user trib to the timeline
        for trib in my_tribs.iter() {
            tribs.push(Arc::new(self.str_to_trib(user, trib)?));
        }

        // Sort tribs
        tribs.sort_by(|a, b| {
            Ord::cmp(&a.clock, &b.clock)
                .then(Ord::cmp(&a.time, &b.time))
                .then(Ord::cmp(&a.user, &b.user))
                .then(Ord::cmp(&a.message, &b.message))
        });

        // Return at most max_trib_fetch
        let tribs: Vec<Arc<Trib>> = tribs.drain(..min(tribs.len(), MAX_TRIB_FETCH)).collect();

        Ok(tribs)
    }
}
