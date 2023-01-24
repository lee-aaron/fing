use tribbler::colon::escape;

// Strings for reading various system properties
pub struct FrontMetadata {
    // Associated with each backend

    // Default user_name
    pub default_server: String,

    // Access all users in the system
    pub all_users_key: String,

    // Associated with each user
    // Access a user and their tribs
    pub exist_key: String,
    pub tribs_key: String,
    pub follow_key: String,
    pub follow_log_key: String,
}

impl FrontMetadata {
    pub fn new() -> Self {
        FrontMetadata {
            default_server: "_metadata".to_string(),
            all_users_key: "_users".to_string(),
            exist_key: "_exist".to_string(),
            tribs_key: "_tribs".to_string(),
            follow_key: "_follow".to_string(),
            follow_log_key: "_log".to_string(),
        }
    }
    pub fn get_user_exist_key(&self) -> String {
        return self.exist_key.clone();
    }
    pub fn get_user_tribs_key(&self) -> String {
        return self.tribs_key.clone();
    }
    pub fn get_user_follows_key(&self) -> String {
        return self.follow_key.clone();
    }
    pub fn get_user_follow_logs_key(&self, follow: &str) -> String {
        format!("{}::{}", self.follow_log_key, escape(follow))
    }
    pub fn get_all_users_key(&self) -> String {
        format!("{}::{}", self.default_server, self.all_users_key)
    }
}
