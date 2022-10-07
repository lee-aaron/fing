use bytes::Bytes;

use crate::quic::{self, SendError};

use super::config::FileConfig;

pub struct File {
    connection: quic::Connection,
}

impl File {
    pub fn new(connection: quic::Connection) -> Self {
        File { connection }
    }

    // Tries to connect to the connection and send content
    pub async fn create(&self, content: String) -> Result<(), SendError> {
        let msg = Bytes::from("create");
        self.connection
            .send((msg.clone(), Bytes::new(), Bytes::from(content)))
            .await
    }

    pub async fn read(&self, file_name: String) -> Result<(), SendError> {
        let msg = Bytes::from("read");
        self.connection
            .send((msg.clone(), Bytes::new(), Bytes::from(file_name)))
            .await
    }

    pub async fn update(&self, file_name: String, content: String) -> Result<(), SendError> {
        let msg = Bytes::from("update");
        self.connection
            .send((msg.clone(), Bytes::new(), Bytes::from(file_name)))
            .await
    }

    pub async fn delete(&self, file_name: String) -> Result<(), SendError> {
        let msg = Bytes::from("delete");
        self.connection
            .send((msg.clone(), Bytes::new(), Bytes::from(file_name)))
            .await
    }

    pub async fn list(&self, prefix: String) -> Result<(), SendError> {
        let msg = Bytes::from("list");
        self.connection
            .send((msg.clone(), Bytes::new(), Bytes::from(prefix)))
            .await
    }
}
