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
        self.connection.send((msg.clone(), Bytes::new(), Bytes::from(content))).await
    }
}
