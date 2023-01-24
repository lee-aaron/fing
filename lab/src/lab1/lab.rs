use std::net::ToSocketAddrs;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

use super::client::StorageClient;
use super::server::StorageServer;

async fn handle_signal(mut shutdown: tokio::sync::mpsc::Receiver<()>) {
    shutdown.recv().await;
}

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let addr = match config.addr.to_socket_addrs() {
        Ok(mut addrs) => addrs.next().unwrap(),
        Err(e) => return Err(e.into()),
    };
    let storage = Mutex::new(config.storage);

    println!("try to initialize server at: {}...", addr.to_string());

    // Get TCP incoming stream
    let incoming: TcpListenerStream = match TcpListener::bind(addr).await {
        Ok(listener) => tokio_stream::wrappers::TcpListenerStream::new(listener),
        Err(e) => {
            println!("Error: {:?}", e);
            log::error!("Error: {:?}", e);
            if config.ready.is_some() {
                config.ready.unwrap().send(false)?;
            }
            return Err(e.into());
        }
    };

    // Build server
    let service = TribStorageServer::new(StorageServer::new(config.addr, storage));

    match config.ready {
        Some(ready) => {
            ready.send(true)?;
        }
        None => (),
    };

    log::info!("Serving requests... {}", addr.to_string());
    match config.shutdown {
        Some(shutdown) => {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, handle_signal(shutdown))
                .await?;
            log::info!("Shutdown backend: {}", addr);
        }
        None => {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await?;
        }
    };

    // 1. How to send ready signal (ie. why timeout)?
    // 2. When is shutdown used?
    // 3. How to report "None" value (ie. use empty string as None)?
    // 4. Do I need to use Mutex? Arc?

    // Optional
    // 5. How to report TribResult Error in RPC calls?
    // 6. How to properly deref a MutexGuard?

    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    let client = StorageClient::new(addr).await;
    match client {
        Ok(v) => Ok(Box::new(v)),
        Err(v) => Err(v),
    }
}
