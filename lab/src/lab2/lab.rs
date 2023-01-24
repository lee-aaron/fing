use std::net::ToSocketAddrs;

use tokio::{join, net::TcpListener};
use tribbler::{config::KeeperConfig, err::TribResult, storage::BinStorage, trib::Server};

use crate::{keeper::trib_keeper_server::TribKeeperServer, lab2::keeper::Keeper};

use super::{bin_storage::BinStorageClient, front::FrontClient};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient::new(backs)))
}

async fn handle_signal(mut shutdown: tokio::sync::mpsc::Receiver<()>) {
    shutdown.recv().await;
}
/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let addr = match kc.addrs[kc.this].to_socket_addrs() {
        Ok(mut addrs) => addrs.next().unwrap(),
        Err(e) => return Err(e.into()),
    };

    let incoming = match TcpListener::bind(addr).await {
        Ok(listener) => tokio_stream::wrappers::TcpListenerStream::new(listener),
        Err(e) => {
            println!("{:?}", e);
            log::error!("{:?}", e);
            if kc.ready.is_some() {
                kc.ready.unwrap().send(false)?;
            }
            return Err(e.into());
        }
    };

    let keeper = Keeper::new(kc.backs.clone(), kc.addrs.clone(), kc.this, kc.id).await;

    match kc.ready {
        Some(ready) => ready.send(true)?,
        None => (),
    };

    // Start RPC service
    let rpc_service =
        tonic::transport::Server::builder().add_service(TribKeeperServer::new(keeper.keeper_rpc));

    match kc.shutdown {
        Some(shutdown) => {
            let rpc_handle =
                rpc_service.serve_with_incoming_shutdown(incoming, handle_signal(shutdown));
            // Start daemon and wait for shutdown
            let daemon_handle = tokio::spawn(keeper.keeper_daemon.serve());
            let _ = join!(rpc_handle);
            daemon_handle.abort();
            log::info!("Shutdown keeper: {}", addr);
        }
        None => {
            let rpc_handle = rpc_service.serve_with_incoming(incoming);
            // Start daemon and wait for shutdown
            let daemon_handle = tokio::spawn(keeper.keeper_daemon.serve());
            let _ = join!(rpc_handle);
            daemon_handle.abort();
            log::info!("Shutdown keeper: {}", addr);
        }
    }

    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontClient::new(bin_storage)))
}
