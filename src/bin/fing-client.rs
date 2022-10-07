use std::{
  net::{SocketAddr, ToSocketAddrs},
  path::PathBuf,
  time::Duration,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use clap::Parser;
use common::quic::{Config, Endpoint};
use url::Url;

#[derive(Parser, Debug)]
#[clap(name = "server")]
struct Opt {
  /// Perform NSS-compatible TLS key logging to the file specified in `SSLKEYLOGFILE`.
  #[clap(long = "keylog")]
  keylog: bool,

  url: Url,

  primary: Url,

  /// Override hostname used for certificate verification
  #[clap(long = "host")]
  host: Option<String>,

  /// Custom certificate authority to trust, in DER format
  #[clap(parse(from_os_str), long = "ca")]
  ca: Option<PathBuf>,

  /// Simulate NAT rebinding after connecting
  #[clap(long = "rebind")]
  rebind: bool,
}
fn main() {
  tracing::subscriber::set_global_default(
      tracing_subscriber::FmtSubscriber::builder()
          .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
          .finish(),
  )
  .unwrap();
  let opt = Opt::parse();
  let code = {
      if let Err(e) = run(opt) {
          eprintln!("ERROR: {}", e);
          1
      } else {
          0
      }
  };
  ::std::process::exit(code);
}

#[tokio::main]
async fn run(options: Opt) -> Result<()> {
  const MSG_MARCO: &str = "marco";
  const MSG_POLO: &str = "polo";

  let url = options.url;
  let remote = (url.host_str().unwrap(), url.port().unwrap_or(4433))
      .to_socket_addrs()?
      .next()
      .ok_or_else(|| anyhow!("couldn't resolve to an address"))?;

  let (node, mut incoming, _contact) = Endpoint::new_peer(
      remote,
      &[],
      Config {
          idle_timeout: Duration::from_secs(60).into(),
          ..Default::default()
      },
  )
  .await?;

  let primary_url = options.primary;
  let peer: SocketAddr = (
      primary_url.host_str().unwrap(),
      primary_url.port().unwrap_or(4433),
  )
      .to_socket_addrs()?
      .next()
      .ok_or_else(|| anyhow!("couldn't resolve to an address"))?;
  let msg = Bytes::from(MSG_MARCO);
  println!("Sending to {:?} --> {:?}\n", peer, msg);
  let (conn, mut incoming_conn) = node.connect_to(&peer).await?;
  conn.send((Bytes::new(), Bytes::new(), msg.clone())).await?;
  // `Endpoint` no longer having `connection_pool` to hold established connection.
  // Which means the connection get closed immediately when it reaches end of life span.
  // And causes the receiver side a sending error when reply via the in-coming connection.
  // Hence here have to listen for the reply to avoid such error
  let reply = incoming_conn.next().await?.unwrap();
  println!("Received from {:?} --> {:?}", peer, reply);
  println!("Done sending");

  println!("\n---");
  println!("Listening on: {:?}", node.public_addr());
  println!("---\n");

  while let Some((connection, mut incoming_messages)) = incoming.next().await {
      let src = connection.remote_address();

      // loop over incoming messages
      while let Some((_, _, bytes)) = incoming_messages.next().await? {
          println!("Received from {:?} --> {:?}", src, bytes);
          if bytes == *MSG_MARCO {
              let reply = Bytes::from(MSG_POLO);
              connection
                  .send((Bytes::new(), Bytes::new(), reply.clone()))
                  .await?;
              println!("Replied to {:?} --> {:?}", src, reply);
          }
          println!();
      }
  }

  Ok(())
}
