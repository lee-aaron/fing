use std::{fmt, net::SocketAddr, pin::Pin, sync::Arc, task, time::Duration};

use futures::{
    future,
    stream::{self, Stream, StreamExt, TryStream, TryStreamExt},
};
use tokio::{
    sync::{mpsc, watch, Mutex},
    time::timeout,
};
use tracing::{trace, warn};

use crate::quic::{config::SERVER_NAME, error::RpcError};

use super::{
    config::RetryConfig,
    error::{ConnectionError, RecvError, SendError, SerializationError, StreamError},
    wire_msg::{UsrMsgBytes, WireMsg},
};

const INCOMING_MESSAGE_BUFFER_LEN: usize = 10_000;

const ENDPOINT_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(30);

const CLOSED_CONNECTION: &str = "The connection was closed intentionally.";

type ResponseStream = Arc<Mutex<SendStream>>;

#[derive(Clone)]
pub struct Connection {
    inner: quinn::Connection,
    default_retry_config: Option<Arc<RetryConfig>>,

    // A reference to the 'alive' marker for the connection. This isn't read by `Connection`, but
    // must be held to keep background listeners alive until both halves of the connection are
    // dropped.
    _alive_tx: Arc<watch::Sender<()>>,
}

impl Connection {
    pub(crate) fn new(
        endpoint: quinn::Endpoint,
        default_retry_config: Option<Arc<RetryConfig>>,
        connection: quinn::NewConnection,
    ) -> (Connection, ConnectionIncoming) {
        // this channel serves to keep the background message listener alive so long as one side of
        // the connection API is alive.
        let (alive_tx, alive_rx) = watch::channel(());
        let alive_tx = Arc::new(alive_tx);
        let peer_address = connection.connection.remote_address();

        (
            Self {
                inner: connection.connection,
                default_retry_config,
                _alive_tx: Arc::clone(&alive_tx),
            },
            ConnectionIncoming::new(
                endpoint,
                peer_address,
                connection.uni_streams,
                connection.bi_streams,
                alive_tx,
                alive_rx,
            ),
        )
    }

    /// A stable identifier for the connection.
    ///
    /// This ID will not change for the lifetime of the connection to a given ip.
    ///
    /// The ID pulls the internal conneciton id and concats with the SocketAddr of
    /// the peer. So this _should_ be unique per peer (without IP spoofing).
    ///
    pub fn id(&self) -> String {
        let socket = self.remote_address();

        format!("{}{}", socket, self.inner.stable_id())
    }

    /// The address of the remote peer.
    pub fn remote_address(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    /// Send a message to the peer with default retry configuration.
    ///
    /// The message will be sent on a unidirectional QUIC stream, meaning the application is
    /// responsible for correlating any anticipated responses from incoming streams.
    ///
    /// The priority will be `0` and retry behaviour will be determined by the
    /// [`Config`](crate::Config) that was used to construct the [`Endpoint`] this connection
    /// belongs to. See [`send_with`](Self::send_with) if you want to send a message with specific
    /// configuration.
    pub async fn send(&self, bytes: UsrMsgBytes) -> Result<(), SendError> {
        self.send_with(bytes, 0, None).await
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    pub async fn send_with(
        &self,
        user_msg_bytes: UsrMsgBytes,
        priority: i32,
        retry_config: Option<&RetryConfig>,
    ) -> Result<(), SendError> {
        match retry_config.or(self.default_retry_config.as_deref()) {
            Some(retry_config) => {
                retry_config
                    .retry(|| async {
                        self.send_uni(user_msg_bytes.clone(), priority)
                            .await
                            .map_err(|error| match &error {
                                // don't retry on connection loss, since we can't recover that from here
                                SendError::ConnectionLost(_) => backoff::Error::Permanent(error),
                                _ => backoff::Error::Transient {
                                    err: error,
                                    retry_after: Some(retry_config.initial_retry_interval),
                                },
                            })
                    })
                    .await?;
            }
            None => {
                self.send_uni(user_msg_bytes, priority).await?;
            }
        }
        Ok(())
    }

    /// Open a unidirection stream to the peer.
    ///
    /// Messages sent over the stream will arrive at the peer in the order they were sent.
    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let send_stream = self.inner.open_uni().await?;
        Ok(SendStream::new(send_stream))
    }

    /// Open a bidirectional stream to the peer.
    ///
    /// Bidirectional streams allow messages to be sent in both directions. This can be useful to
    /// automatically correlate response messages, for example.
    ///
    /// Messages sent over the stream will arrive at the peer in the order they were sent.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (send_stream, recv_stream) = self.inner.open_bi().await?;
        Ok((SendStream::new(send_stream), RecvStream::new(recv_stream)))
    }

    /// Close the connection immediately.
    ///
    /// This is not a graceful close - pending operations will fail immediately with
    /// [`ConnectionError::Closed`]`(`[`Close::Local`]`)`, and data on unfinished streams is not
    /// guaranteed to be delivered.
    pub fn close(&self, reason: Option<String>) {
        let reason = reason.unwrap_or_else(|| CLOSED_CONNECTION.to_string());
        self.inner.close(0u8.into(), &reason.into_bytes());
    }

    /// Opens a uni directional stream and sends message on this stream
    async fn send_uni(&self, user_msg_bytes: UsrMsgBytes, priority: i32) -> Result<(), SendError> {
        let mut send_stream = self.open_uni().await.map_err(SendError::ConnectionLost)?;
        send_stream.set_priority(priority);

        send_stream.send_user_msg(user_msg_bytes).await?;

        // We try to make sure the stream is gracefully closed and the bytes get sent, but if it
        // was already closed (perhaps by the peer) then we ignore the error.
        // TODO: we probably shouldn't ignore the error...
        send_stream.finish().await.or_else(|err| match err {
            SendError::StreamLost(StreamError::Stopped(_)) => Ok(()),
            _ => Err(err),
        })?;

        Ok(())
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id())
            .field("remote_address", &self.remote_address())
            .finish_non_exhaustive()
    }
}

pub struct SendStream {
    inner: quinn::SendStream,
}

impl SendStream {
    fn new(inner: quinn::SendStream) -> Self {
        Self { inner }
    }

    /// Set the priority of the send stream.
    ///
    /// Every send stream has an initial priority of 0. Locally buffered data from streams with
    /// higher priority will be transmitted before data from streams with lower priority. Changing
    /// the priority of a stream with pending data may only take effect after that data has been
    /// transmitted. Using many different priority levels per connection may have a negative impact
    /// on performance.
    pub fn set_priority(&self, priority: i32) {
        // quinn returns `UnknownStream` error if the stream does not exist. We ignore it, on the
        // basis that operations on the stream will fail instead (and the effect of setting priority
        // or not is only observable if the stream exists).
        let _ = self.inner.set_priority(priority);
    }

    /// Send a message over the stream to the peer.
    ///
    /// Messages sent over the stream will arrive at the peer in the order they were sent.
    pub async fn send_user_msg(&mut self, user_msg_bytes: UsrMsgBytes) -> Result<(), SendError> {
        WireMsg::UserMsg(user_msg_bytes)
            .write_to_stream(&mut self.inner)
            .await
    }

    /// Shut down the send stream gracefully.
    ///
    /// The returned future will complete once the peer has acknowledged all sent data.
    pub async fn finish(&mut self) -> Result<(), SendError> {
        self.inner.finish().await?;
        Ok(())
    }

    pub(crate) async fn send_wire_msg(&mut self, msg: WireMsg) -> Result<(), SendError> {
        msg.write_to_stream(&mut self.inner).await
    }
}

impl fmt::Debug for SendStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SendStream").finish_non_exhaustive()
    }
}

pub struct RecvStream {
    inner: quinn::RecvStream,
}

impl RecvStream {
    fn new(inner: quinn::RecvStream) -> Self {
        Self { inner }
    }

    /// Get the next message sent by the peer over this stream.
    pub async fn next(&mut self) -> Result<UsrMsgBytes, RecvError> {
        match self.next_wire_msg().await? {
            Some(WireMsg::UserMsg(msg)) => Ok(msg),
            msg => Err(SerializationError::unexpected(&msg).into()),
        }
    }

    pub(crate) async fn next_wire_msg(&mut self) -> Result<Option<WireMsg>, RecvError> {
        WireMsg::read_from_stream(&mut self.inner).await
    }
}

impl fmt::Debug for RecvStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RecvStream").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ConnectionIncoming {
    message_rx: mpsc::Receiver<Result<(UsrMsgBytes, Option<ResponseStream>), RecvError>>,
    _alive_tx: Arc<watch::Sender<()>>,
}

impl ConnectionIncoming {
    fn new(
        endpoint: quinn::Endpoint,
        peer_addr: SocketAddr,
        uni_streams: quinn::IncomingUniStreams,
        bi_streams: quinn::IncomingBiStreams,
        alive_tx: Arc<watch::Sender<()>>,
        alive_rx: watch::Receiver<()>,
    ) -> Self {
        let (message_tx, message_rx) = mpsc::channel(INCOMING_MESSAGE_BUFFER_LEN);

        // offload the actual message handling to a background task - the task will exit when
        // `alive_tx` is dropped, which would be when both sides of the connection are dropped.
        start_message_listeners(
            endpoint,
            peer_addr,
            uni_streams,
            bi_streams,
            alive_rx,
            message_tx,
        );

        Self {
            message_rx,
            _alive_tx: alive_tx,
        }
    }

    /// Get the next message sent by the peer, over any stream.
    pub async fn next(&mut self) -> Result<Option<UsrMsgBytes>, RecvError> {
        if let Some((bytes, _opt)) = self.next_with_stream().await? {
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    /// Get the next message sent by the peer, over any stream along with the stream to respond with.
    pub async fn next_with_stream(
        &mut self,
    ) -> Result<Option<(UsrMsgBytes, Option<ResponseStream>)>, RecvError> {
        self.message_rx.recv().await.transpose()
    }
}

// Start listeners in background tokio tasks. These tasks will run until they terminate, which would
// be when the connection terminates, or all connection handles are dropped.
//
// `alive_tx` is used to detect when all connection handles are dropped.
// `message_tx` is used to exfiltrate messages and stream errors.
fn start_message_listeners(
    endpoint: quinn::Endpoint,
    peer_addr: SocketAddr,
    uni_streams: quinn::IncomingUniStreams,
    bi_streams: quinn::IncomingBiStreams,
    alive_rx: watch::Receiver<()>,
    message_tx: mpsc::Sender<Result<(UsrMsgBytes, Option<ResponseStream>), RecvError>>,
) {
    let _ = tokio::spawn(listen_on_uni_streams(
        peer_addr,
        FilterBenignClose(uni_streams),
        alive_rx.clone(),
        message_tx.clone(),
    ));

    let _ = tokio::spawn(listen_on_bi_streams(
        endpoint,
        peer_addr,
        FilterBenignClose(bi_streams),
        alive_rx,
        message_tx,
    ));
}

async fn listen_on_uni_streams(
    peer_addr: SocketAddr,
    uni_streams: FilterBenignClose<quinn::IncomingUniStreams>,
    mut alive_rx: watch::Receiver<()>,
    message_tx: mpsc::Sender<Result<(UsrMsgBytes, Option<ResponseStream>), RecvError>>,
) {
    trace!(
        "Started listener for incoming uni-streams from {}",
        peer_addr
    );

    let mut uni_messages = Box::pin(
        uni_streams
            .map_ok(|recv_stream| {
                trace!("Handling incoming uni-stream from {}", peer_addr);

                stream::try_unfold(recv_stream, |mut recv_stream| async move {
                    WireMsg::read_from_stream(&mut recv_stream)
                        .await
                        .and_then(|msg| match msg {
                            Some(WireMsg::UserMsg(msg)) => Ok(Some((msg, recv_stream))),
                            None => Ok(None),
                            _ => Err(SerializationError::unexpected(&msg).into()),
                        })
                })
            })
            .try_flatten(),
    );

    // it's a shame to allocate, but there are `Pin` errors otherwise ??? and we should only be doing
    // this once (per connection).
    let mut alive = Box::pin(alive_rx.changed());

    while let Some(result) = {
        match future::select(uni_messages.next(), &mut alive).await {
            future::Either::Left((result, _)) => result,
            future::Either::Right((Ok(_), pending_message)) => {
                // we don't expect to actually send a value over the alive channel, so just ignore
                pending_message.await
            }
            future::Either::Right((Err(_), _)) => {
                // the connection has been dropped
                // TODO: should we just drop pending messages here? if not, how long do we wait?
                trace!(
                    "Stopped listener for incoming uni-streams from {}: connection handles dropped",
                    peer_addr
                );
                None
            }
        }
    } {
        let mut break_ = false;

        if let Err(RecvError::ConnectionLost(_)) = &result {
            // if the connection is lost, we should stop processing (after sending the error)
            break_ = true;
        }

        if message_tx.send(result.map(|b| (b, None))).await.is_err() {
            // if we can't send the result, the receiving end is closed so we should stop processing
            break_ = true;
        }

        if break_ {
            break;
        }
    }
    trace!(
        "Stopped listener for incoming uni-streams from {}: stream finished",
        peer_addr
    );
}

async fn listen_on_bi_streams(
    endpoint: quinn::Endpoint,
    peer_addr: SocketAddr,
    bi_streams: FilterBenignClose<quinn::IncomingBiStreams>,
    mut alive_rx: watch::Receiver<()>,
    message_tx: mpsc::Sender<Result<(UsrMsgBytes, Option<ResponseStream>), RecvError>>,
) {
    trace!(
        "Started listener for incoming bi-streams from {}",
        peer_addr
    );

    let streaming = bi_streams.try_for_each_concurrent(None, |(send_stream, mut recv_stream)| {
        let endpoint = &endpoint;
        let message_tx = &message_tx;
        async move {
            trace!("Handling incoming bi-stream from {}", peer_addr);
            let arc_mutex = Arc::new(Mutex::new(SendStream::new(send_stream)));

            loop {
                match WireMsg::read_from_stream(&mut recv_stream).await {
                    Err(error) => {
                        let mut break_ = false;

                        if let RecvError::ConnectionLost(_) = &error {
                            break_ = true;
                        }

                        if let Err(error) = message_tx.send(Err(error)).await {
                            // if we can't send the result, the receiving end is closed so we should stop
                            trace!("Receiver gone, dropping error: {:?}", error);
                            break_ = true;
                        }

                        if break_ {
                            break;
                        }
                    }
                    Ok(None) => {
                        break;
                    }
                    Ok(Some(WireMsg::UserMsg((header, dst, payload)))) => {
                        if let Err(msg) = message_tx
                            .send(Ok(((header, dst, payload), Some(arc_mutex.clone()))))
                            .await
                        {
                            // if we can't send the result, the receiving end is closed so we should stop
                            trace!("Receiver gone, dropping message: {:?}", msg);
                            break;
                        }
                    }
                    Ok(Some(WireMsg::EndpointEchoReq)) => {
                        if let Err(error) =
                            handle_endpoint_echo(&mut arc_mutex.lock().await.inner, peer_addr).await
                        {
                            // TODO: consider more carefully how to handle this
                            warn!("Error handling endpoint echo request: {}", error);
                        }
                    }
                    Ok(Some(WireMsg::EndpointVerificationReq(addr))) => {
                        if let Err(error) = handle_endpoint_verification(
                            endpoint,
                            &mut arc_mutex.lock().await.inner,
                            addr,
                        )
                        .await
                        {
                            // TODO: consider more carefully how to handle this
                            warn!("Error handling endpoint verification request: {}", error);
                        }
                    }
                    Ok(msg) => {
                        // TODO: consider more carefully how to handle this
                        warn!(
                            "Error on bi-stream: {}",
                            SerializationError::unexpected(&msg)
                        );
                    }
                }
            }

            Ok(())
        }
    });

    // it's a shame to allocate, but there are `Pin` errors otherwise ??? and we should only be doing
    // this once.
    let mut alive = Box::pin(alive_rx.changed());

    match future::select(streaming, &mut alive).await {
        future::Either::Left((Ok(()), _)) => {
            trace!(
                "Stopped listener for incoming bi-streams from {}: stream ended",
                peer_addr
            );
        }
        future::Either::Left((Err(error), _)) => {
            // A connection error occurred on bi_streams, we don't propagate anything here as we
            // expect propagation to be handled in listen_on_uni_streams.
            warn!(
                "Stopped listener for incoming bi-streams from {} due to error: {:?}",
                peer_addr, error
            );
        }
        future::Either::Right((_, _)) => {
            // the connection was closed
            // TODO: should we just drop pending messages here? if not, how long do we wait?
            trace!(
                "Stopped listener for incoming bi-streams from {}: connection handles dropped",
                peer_addr
            );
        }
    }
}

async fn handle_endpoint_echo(
    send_stream: &mut quinn::SendStream,
    peer_addr: SocketAddr,
) -> Result<(), SendError> {
    trace!("Replying to EndpointEchoReq from {}", peer_addr);
    WireMsg::EndpointEchoResp(peer_addr)
        .write_to_stream(send_stream)
        .await
}

async fn handle_endpoint_verification(
    endpoint: &quinn::Endpoint,
    send_stream: &mut quinn::SendStream,
    addr: SocketAddr,
) -> Result<(), SendError> {
    trace!("Performing endpoint verification for {}", addr);

    let verify = async {
        trace!(
            "EndpointVerificationReq: opening new connection to {}",
            addr
        );
        let connection = endpoint
            .connect(addr, SERVER_NAME)
            .map_err(ConnectionError::from)?
            .await?;

        let (mut send_stream, mut recv_stream) = connection.connection.open_bi().await?;
        trace!(
            "EndpointVerificationReq: sending EndpointEchoReq to {} over connection {}",
            addr,
            connection.connection.stable_id()
        );
        WireMsg::EndpointEchoReq
            .write_to_stream(&mut send_stream)
            .await?;

        match WireMsg::read_from_stream(&mut recv_stream).await? {
            Some(WireMsg::EndpointEchoResp(_)) => {
                trace!(
                    "EndpointVerificationReq: Received EndpointEchoResp from {}",
                    addr
                );
                Ok(())
            }
            msg => Err(RecvError::from(SerializationError::unexpected(&msg)).into()),
        }
    };

    let verified: Result<_, RpcError> = timeout(ENDPOINT_VERIFICATION_TIMEOUT, verify)
        .await
        .unwrap_or_else(|error| Err(error.into()));

    if let Err(error) = &verified {
        warn!("Endpoint verification for {} failed: {:?}", addr, error);
    }

    WireMsg::EndpointVerificationResp(verified.is_ok())
        .write_to_stream(send_stream)
        .await?;

    Ok(())
}

struct FilterBenignClose<S>(S);

impl<S> Stream for FilterBenignClose<S>
where
    S: Stream<Item = Result<S::Ok, S::Error>> + TryStream + Unpin,
    S::Error: Into<ConnectionError>,
{
    type Item = Result<S::Ok, ConnectionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
    ) -> task::Poll<Option<Self::Item>> {
        let next = futures::ready!(self.0.poll_next_unpin(ctx));
        task::Poll::Ready(match next.transpose() {
            Ok(next) => next.map(Ok),
            Err(error) => {
                let error = error.into();
                if error.is_benign() {
                    warn!("Benign error ignored {:?}", error);
                    None
                } else {
                    Some(Err(error))
                }
            }
        })
    }
}
