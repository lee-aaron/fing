use std::net::IpAddr;
use std::time::Duration;
use std::{future::Future, sync::Arc};

use quinn::IdleTimeout;
use rustls::{Certificate, ClientConfig, ServerName};
use serde::{Deserialize, Serialize};

pub const DEFAULT_INITIAL_RETRY_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(feature = "structopt")]
const DEFAULT_INITIAL_RETRY_INTERVAL_STR: &str = "500";

pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(18);

pub const DEFAULT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(feature = "structopt")]
const DEFAULT_MAX_RETRY_INTERVAL_STR: &str = "15";

pub const DEFAULT_RETRY_INTERVAL_MULTIPLIER: f64 = 1.5;
#[cfg(feature = "structopt")]
const DEFAULT_RETRY_INTERVAL_MULTIPLIER_STR: &str = "1.5";

pub const DEFAULT_RETRY_DELAY_RAND_FACTOR: f64 = 0.3;
#[cfg(feature = "structopt")]
const DEFAULT_RETRY_DELAY_RAND_FACTOR_STR: &str = "0.3";

pub const DEFAULT_RETRYING_MAX_ELAPSED_TIME: Duration = Duration::from_secs(30);
#[cfg(feature = "structopt")]
const DEFAULT_RETRYING_MAX_ELAPSED_TIME_STR: &str = "30";

pub(crate) const SERVER_NAME: &str = "localhost";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// An error occurred when generating the TLS certificate.
    #[error("An error occurred when generating the TLS certificate")]
    CertificateGeneration(#[from] CertificateGenerationError),
    /// Invalid idle timeout
    #[error("An error occurred parsing idle timeout duration")]
    InvalidIdleTimeout(#[from] quinn_proto::VarIntBoundsExceeded),
    /// rustls error
    #[error("An error occurred within rustls")]
    Rustls(#[from] rustls::Error),
    /// rustls error
    #[error("An error occurred generaeting client config certificates")]
    Webpki,
}

impl From<rcgen::RcgenError> for ConfigError {
    fn from(error: rcgen::RcgenError) -> Self {
        Self::CertificateGeneration(CertificateGenerationError(error.into()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct CertificateGenerationError(
    // Though there are multiple different errors that could occur by the code, since we are
    // generating a certificate, they should only really occur due to buggy implementations. As
    // such, we don't attempt to expose more detail than 'something went wrong', which will
    // hopefully be enough for someone to file a bug report...
    Box<dyn std::error::Error + Send + Sync>,
);

type Result<T, E = ConfigError> = std::result::Result<T, E>;

#[cfg_attr(feature = "structopt", derive(StructOpt))]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Config {
    #[cfg_attr(feature = "structopt", structopt(long))]
    pub external_port: Option<u16>,

    #[cfg_attr(feature = "structopt", structopt(long))]
    pub external_ip: Option<IpAddr>,

    #[serde(default)]
    #[cfg_attr(feature = "structopt", structopt(long, parse(try_from_str = parse_millis), value_name = "MILLIS"))]
    pub idle_timeout: Option<Duration>,

    #[serde(default)]
    #[cfg_attr(feature = "structopt", structopt(long, parse(try_from_str = parse_millis), value_name = "MILLIS"))]
    pub keep_alive_interval: Option<Duration>,

    #[serde(default)]
    #[cfg_attr(feature = "structopt", structopt(flatten))]
    pub retry_config: RetryConfig,
}

#[cfg(feature = "structopt")]
fn parse_millis(millis: &str) -> Result<Duration, std::num::ParseIntError> {
    Ok(Duration::from_millis(millis.parse()?))
}

#[derive(Clone, Debug, Copy, Serialize, Deserialize)]
pub struct RetryConfig {
    #[cfg_attr(feature = "structopt", structopt(long, default_value = DEFAULT_INITIAL_RETRY_INTERVAL_STR, parse(try_from_str = parse_millis), value_name = "MILLIS"))]
    pub initial_retry_interval: Duration,

    #[cfg_attr(feature = "structopt", structopt(long, default_value = DEFAULT_MAX_RETRY_INTERVAL_STR, parse(try_from_str = parse_millis), value_name = "MILLIS"))]
    pub max_retry_interval: Duration,

    #[cfg_attr(feature = "structopt", structopt(long, default_value = DEFAULT_RETRY_INTERVAL_MULTIPLIER_STR))]
    pub retry_delay_multiplier: f64,

    #[cfg_attr(feature = "structopt", structopt(long, default_value = DEFAULT_RETRY_DELAY_RAND_FACTOR_STR))]
    pub retry_delay_rand_factor: f64,

    #[cfg_attr(feature = "structopt", structopt(long, default_value = DEFAULT_RETRYING_MAX_ELAPSED_TIME_STR, parse(try_from_str = parse_millis), value_name = "MILLIS"))]
    pub retrying_max_elapsed_time: Duration,
}

impl RetryConfig {
    // Perform `op` and retry on errors as specified by this configuration.
    //
    // Note that `backoff::Error<E>` implements `From<E>` for any `E` by creating a
    // `backoff::Error::Transient`, meaning that errors will be retried unless explicitly returning
    // `backoff::Error::Permanent`.
    pub(crate) fn retry<R, E, Fn, Fut>(&self, op: Fn) -> impl Future<Output = Result<R, E>>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<R, backoff::Error<E>>>,
    {
        let backoff = backoff::ExponentialBackoff {
            initial_interval: self.initial_retry_interval,
            randomization_factor: self.retry_delay_rand_factor,
            multiplier: self.retry_delay_multiplier,
            max_interval: self.max_retry_interval,
            max_elapsed_time: Some(self.retrying_max_elapsed_time),
            ..Default::default()
        };
        backoff::future::retry(backoff, op)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_retry_interval: DEFAULT_INITIAL_RETRY_INTERVAL,
            max_retry_interval: DEFAULT_MAX_RETRY_INTERVAL,
            retry_delay_multiplier: DEFAULT_RETRY_INTERVAL_MULTIPLIER,
            retry_delay_rand_factor: DEFAULT_RETRY_DELAY_RAND_FACTOR,
            retrying_max_elapsed_time: DEFAULT_RETRYING_MAX_ELAPSED_TIME,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InternalConfig {
    pub(crate) client: quinn::ClientConfig,
    pub(crate) server: quinn::ServerConfig,
    pub(crate) external_port: Option<u16>,
    pub(crate) external_ip: Option<IpAddr>,
    pub(crate) retry_config: Arc<RetryConfig>,
}

impl InternalConfig {
    pub(crate) fn try_from_config(config: Config) -> Result<Self> {
        let default_idle_timeout: IdleTimeout = IdleTimeout::try_from(DEFAULT_IDLE_TIMEOUT)?;

        let idle_timeout = config
            .idle_timeout
            .map(IdleTimeout::try_from)
            .unwrap_or(Ok(default_idle_timeout))
            .map_err(ConfigError::from)?;

        let keep_alive_interval = config.keep_alive_interval;

        let transport = Self::new_transport_config(idle_timeout, keep_alive_interval);

        let mut roots = rustls::RootCertStore::empty();
        let (cert, key) = Self::generate_cert()?;
        roots.add(&cert).map_err(|_e| ConfigError::Webpki)?;

        let mut client_crypto = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth();

        client_crypto
            .dangerous()
            .set_certificate_verifier(Arc::new(SkipCertificateVerification));

        let mut server = quinn::ServerConfig::with_single_cert(vec![cert], key)?;
        server.transport = transport.clone();

        let mut client = quinn::ClientConfig::new(Arc::new(client_crypto));
        client.transport = transport;

        Ok(Self {
            client,
            server,
            external_port: config.external_port,
            external_ip: config.external_ip,
            retry_config: Arc::new(config.retry_config),
        })
    }

    fn new_transport_config(
        idle_timeout: IdleTimeout,
        keep_alive_interval: Option<Duration>,
    ) -> Arc<quinn::TransportConfig> {
        let mut config = quinn::TransportConfig::default();

        let _ = config.max_idle_timeout(Some(idle_timeout));
        let _ = config.keep_alive_interval(keep_alive_interval);

        Arc::new(config)
    }

    fn generate_cert() -> Result<(Certificate, rustls::PrivateKey)> {
        let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])?;

        let key = cert.serialize_private_key_der();
        let cert = cert.serialize_der().unwrap();

        let key = rustls::PrivateKey(key);
        let cert = Certificate(cert);
        Ok((cert, key))
    }
}

struct SkipCertificateVerification;

impl rustls::client::ServerCertVerifier for SkipCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
