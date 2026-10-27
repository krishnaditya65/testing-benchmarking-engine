//! QUIC/HTTP-3 request/response client, modeled on quinn's `Endpoint` /
//! `Connection` / `SendStream` / `RecvStream` API surface.
//!
//! This is the "universal protocol client" bullet's QUIC-HTTP3 slice: REST, GraphQL,
//! gRPC, and WebSocket clients live in the Go sibling
//! `services/execution-fleet/protocol-load-worker-go/`, since those protocols are
//! better served by Go's HTTP/2 and net/http ecosystem than by hand-rolled Rust
//! clients. This crate owns the raw-transport, max-throughput end of the spectrum.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::time::timeout;
use tracing::{debug, instrument};

/// Errors surfaced by [`QuicHttp3Client`]. Kept distinct from `anyhow::Error` because
/// callers (e.g. a load-phase runner deciding whether to retry) need to branch on
/// *why* a request failed, not just log it.
#[derive(Debug, Error)]
pub enum QuicHttp3ClientError {
    #[error("failed to bind local QUIC endpoint: {0}")]
    EndpointBind(String),
    #[error("QUIC handshake to {addr} did not complete within {timeout_ms}ms")]
    HandshakeTimeout { addr: SocketAddr, timeout_ms: u64 },
    #[error("QUIC connection to {addr} failed: {reason}")]
    ConnectionFailed { addr: SocketAddr, reason: String },
    #[error("stream open failed: {0}")]
    StreamOpenFailed(String),
    #[error("request write failed: {0}")]
    RequestWriteFailed(String),
    #[error("response read failed: {0}")]
    ResponseReadFailed(String),
}

/// A minimal HTTP/3-style request: a request-line-equivalent plus a body. Real HTTP/3
/// framing (QPACK-compressed headers, DATA frames per RFC 9114) is deliberately not
/// modeled here - see the NOTE on [`QuicHttp3Client::send_request`].
#[derive(Debug, Clone)]
pub struct QuicRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

/// The response read back off a QUIC stream for one [`QuicRequest`].
#[derive(Debug, Clone)]
pub struct QuicResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// TLS/ALPN configuration a real deployment must supply. Held here as plain data
/// rather than a live `rustls::ClientConfig` so this starter crate compiles without a
/// trust store or cert material wired up.
#[derive(Debug, Clone)]
pub struct QuicClientTlsConfig {
    /// ALPN protocol IDs offered during the QUIC/TLS handshake. Real HTTP/3 requires
    /// exactly `h3` (RFC 9114 ยง3.1) here; anything else and a compliant HTTP/3 server
    /// will reject the connection during ALPN negotiation.
    pub alpn_protocols: Vec<Vec<u8>>,
    /// Whether the peer's certificate chain must validate against a trust store.
    /// Load-testing tools frequently need to disable this against self-signed test
    /// fixtures - real deployments must never default this to `false`.
    pub verify_peer_certificate: bool,
}

impl Default for QuicClientTlsConfig {
    fn default() -> Self {
        Self {
            alpn_protocols: vec![b"h3".to_vec()],
            verify_peer_certificate: true,
        }
    }
}

/// A QUIC/HTTP-3 client saturating one target with request/response traffic over
/// QUIC streams. Mirrors quinn's shape (an `Endpoint` that dials `Connection`s, each
/// of which opens bidirectional streams) so swapping this scaffolding for real `quinn`
/// calls is a mechanical, not structural, change.
///
/// NOTE: production wiring needs a real `quinn::Endpoint::client(bind_addr)` bound to
/// an ephemeral UDP socket, a `rustls::ClientConfig` built from
/// `QuicClientTlsConfig` (webpki-roots or a custom `RootCertStore`, ALPN set to
/// `["h3"]`), and `quinn::ClientConfig::new(Arc::new(tls_config))` passed to
/// `endpoint.connect(addr, server_name)`. None of that is implemented here; this
/// struct models the call sequence and concurrency shape only.
pub struct QuicHttp3Client {
    /// Local bind address for the (simulated) QUIC UDP endpoint.
    bind_addr: SocketAddr,
    tls: Arc<QuicClientTlsConfig>,
    handshake_timeout: Duration,
    request_timeout: Duration,
}

impl QuicHttp3Client {
    /// Constructs a client that will bind its (simulated) QUIC endpoint at
    /// `bind_addr` and use `tls` for the handshake. Mirrors
    /// `quinn::Endpoint::client(bind_addr)` construction.
    pub fn new(bind_addr: SocketAddr, tls: QuicClientTlsConfig) -> Self {
        Self {
            bind_addr,
            tls: Arc::new(tls),
            handshake_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = d;
        self
    }

    pub fn with_request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    /// Establishes a QUIC connection to `addr`, mirroring
    /// `endpoint.connect(addr, server_name)?.await`. Returns a [`QuicConnectionHandle`]
    /// representing what would be a `quinn::Connection` in a real implementation.
    #[instrument(skip(self), fields(alpn = ?self.tls.alpn_protocols))]
    pub async fn connect(
        &self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<QuicConnectionHandle, QuicHttp3ClientError> {
        debug!(bind_addr = %self.bind_addr, %server_name, "opening quic endpoint + dialing");

        // NOTE: a real implementation binds a UDP socket here
        // (`quinn::Endpoint::client(self.bind_addr)`), builds a
        // `quinn::ClientConfig` from `self.tls` (rustls `ClientConfig` with the
        // configured ALPN + trust store), then calls
        // `endpoint.connect_with(client_config, addr, server_name)`. The
        // `.await` on the resulting `Connecting` future is where the QUIC/TLS 1.3
        // handshake (1-RTT, or 0-RTT if resuming) actually happens on the wire.
        let handshake = simulate_quic_handshake(addr, self.handshake_timeout);

        match timeout(self.handshake_timeout, handshake).await {
            Ok(Ok(())) => Ok(QuicConnectionHandle {
                remote_addr: addr,
                request_timeout: self.request_timeout,
            }),
            Ok(Err(reason)) => Err(QuicHttp3ClientError::ConnectionFailed { addr, reason }),
            Err(_) => Err(QuicHttp3ClientError::HandshakeTimeout {
                addr,
                timeout_ms: self.handshake_timeout.as_millis() as u64,
            }),
        }
    }
}

/// Handle to an established QUIC connection, standing in for `quinn::Connection`.
/// Every request opens a fresh bidirectional stream, matching how HTTP/3 multiplexes
/// independent request/response exchanges over one QUIC connection without
/// head-of-line blocking between them.
pub struct QuicConnectionHandle {
    remote_addr: SocketAddr,
    request_timeout: Duration,
}

impl QuicConnectionHandle {
    /// Opens a bidirectional stream (mirrors `connection.open_bi().await`), writes the
    /// request, and reads back the response.
    ///
    /// NOTE: real HTTP/3 framing is significantly more involved than "write bytes,
    /// read bytes": headers are QPACK-compressed onto the stream as a HEADERS frame
    /// (RFC 9114 ยง4.2.1 + RFC 9204 for QPACK), the body follows as DATA frame(s), and
    /// the connection's stream 3 (or the configured control stream) carries SETTINGS
    /// that must be exchanged before any request stream is used. This method models
    /// only the request/response round trip shape, not the framing.
    #[instrument(skip(self, req), fields(remote = %self.remote_addr, path = %req.path))]
    pub async fn send_request(
        &self,
        req: QuicRequest,
    ) -> Result<QuicResponse, QuicHttp3ClientError> {
        let (mut send, mut recv) = simulate_open_bi_stream()
            .await
            .map_err(QuicHttp3ClientError::StreamOpenFailed)?;

        timeout(self.request_timeout, send.write_request(&req))
            .await
            .map_err(|_| QuicHttp3ClientError::RequestWriteFailed("write timed out".into()))?
            .map_err(QuicHttp3ClientError::RequestWriteFailed)?;

        // Real quinn usage calls `send.finish()?` here to signal end-of-request on the
        // stream before awaiting the response.
        send.finish();

        let response = timeout(self.request_timeout, recv.read_response())
            .await
            .map_err(|_| QuicHttp3ClientError::ResponseReadFailed("read timed out".into()))?
            .map_err(QuicHttp3ClientError::ResponseReadFailed)?;

        Ok(response)
    }
}

/// Stand-in for `quinn::SendStream`.
pub struct SimulatedSendStream {
    finished: bool,
}

impl SimulatedSendStream {
    async fn write_request(&mut self, req: &QuicRequest) -> Result<(), String> {
        // NOTE: real usage is `send_stream.write_all(&framed_bytes).await`, where
        // `framed_bytes` is the QPACK-encoded HEADERS frame followed by DATA frame(s)
        // built from `req.body`.
        debug!(method = %req.method, path = %req.path, body_len = req.body.len(), "quic stream write");
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

/// Stand-in for `quinn::RecvStream`.
pub struct SimulatedRecvStream;

impl SimulatedRecvStream {
    async fn read_response(&mut self) -> Result<QuicResponse, String> {
        // NOTE: real usage is `recv_stream.read_to_end(max_size).await` (or a
        // streaming read loop for large bodies), followed by QPACK-decoding the
        // HEADERS frame to recover the status code before the DATA frame body.
        Ok(QuicResponse {
            status: 200,
            body: Vec::new(),
        })
    }
}

/// Simulates the latency and failure surface of a QUIC/TLS 1.3 handshake without a
/// live UDP socket, so this starter crate is runnable/testable without real infra.
async fn simulate_quic_handshake(_addr: SocketAddr, _budget: Duration) -> Result<(), String> {
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(())
}

/// Simulates `connection.open_bi().await`.
async fn simulate_open_bi_stream() -> Result<(SimulatedSendStream, SimulatedRecvStream), String> {
    Ok((
        SimulatedSendStream { finished: false },
        SimulatedRecvStream,
    ))
}
