//! Protocol clients that drive traffic once a connection exists.
//!
//! This service owns QUIC/HTTP-3. REST/gRPC/WebSocket/GraphQL clients live in the Go
//! sibling `services/execution-fleet/protocol-load-worker-go/`.

mod quic_http3_client;

pub use quic_http3_client::{QuicHttp3Client, QuicHttp3ClientError, QuicRequest, QuicResponse};
