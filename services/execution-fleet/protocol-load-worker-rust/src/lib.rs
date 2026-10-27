//! `protocol-load-worker-rust` - max-throughput TCP/QUIC socket saturation worker.
//!
//! This crate is one member of the `execution-fleet` layer described in
//! `ARCHITECTURE.md`: a stateless, horizontally scalable worker that takes load-phase
//! orders from the orchestration control plane over Redpanda's `dag.node.dispatch.v1`
//! topic, generates raw TCP/QUIC traffic against a target, and emits OTLP telemetry
//! outward to the OTel Collector. It never calls other workers directly.
//!
//! Module map:
//! - [`saturation`] - TCP connection-pool ramp-up/saturation logic.
//! - [`protocol`] - protocol clients used to drive traffic once a socket exists
//!   (currently QUIC/HTTP-3; REST/gRPC/WebSocket/GraphQL live in the Go sibling
//!   service `protocol-load-worker-go`).
//! - [`telemetry`] - latency capture (eBPF trait + userspace fallback).
//! - [`timing`] - cross-region clock-drift compensation used to make latency
//!   measurements comparable across hosts.

pub mod protocol;
pub mod saturation;
pub mod telemetry;
pub mod timing;
