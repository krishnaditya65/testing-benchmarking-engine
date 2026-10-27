# protocol-load-worker-rust

A tokio-based, max-throughput TCP/QUIC socket saturation worker. Part of the
`execution-fleet` layer (`ARCHITECTURE.md`): stateless, horizontally scalable, takes
load-phase orders from the orchestration control plane over Redpanda's
`dag.node.dispatch.v1` topic, and emits telemetry outward via OTLP to the OTel
Collector DaemonSet. It never calls other workers directly.

## Why this service exists alongside `protocol-load-worker-go`

The execution fleet has two protocol-load workers on purpose (see
`ARCHITECTURE.md` → "Language matches the job, not convention"):

- **`protocol-load-worker-go`** owns the request/response protocol clients where
  developer velocity and a mature HTTP/2 + gRPC ecosystem matter most: REST, gRPC,
  WebSocket, GraphQL.
- **`protocol-load-worker-rust`** (this service) owns the raw-transport,
  max-throughput end: opening and holding thousands of concurrent TCP/QUIC sockets
  per replica with a minimal per-connection memory footprint, where Rust/tokio's lack
  of GC pauses and small async task overhead directly translate into higher achievable
  concurrency per core.

## Implements (feature-manifest → code map)

| Feature-manifest bullet | Implementation | Notes |
|---|---|---|
| "Universal protocol client ... WebTransport/MQTT/QUIC-HTTP3" | `src/protocol/quic_http3_client.rs` (`QuicHttp3Client`) | This service's slice of the universal protocol client is QUIC-HTTP3 request/response saturation, modeled on quinn's `Endpoint`/`Connection`/`SendStream`/`RecvStream` API surface. REST/GraphQL/gRPC/WebSocket live in `protocol-load-worker-go`; WebTransport/MQTT are not yet implemented anywhere in this repo. |
| Raw TCP saturation (supporting infra for the above) | `src/saturation/tcp_connection_pool_saturator.rs` (`TcpConnectionPoolSaturator`) | Semaphore-bounded ramp-up to a target concurrency, per-connection connect/write/read loop, live atomic counters (`SaturationCounters`). |
| "eBPF kernel-level latency capture" | `src/telemetry/ebpf_latency_probe.rs` (`EbpfLatencyProbe` trait + `UserspaceApproximateLatencyProbe`) | The trait defines the real capture contract (`attach`, `read_ttfb_histogram`, `read_tls_handshake_latency`). This crate only ships the unprivileged userspace-timestamp approximation, since this worker deliberately does not run with `CAP_BPF`/`CAP_NET_ADMIN`. Real kernel-level attachment belongs to `services/execution-fleet/chaos-ebpf-sidecar/` (already privileged) or a dedicated eBPF program deployed alongside this worker — see the NOTE comments in that file for the kprobe/XDP attach points a production implementation needs. |
| "Clock-drift compensation (PTP/NTP)" | `src/timing/clock_drift_compensator.rs` (`ClockDriftCompensator`) | Classic NTP offset/round-trip-delay formulas over client/server timestamps (t0–t3), an EWMA-smoothed running offset estimate, and `corrected_now()`/`adjust_timestamp()` so cross-region latency math isn't polluted by per-host clock skew. |

## Explicitly NOT this service

To avoid duplicate/drifting implementations, the following feature-manifest bullets
are owned elsewhere:

- **IPLD / CID resolution, crypto-overhead profiling** — `services/execution-fleet/dag-ipld-benchmark-worker-rust/`.
- **Chaos injection** — `services/execution-fleet/chaos-ebpf-sidecar/` (also the eventual home of real kernel-level eBPF attachment referenced above).
- **Wasm payload generation / user pre/post scripts** — `services/execution-fleet/wasm-payload-sandbox-runtime/`.
- **Fuzzing** — not yet mapped to a service in this repo; do not add it here.
- **REST/gRPC/WebSocket/GraphQL protocol clients** — `services/execution-fleet/protocol-load-worker-go/`.

## Communication contracts (per `ARCHITECTURE.md`)

- **Control plane → this worker**: `dag.node.dispatch.v1` on Redpanda, payload shape
  defined by `shared-libraries/protocol-contracts/proto/dag_dispatch.proto`. *(Not yet
  wired in this starter — see the NOTE at the top of `src/main.rs`.)*
- **This worker → telemetry lake**: OTLP (gRPC) to the OTel Collector DaemonSet.
  *(Not yet wired in this starter; `src/main.rs` currently logs saturation-progress
  snapshots via `tracing` as a placeholder for the real OTLP metric export.)*
- **No worker-to-worker calls.** This service only ever talks to its configured
  target address and (once wired) the control plane/telemetry lake.
- **Checkpointing**: per `ARCHITECTURE.md`, worker state is checkpointed to Redis
  before load-phase transitions. This service holds no state worth checkpointing
  (see `SCALING.md` — sockets are ephemeral); a production build should still confirm
  a load-phase-transition checkpoint hook exists once the control-plane wiring lands,
  so the scheduler's assumptions hold even for state-light workers like this one.

## Local development

```bash
# From this directory:
cargo run -- --target-addr 127.0.0.1:9000 --target-concurrency 500

# Or via env vars (as this worker is configured in Kubernetes):
PLW_TARGET_ADDR=127.0.0.1:9000 PLW_TARGET_CONCURRENCY=500 cargo run
```

No `cargo build`/`cargo check` has been run against this starter (no network access in
the environment it was authored in) — dependency versions in `Cargo.toml` are
hand-written and should be verified on first real build.
