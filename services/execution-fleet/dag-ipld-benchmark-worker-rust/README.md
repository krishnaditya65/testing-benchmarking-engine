# dag-ipld-benchmark-worker-rust

Execution-fleet worker (see [ARCHITECTURE.md](../../../ARCHITECTURE.md)) that benchmarks
DAG/IPLD block traversal and the cryptographic primitives on its hot path, and offers a
bisection-based diagnostic for isolating the root cause of flaky DAG runs. Stateless,
horizontally scalable, takes orders from the control plane over Redpanda's
`dag.node.dispatch.v1` topic, emits results as OTLP telemetry — never talks to sibling
workers directly.

## Implements (feature-manifest → code map)

- **"IPLD block traversal profiling"** — [`src/traversal/ipld_block_traversal_profiler.rs`](./src/traversal/ipld_block_traversal_profiler.rs).
  `IpldBlockTraversalProfiler` walks a DAG of `IpldBlock`s (each identified by a `CidLike`
  content identifier), timing every CID resolution/link-traversal hop with
  `std::time::Instant` at microsecond precision, and aggregates min/max/mean/p50/p99 latency
  via `LatencyStats`. The actual block-fetch I/O boundary is the `BlockFetcher` trait, with
  `InMemoryBlockFetcher` as the real starter/test implementation — see the `// NOTE:` on
  `BlockFetcher::fetch` for what a production IPFS/IPLD network fetcher needs to add
  (bitswap/libp2p or HTTP gateway transport, retry/backoff policy).

- **"Cryptographic overhead profiling (multihash / signature / ZK-proof verification cost)"**
  — [`src/crypto/cryptographic_overhead_profiler.rs`](./src/crypto/cryptographic_overhead_profiler.rs).
  `CryptographicOverheadProfiler` isolates and times three primitives:
  - multihash computation (sha2-256, via the `sha2` crate),
  - ed25519 signature verification (via `ed25519-dalek`),
  - ZK-proof verification (via the pluggable `ZkProofVerifier` trait; `NoOpZkProofVerifier`
    is a CPU-real stand-in — see its `// NOTE:` for wiring in a real proving-system crate
    such as `ark-groth16` or `halo2_proofs`).

  Each primitive is also profiled under concurrent load
  (`profile_*_under_concurrent_load`, using `std::thread::scope` across
  `worker_thread_count` OS threads) to surface contention and throughput degradation that a
  single-threaded measurement would hide — relevant because production dispatch runs many
  DAG nodes' crypto work in parallel across worker threads.

## Value-add diagnostic (not a feature-manifest bullet on its own)

- [`src/diagnostics/dag_flakiness_root_cause_isolator.rs`](./src/diagnostics/dag_flakiness_root_cause_isolator.rs)
  — `DagFlakinessRootCauseIsolator` takes a recorded DAG execution history (node id, parent
  node id, timestamp, latency, outcome) and performs a real binary-search ("git bisect"-style)
  walk back through the parent chain leading to a symptomatic node, isolating the earliest
  ancestor whose execution is anomalous under a caller-supplied `AnomalyPredicate`
  (`LatencySpikeThresholdPredicate`, `CorruptedOutcomePredicate`, or a custom one). This
  is a diagnostic capability this worker contributes in support of the broader DAG-workflow
  scheduling story owned by `orchestration-control-plane/dag-workflow-scheduler` — when a
  scheduled DAG test run is flaky, this isolator turns a raw execution history (which the
  scheduler already records for durability) into an actionable "start here" node, without the
  scheduler itself needing bisection logic.

## What does NOT belong in this service

Per ARCHITECTURE.md's execution-fleet layout, the following feature-manifest bullets are
implemented by sibling services, not here:

- **Protocol clients (REST/gRPC/WebSocket/GraphQL/MQTT load generation)** —
  `execution-fleet/protocol-load-worker-go` (concurrent multi-protocol client) and
  `execution-fleet/protocol-load-worker-rust` (tokio-based max-throughput TCP/QUIC
  saturation).
- **eBPF fault injection / chaos** — `execution-fleet/chaos-ebpf-sidecar` (privileged eBPF
  agent).
- **Wasm sandboxing of user pre/post-test scripts** —
  `execution-fleet/wasm-payload-sandbox-runtime` (wasmtime host embedding).
- **Fuzzing** — not an execution-fleet concern in this repo's manifest; fuzz-style input
  generation, if present, belongs alongside `test-data-factory-service` in the orchestration
  control plane, not in a stateless benchmarking worker.
- **Clock drift measurement/correction** — a telemetry-lake/observability concern
  (`telemetry-lake/otel-collector-config`, `shared-libraries/observability-kit`), not
  something this worker measures about itself; this worker only measures its own local,
  monotonic (`Instant`-based) durations, which are immune to wall-clock drift by
  construction.

## Local development

```bash
cargo run --bin dag-ipld-benchmark-worker
```

Configuration is via CLI flags or `DAG_IPLD_BENCH_*` environment variables — see
`WorkerConfig` in [`src/main.rs`](./src/main.rs) (`--help` for the full list).

This starter binary runs each profiler/diagnostic once against an in-memory sample DAG and
logs structured (`tracing`, JSON-formatted) output. It does not yet consume
`dag.node.dispatch.v1` or export OTLP — see the `// NOTE:` block at the top of
[`src/main.rs`](./src/main.rs) for the production wiring points.
