# 0002. Split the execution fleet by language, per workload profile

- Status: Accepted
- Date: 2024-03-04
- Owners: execution-fleet maintainers

## Context

`services/execution-fleet/` hosts five distinct worker types:
`protocol-load-worker-go`, `protocol-load-worker-rust`,
`dag-ipld-benchmark-worker-rust`, `chaos-ebpf-sidecar`, and
`wasm-payload-sandbox-runtime`. Early design drafts proposed a single "execution
worker" service that handled REST/gRPC/WebSocket/GraphQL load generation, raw
TCP/QUIC saturation, IPLD/CID benchmarking, and chaos injection behind one set of
internal interfaces, in one language, to minimize the number of services engineers
need to context-switch between.

In practice these workloads have materially different performance profiles and
different points on the "developer velocity vs. raw throughput" trade-off:

- REST/gRPC/WebSocket/GraphQL client load generation needs a rich, fast-moving
  protocol/library ecosystem (HTTP/2, GraphQL clients, WebSocket libraries) and
  benefits from a garbage-collected language's iteration speed — new protocol
  support and new assertion types are added frequently.
- Raw TCP/QUIC saturation (`protocol-load-worker-rust`) and CID resolution /
  crypto-overhead profiling (`dag-ipld-benchmark-worker-rust`) are dominated by
  per-connection and per-hash-operation overhead, where GC pauses and allocator
  behavior directly show up in the tail-latency numbers the platform exists to
  measure. A GC-paused worker cannot be trusted to report an unbiased P99.
- Chaos fault injection (`chaos-ebpf-sidecar`) requires eBPF/kernel-level
  programming, a domain where Go and Rust ecosystems (cilium/ebpf, aya) are both
  viable, but the operational profile (privileged DaemonSet, no business logic
  beyond program lifecycle) doesn't share code with the load-generation workers
  regardless of language choice.

A single shared worker binary would force every workload onto the constraints of the
most demanding one, or produce an internal split (feature flags, workload-specific
code paths in one binary) that recreates separate services anyway without the
benefit of separate deployability.

## Decision

Keep the execution fleet split into independently deployable services, chosen by
workload throughput/latency sensitivity rather than by a blanket "one language for
the monorepo" rule:

- **Go** for `protocol-load-worker-go` — protocol breadth and iteration speed matter
  more than shaving allocator overhead at the client-request level; Go's HTTP/gRPC
  ecosystem and concurrency model (goroutines per virtual user) fit this workload
  well, and it stays consistent with `dag-workflow-scheduler` and
  `environment-lifecycle-manager`'s language choice on the control-plane side.
- **Rust** for `protocol-load-worker-rust` (tokio-based max-throughput TCP/QUIC) and
  `dag-ipld-benchmark-worker-rust` (CID resolution, crypto-overhead profiling) —
  workloads where the benchmark result itself must not be contaminated by the
  measuring tool's own runtime overhead.
- **Rust or Go** for `chaos-ebpf-sidecar`, decided per-maintainer on eBPF tooling
  maturity at implementation time, not fixed by this ADR — its concerns are
  orthogonal to the load-generation split above.
- Every worker communicates with the control plane only through the same
  `dag.node.dispatch.v1` contract (`shared-libraries/protocol-contracts`) and emits
  telemetry only via OTLP, so language choice is fully encapsulated per service and
  invisible to the scheduler.

## Consequences

**Positive:**

- Each worker's language matches what its own numbers need to be trustworthy — see
  `ARCHITECTURE.md`'s "Language matches the job, not convention".
- Independent deploys and independent Kubernetes node-group tuning per worker type
  (e.g. `protocol-load-worker-rust` can be pinned to different CPU-pinned instance
  types than `protocol-load-worker-go` without cross-affecting either).
- A regression in one worker's dependency tree (e.g. a Go HTTP library CVE) never
  forces a rebuild/redeploy of the Rust workers.

**Negative / trade-offs:**

- No shared application-level code between the Go and Rust workers beyond the
  wire-level contracts in `shared-libraries/protocol-contracts` — logic like
  virtual-user ramp scheduling is implemented twice, once per language family, and
  must be kept behaviorally consistent by convention and cross-worker integration
  tests, not by a shared library.
- Two more toolchains (Go and Rust, plus TypeScript for the control plane) for
  platform engineers to be productive in, versus a single-language monorepo.
- Local development requires running `go run ./...` in one directory and
  `cargo run` in another rather than one unified build command — documented per
  service's own `README.md` per the root `README.md`'s "Local development" section.
