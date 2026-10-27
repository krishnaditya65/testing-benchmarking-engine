# protocol-load-worker-go

A stateless, horizontally scalable **execution-fleet** worker: it opens thousands
of concurrent protocol connections against a test target, drives them along a
configurable traffic shape, and never talks to any other worker directly. It
takes orders from the orchestration control plane over Redpanda's
`dag.node.dispatch.v1` topic and emits telemetry outward via OTLP — see
[ARCHITECTURE.md](../../../ARCHITECTURE.md) for the full contract.

## Implements (feature-manifest → code map)

> "universal protocol client (REST/GraphQL/gRPC with dynamic protobuf
> compiling/WebSockets/WebTransport/MQTT/QUIC-HTTP3)"

Implemented here, one file per protocol behind a single `protocolclient.ProtocolClient`
interface (`internal/protocolclient/types.go`):

| Protocol | File | Status |
|---|---|---|
| REST | `internal/protocolclient/rest_http_client.go` | Implemented — real `net/http` request/response with timing capture. |
| GraphQL | `internal/protocolclient/graphql_client.go` | Implemented — POSTs `{query, variables}`, surfaces the spec's `errors[]` array as a call error. |
| gRPC (bidi streaming) | `internal/protocolclient/grpc_stream_client.go` | Implemented at the stream-lifecycle level (dial, open stream, send, receive-until-close) against `google.golang.org/grpc`. **Dynamic protobuf descriptor compiling** for schema-less targets is *not* implemented — see the `// NOTE:` on `GrpcStreamClient` for the exact integration point (server reflection or an uploaded `FileDescriptorSet` decoded via `protodesc`/`dynamicpb`, plus a registered passthrough `encoding.Codec`). |
| WebSocket | `internal/protocolclient/websocket_client.go` | Implemented — real dial/send/receive loop via `gorilla/websocket`. |
| WebTransport / MQTT / QUIC-HTTP3 | — | **Not implemented in this service.** Per `ARCHITECTURE.md`, raw TCP/QUIC saturation and non-HTTP transports belong to `services/execution-fleet/protocol-load-worker-rust` (tokio-based), which is better suited to those transports' performance characteristics than a goroutine-per-connection Go model. |

## Explicitly out of scope for this service

These feature-manifest bullets belong to sibling execution-fleet workers, not
here — listed so a reader doesn't go looking for them in this codebase:

- **DAG/IPLD traversal, CID resolution, crypto-overhead profiling** — owned by
  `dag-ipld-benchmark-worker-rust`.
- **eBPF fault injection / chaos** — owned by `chaos-ebpf-sidecar` (requires
  privileged host access this worker deliberately does not have).
- **User-supplied Wasm pre/post scripts** — owned by `wasm-payload-sandbox-runtime`.
- **Clock-drift compensation** across distributed load generators — implemented
  in `protocol-load-worker-rust`, not here. Being honest about scope: this Go
  worker reports wall-clock timings as measured locally and does not currently
  correct for inter-node clock skew. A production rollout that needs
  cross-worker latency comparisons at sub-millisecond precision should use the
  Rust worker or add NTP/PTP-disciplined clock sync here as follow-up work.

## Load generation and pipelines

- `internal/loadgen/traffic_profile.go` — `TrafficProfile` implementations
  (`LinearRampProfile`, `StepProfile`, `SpikeProfile`, `WaveProfile`,
  `ReplayCurveProfile`) each compute target concurrency as a pure function of
  elapsed time.
- `internal/loadgen/concurrent_load_generator.go` — `ConcurrentLoadGenerator`
  runs a real worker-pool of goroutines that grows/shrinks every
  `SampleInterval` tick to track the active `TrafficProfile`, aggregates
  `RunMetrics` with atomics (not a shared mutex on the hot path), and respects
  `context.Context` cancellation for shutdown.
- `internal/pipeline/multi_protocol_pipeline_executor.go` — chains
  `PipelineStep`s that can each use a *different* protocol client, threading a
  shared `extraction.VariableContext` between them (e.g. REST JWT fetch →
  extract token → inject into an authenticated gRPC stream's headers).
- `internal/extraction/jsonpath_variable_extractor.go` — hand-rolled JSONPath
  subset (`$.field.nested[0].id`) that extracts a value from a JSON response
  body into the shared `VariableContext`. `go.mod` also lists
  `github.com/PaesslerAG/jsonpath` for a production build that needs the full
  JSONPath spec (wildcards, filters, recursive descent) — swapping it in means
  replacing `JsonPathVariableExtractor.ExtractInto`'s body without changing its
  signature.

## Connection-reuse topology & HTTP/2 multiplexed scheduling

> Feature-manifest bullet: naive load generators break CDNs/LBs by opening
> one socket per virtual user, when real browsers/mobile clients multiplex
> many logical requests over a handful of physical HTTP/2 connections.

- `internal/loadgen/connection_reuse_topology.go` — `ConnectionReusePolicy`
  is the interface answering "how many physical connections per host, and
  how many logical requests multiplexed onto each one." `BrowserLikeMultiplexingPolicy`
  models a real HTTP/2 client (100 streams/connection, 6 connections/host by
  default — the common server-advertised `SETTINGS_MAX_CONCURRENT_STREAMS`
  and the historical per-origin browser connection cap, respectively).
  `OneSocketPerVirtualUserPolicy` names the naive topology explicitly, so a
  load-test plan can deliberately A/B "realistic multiplexed load" against
  "naive one-socket-per-VU load" using the same scheduler code path.
- `internal/loadgen/http2_multiplexed_request_scheduler.go` —
  `Http2MultiplexedRequestScheduler` schedules many logical requests
  (produced by the same `RequestFactory` shape `ConcurrentLoadGenerator`
  uses) over a `ConnectionReusePolicy`-bounded pool of physical HTTP/2
  connections per target host, instead of one connection per worker
  goroutine. It is a **selectable strategy alongside**, not a replacement
  for, `ConcurrentLoadGenerator` — both implement the new
  `loadgen.LoadGenerationStrategy` interface (`Run(ctx, profile) (RunMetrics, error)`
  + `StrategyName() string`), and `cmd/protocol-load-worker`'s
  `-load-strategy` flag (`goroutine-per-vu` | `http2-multiplexed`) picks
  between them per load phase. Scope: this scheduler speaks REST/GraphQL-shaped
  HTTP directly (it does not wrap `protocolclient.ProtocolClient`) because
  multiplexing topology is HTTP/2-specific and doesn't have a meaningful
  analog for gRPC streams or long-lived WebSocket connections — those stay
  on `ConcurrentLoadGenerator`. HTTP/3/QUIC multiplexing is architecturally
  identical but needs a QUIC `RoundTripper` this starter doesn't vendor; see
  the `// NOTE:` on `newPooledTransport` for the exact swap point.

## Record-and-replay traffic capture

> Feature-manifest-adjacent capability: run this worker as a passive sidecar
> that captures real request/response pairs flowing through its
> `protocolclient.ProtocolClient` implementations, for later replay (see
> `services/execution-fleet/traffic-shadow-replay-worker/`, which owns
> "Production Traffic Shadowing" and "Log-to-Test Generation").

- `internal/capture/live_traffic_capture_recorder.go` —
  `LiveTrafficCaptureRecorder` is a decorator that satisfies
  `protocolclient.ProtocolClient` itself: it wraps a real client, delegates
  every `Execute` call unchanged, and records the request/response pair on
  the way past. Nothing downstream (the pipeline executor, the load
  generator) needs to know capture is happening.
- `internal/capture/sensitive_header_redactor.go` —
  `SensitiveHeaderRedactor` strips `Authorization`, `Cookie`, `Set-Cookie`,
  `Proxy-Authorization`, and common vendor API-key/token/signature header
  name patterns (`*api-key*`, `*auth-token*`, `*secret*`, `*signature*`,
  etc.) before anything is retained in memory — redaction happens
  unconditionally in `LiveTrafficCaptureRecorder.Observe`, there is no "raw
  capture" mode. Body-level PII redaction is explicitly **not** implemented
  (see the `// NOTE:` on `SensitiveHeaderRedactor` for why and what the
  mitigation is in the meantime).
- `internal/capture/replayable_traffic_fixture.go` — `ReplayableTrafficFixture`
  is the JSON-serializable recording: ordered `CapturedExchange`s (method,
  host-stripped path, redacted headers, bodies, status, latency) plus
  provenance (`SourceRunID`, `CapturedFromHost`, capture window). `WriteJSONFile`/
  `ReadReplayableTrafficFixtureFile` round-trip it to/from disk as indented
  JSON so a fixture is reviewable in a PR diff before a team trusts it as a
  shadow-replay traffic source.
- `cmd/protocol-load-worker/main.go` — the `-capture-mode` flag (env
  `CAPTURE_MODE`) wraps whichever client `newProtocolClient` built in a
  `LiveTrafficCaptureRecorder` before it reaches the pipeline executor or
  load generator, and `-capture-output-path` (env `CAPTURE_OUTPUT_PATH`)
  names the fixture file written once the load phase completes.

## Cost-aware spot harvesting / checkpointing

`ARCHITECTURE.md` requires load-test state to be "checkpointed to Redis before
each load-phase transition" so a run survives spot-instance preemption.
`internal/loadgen/concurrent_load_generator.go` implements the *hook*:
`ConcurrentLoadGenerator.Run` always calls `CheckpointStore.SaveCheckpoint`
with a `PhaseCheckpoint` (calls completed/succeeded, last concurrency level)
before returning from a phase — i.e. before the caller can transition to the
next phase. The default `CheckpointStore` is `NoopCheckpointStore`, which
discards checkpoints; a production build supplies a real Redis-backed
implementation (see the `// NOTE:` on the `CheckpointStore` interface) without
touching `ConcurrentLoadGenerator` itself.

Cost-aware spot-instance harvesting (bidding/reclaiming spot capacity,
draining a worker ahead of a preemption notice) is **not implemented** here —
it's a scheduling concern that belongs to the control plane's
`environment-lifecycle-manager` deciding *where* to run workers, not to the
worker itself. This service's job is only to make sure it *can* be killed and
resumed cleanly, which the checkpoint hook above provides.

## Running locally

```bash
cd services/execution-fleet/protocol-load-worker-go
go run ./cmd/protocol-load-worker \
  -target-url http://localhost:8080/health \
  -protocol rest \
  -peak-concurrency 50 \
  -phase-duration 30s
```

Flags fall back to environment variables (`TARGET_URL`, `PROTOCOL`, `RUN_ID`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `LOAD_STRATEGY`, `CAPTURE_MODE`,
`CAPTURE_OUTPUT_PATH`) so the same binary runs unmodified under a Kubernetes
Deployment.

To run the multiplexed scheduler against a CDN-fronted target instead of the
default goroutine-per-VU strategy:

```bash
go run ./cmd/protocol-load-worker -target-url https://example.com -load-strategy http2-multiplexed
```

To capture real traffic (headers redacted) while a load phase runs, for
later shadow replay:

```bash
go run ./cmd/protocol-load-worker -target-url https://example.com -capture-mode -capture-output-path ./checkout-flow.fixture.json
```

> This is a starter repo without live infra wired up: dependencies in `go.mod`
> are declared but not vendored/fetched, so `go build`/`go mod tidy` are not
> expected to succeed offline. Every I/O boundary (`net/http`, `grpc`,
> `gorilla/websocket`) is isolated to `internal/protocolclient/*.go` with
> `// NOTE:` comments marking exactly what a production implementation adds
> (TLS/mTLS config, JWT signature validation, dynamic protobuf descriptors).
