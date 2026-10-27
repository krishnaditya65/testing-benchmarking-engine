# Scaling & failure characteristics

## Horizontal scaling

`protocol-load-worker-go` is stateless between load phases by design (see
`ARCHITECTURE.md`'s "Independent scaling" rationale), so it scales the way
the rest of the execution fleet does: run many identical replicas, not a
bigger one.

- **Scale dimension**: replica count, via a Kubernetes `HorizontalPodAutoscaler`
  targeting a load test's *desired* concurrency, not CPU alone. Each replica
  runs `internal/loadgen.ConcurrentLoadGenerator`, which internally ramps a
  goroutine pool per `TrafficProfile`; the HPA decides how many *replicas*
  exist, and the control plane's dispatch payload decides how much
  concurrency each replica's `TrafficProfile` should carry, so total fleet
  concurrency is `replicas × per-replica target concurrency`.
- **Trigger metrics**: goroutine/connection-saturation signals exported as
  OTLP metrics — in-flight call count relative to the current
  `TrafficProfile.TargetConcurrency`, dial-error rate (a proxy for
  connection-pool exhaustion or target-side backpressure), and event-loop
  scheduling latency (Go runtime `sched_latency` — high values mean the
  goroutine pool has grown past what the pod's CPU allocation can service).
  A production HPA config lives in `infrastructure/kubernetes/`, driven by
  these custom metrics rather than CPU/memory alone, since a pod fanning out
  10k idle WebSocket connections can be CPU-idle while still "full".
- **Thousands of replicas**: because workers never call each other
  (`ARCHITECTURE.md`'s "no synchronous RPC that could cascade a failure
  backward"), replica count has no coordination cost — each pod independently
  pulls its own dispatch from `dag.node.dispatch.v1` and reports its own OTLP
  telemetry. There is no leader election, no gossip, no shared in-memory
  state across pods.

## What state this service holds

- **In-memory only, and only for the duration of one load phase**: the live
  goroutine worker pool (`internal/loadgen/concurrent_load_generator.go`),
  in-flight `protocolclient` connections (pooled `*http.Client` transports,
  `*grpc.ClientConn`s, open WebSocket connections), and the
  `extraction.VariableContext` for whatever pipeline is currently running.
  None of this is replicated or shared across pods.
- **Durable state lives outside the process**: per `ARCHITECTURE.md`'s
  durability design, this worker is expected to checkpoint to Redis *before*
  each load-phase transition. `ConcurrentLoadGenerator.Run` implements the
  hook (`CheckpointStore.SaveCheckpoint`, called with a `PhaseCheckpoint`
  right before the function returns) but ships only a `NoopCheckpointStore`
  by default — see the README's "Cost-aware spot harvesting / checkpointing"
  section and the `// NOTE:` on `CheckpointStore` in
  `internal/loadgen/concurrent_load_generator.go` for what a real Redis-backed
  store adds.

## Crash-mid-test recovery

Because a worker holds no state the fleet depends on beyond one in-flight
phase:

1. If a pod dies mid-phase (OOM kill, spot preemption, node drain), whatever
   calls were in flight are simply lost — no different from a load-test
   client crashing against a real target.
2. On the *next* liveness check, the control plane's scheduler
   (`dag-workflow-scheduler`) notices the missing worker (no telemetry/heartbeat)
   and re-dispatches the load-test node — with the last `PhaseCheckpoint` read
   back from Redis — to a fresh worker pod over `dag.node.dispatch.v1`.
3. The fresh pod resumes from the checkpoint's `LastConcurrency`/
   `CallsCompleted` rather than restarting the whole phase from zero
   concurrency, so a long soak test doesn't have to fully re-ramp after a
   single preemption.

This worker deliberately does not try to survive its own crash — it relies on
the control plane's redispatch loop plus the Redis checkpoint, matching
`ARCHITECTURE.md`'s "Independent failure domains" principle: a crashing
worker must never require special-casing in the scheduler beyond "notice it's
gone and redispatch."

## Connection-reuse topology's effect on resource profile

Selecting `-load-strategy http2-multiplexed` (`internal/loadgen/http2_multiplexed_request_scheduler.go`)
changes this worker's resource shape versus the default `goroutine-per-vu`
strategy:

- **Far fewer open sockets at the same logical concurrency.** A phase at
  5,000 target concurrency under `goroutine-per-vu` opens roughly 5,000
  connections; the same phase under `http2-multiplexed` with
  `BrowserLikeMultiplexingPolicy`'s defaults opens at most
  `MaxConnectionsPerHost` (6, by default) connections per distinct target
  host, each carrying up to `LogicalStreamsPerConnection` (100, by default)
  concurrent logical requests. This is the entire point — it reproduces how
  a real CDN/LB-fronted target actually gets hit — but it also means
  ephemeral-port and FD pressure (see `protocol-load-worker-rust`'s
  `SCALING.md` for how severe that pressure gets at the naive-topology end)
  is dramatically lower per replica for the same logical load.
- **More CPU concentrated per connection**, not less overall: each pooled
  connection now serves many concurrent HTTP/2 streams, so the transport's
  per-connection framing/flow-control bookkeeping runs hotter on those few
  sockets instead of being spread across thousands of idle-most-of-the-time
  ones. This is expected and matches what a real multiplexing target
  actually experiences.
- **Backpressure looks different.** Under `goroutine-per-vu`, hitting a
  connection limit means dial failures. Under `http2-multiplexed`, hitting
  `LogicalStreamsPerConnection` capacity on every pooled connection means
  logical requests queue for a free stream slot (see `hostConnectionPool.acquireSlot`)
  rather than failing outright — a closer match to how a real multiplexing
  client behaves when it saturates its own connection pool, but it also
  means a saturated `http2-multiplexed` phase reports rising latency before
  it reports errors, whereas a saturated `goroutine-per-vu` phase tends to
  report errors first.

## `-capture-mode`'s effect on resource profile

`internal/capture.LiveTrafficCaptureRecorder` adds bounded, in-memory-only
overhead on top of whichever `LoadGenerationStrategy` is selected:

- **Memory scales with total exchanges captured for the run**, not just
  concurrent connection count — every `Execute` call appends one
  `CapturedExchange` (headers, bodies, timing) to an in-memory slice that is
  only flushed to disk once, at the end of the phase, via
  `LiveTrafficCaptureRecorder.WriteJSONFile`. A long high-throughput capture
  run should expect memory to grow roughly linearly with call count times
  average request+response body size; this is a starter implementation and
  does not stream exchanges to disk incrementally (see the `// NOTE:` on
  `ReplayableTrafficFixture` for the follow-up).
- **CPU overhead per call is small and constant**: one `SensitiveHeaderRedactor.Redact`
  pass (a map iteration plus substring matching against a short pattern
  list) per request and per response, independent of body size.
- Capture-mode should not be left enabled on production-scale load phases by
  default for this reason — it is meant for deliberately-scoped capture
  runs (e.g. a lower-concurrency phase run specifically to produce a
  `ReplayableTrafficFixture` for `traffic-shadow-replay-worker`), not for
  every load test.

## Resource profile

- **CPU-bound** for goroutine scheduling at high concurrency (thousands of
  goroutines runnable simultaneously stresses the Go scheduler) and for TLS
  handshake/encryption overhead when targets use HTTPS/WSS/gRPC-over-TLS.
  Expect CPU usage to scale roughly linearly with concurrent connection count
  and request rate, not with response payload size.
- **Network-bound** for connection fan-out: thousands of concurrent
  outbound sockets means this pod's network namespace, not its CPU, is often
  the first limit hit (ephemeral port exhaustion, NIC bandwidth, target-side
  connection limits). Pod network policy and node instance type both need
  headroom sized for connection count, not just throughput.
- **Low memory per connection**: each REST/GraphQL call is a short-lived
  goroutine + buffered response body; each WebSocket/gRPC stream is a
  long-lived goroutine + a small read/write buffer. Memory scales with
  concurrent connection count × response payload size, not with total calls
  issued over the run — a long low-concurrency soak test has a flatter memory
  profile than a short high-concurrency spike test at the same total call
  count.
