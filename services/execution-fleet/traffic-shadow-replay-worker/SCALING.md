# Scaling & failure characteristics — traffic-shadow-replay-worker

## Horizontal scaling

Stateless between runs, matching every other execution-fleet worker's
design (`ARCHITECTURE.md`'s "Independent scaling" rationale) — scale by
replica count, each replica independently replaying its own assigned
fixture/log slice against the target environment.

- **Scale dimension**: replica count, where the control plane's dispatch
  payload assigns each replica a disjoint slice of a large fixture (e.g.
  one replica per N captured exchanges) rather than every replica replaying
  the entire fixture — otherwise replica count would multiply `Multiplier`
  unintentionally (10 replicas each replaying a fixture at "10x" is
  effectively 100x, not 10x). A production dispatch payload should carry
  both the fixture reference/slice and the intended aggregate multiplier
  explicitly, so the scheduler's replica-count choice and this worker's
  `-multiplier` flag don't silently compound.
- **Trigger metrics**: in-flight replay count relative to
  `-max-concurrent-replays`, replay error rate (dial/timeout failures
  against the target — a proxy for the target environment itself buckling
  under shadow load, which is sometimes the point of running a shadow test
  in the first place and sometimes a signal to back off), and flagged-finding
  rate from `ResponseShapeDiffer` (a spike here is a signal for a human, not
  necessarily an autoscaling trigger).
- **No coordination between replicas**: each replica reads its own fixture
  slice, replays it, and writes its own findings file — no shared in-memory
  state, no leader election, matching `ARCHITECTURE.md`'s "Independent
  failure domains" principle.

## What state this service holds

- **In-memory only, and only for the duration of one replay run**: the
  loaded `ReplayableTrafficFixture` (every captured exchange, including
  bodies — see "Resource profile" below for what this costs at fixture
  size), the `hostConnectionPool`-free but still transport-pooled `*http.Client`
  issuing replay requests, and the in-flight replay semaphore
  (`TrafficMultiplierReplayer`'s `MaxConcurrentReplays`-bounded channel).
  None of this is replicated or shared across pods.
- **No checkpointing**: unlike `protocol-load-worker-go`'s load phases
  (checkpointed to Redis before every phase transition, per
  `ARCHITECTURE.md`'s durability design), a shadow-replay run is a single
  bounded pass over a fixed fixture rather than an open-ended load phase —
  if a replica dies mid-run, the control plane simply redispatches that
  fixture slice to a fresh replica, which starts the slice over from the
  beginning. A production build replaying very large fixtures (hours of
  captured traffic) should add a checkpoint of "exchanges completed so far"
  using the same `CheckpointStore` pattern `protocol-load-worker-go`'s
  `internal/loadgen/concurrent_load_generator.go` establishes, rather than
  inventing a new one — this starter does not implement it.
- **Findings output is local-disk, not durable**: `-findings-output-path`
  writes to the replica's local filesystem. A production deployment needs
  this uploaded to durable storage (or streamed to the telemetry lake via
  OTLP/Redpanda, matching every other worker's telemetry path) before the
  pod terminates — not implemented here; see the `// NOTE:` at the top of
  `cmd/traffic-shadow-replay-worker/main.go`.

## Crash-mid-run recovery

Because a replica holds no state the fleet depends on beyond its own
in-flight replay slice:

1. If a pod dies mid-run, whatever replays were in flight are lost — no
   different from `protocol-load-worker-go`'s crash-mid-phase behavior.
2. The control plane's scheduler notices the missing worker (no
   telemetry/heartbeat) and redispatches the assigned fixture slice to a
   fresh replica, which replays it from the start of the slice.
3. This worker deliberately does not try to survive its own crash, matching
   `ARCHITECTURE.md`'s "Independent failure domains" principle — a crashing
   replica must never require special-casing in the scheduler beyond
   "notice it's gone and redispatch."

## Resource profile

- **Memory scales with fixture size**, not with `Multiplier` or replay
  concurrency: the entire `ReplayableTrafficFixture` (every
  `CapturedExchange`'s headers and bodies) is loaded into memory up front
  by `loadFixture` before replay starts. A very large fixture (e.g. a full
  day of high-traffic production capture) should be sliced by the control
  plane's dispatch payload before assignment, not loaded whole by one
  replica — this starter loads the whole file named by `-input-path`.
- **Network- and CPU-bound during replay** in roughly the same proportions
  as `protocol-load-worker-go`'s REST client: `TrafficMultiplierReplayer`
  issues real HTTP requests via a pooled `*http.Client`, so replay
  throughput is bounded by `-max-concurrent-replays`, the target's own
  capacity, and TLS handshake/encryption overhead for HTTPS targets — the
  same profile `protocol-load-worker-go`'s `RestHttpClient` documents in its
  own `SCALING.md`.
- **`ResponseShapeDiffer`'s CPU cost is proportional to response body size
  and JSON nesting depth**, not to `Multiplier` or fixture exchange count
  individually — each diff parses two JSON bodies and flattens them into
  path/type maps. Very large or deeply nested response bodies (e.g. a
  paginated list endpoint returning thousands of items) cost proportionally
  more per diff; this is expected and not currently bounded by a size limit
  in this starter.
