# 0006. Connection-reuse-aware load shaping and a dedicated traffic-shadowing service

- Status: Accepted
- Date: 2026-08-11
- Owners: execution-fleet maintainers

## Context

Two gaps existed between what the platform claims to do and what the execution
fleet actually implemented:

1. **`protocol-load-worker-go`'s only load-generation strategy was
   goroutine-per-virtual-user** (`internal/loadgen/concurrent_load_generator.go`):
   every unit of concurrency in a `TrafficProfile` opened its own physical
   connection. That is not how real traffic behaves against anything sitting
   behind a CDN, an L7 load balancer, or an HTTP/2-aware origin — a real
   browser tab or mobile client multiplexes many logical requests over a
   small, capped number of physical HTTP/2 connections per origin. A load
   generator that ignores this produces two false signals: it can trip a
   CDN/LB's per-IP connection-count abuse heuristics that real traffic never
   would, and it never actually exercises the thing that matters at scale —
   how the origin behaves under many multiplexed streams sharing few
   connections (`SETTINGS_MAX_CONCURRENT_STREAMS` exhaustion, connection-level
   flow control), as opposed to how it behaves under raw connection-count
   pressure (which `protocol-load-worker-rust`'s TCP/QUIC saturation already
   covers deliberately, as a *different* test).
2. **"Production Traffic Shadowing" and "Log-to-Test Generation" had no
   owning service.** Every other feature-manifest bullet traces to a service
   per each service's own README.md "Implements" mapping, but replaying real
   captured traffic against a target at a multiplier, and turning existing
   gateway logs into replayable test traffic, fit naturally into neither
   `protocol-load-worker-go` (declarative concurrency-curve load against one
   request shape per phase) nor `protocol-load-worker-rust` (raw socket
   saturation, no request-shape concept). Leaving them unowned meant they
   would keep being skipped every time someone re-derived the feature
   manifest against the codebase.

## Decision

**Connection-reuse topology**, added to `protocol-load-worker-go`:

- `internal/loadgen/connection_reuse_topology.go` defines `ConnectionReusePolicy`,
  answering "how many physical connections per host" and "how many logical
  requests multiplexed onto each one." `BrowserLikeMultiplexingPolicy` models
  a real HTTP/2 client's defaults; `OneSocketPerVirtualUserPolicy` names the
  previously-implicit naive topology explicitly, so a load-test plan can
  deliberately A/B the two against the same target.
- `internal/loadgen/http2_multiplexed_request_scheduler.go` adds
  `Http2MultiplexedRequestScheduler`, which schedules logical requests over a
  `ConnectionReusePolicy`-bounded pool of physical HTTP/2 connections.
  Crucially, this is **additive, not a replacement**: both
  `ConcurrentLoadGenerator` and `Http2MultiplexedRequestScheduler` now
  implement a new `loadgen.LoadGenerationStrategy` interface
  (`Run(ctx, profile) (RunMetrics, error)` + `StrategyName() string`), and
  `cmd/protocol-load-worker`'s `-load-strategy` flag selects between them per
  load phase. Goroutine-per-VU remains correct and necessary for protocols
  where connection multiplexing doesn't apply (WebSocket, gRPC streams) and
  for load tests that are deliberately testing raw connection-exhaustion
  behavior rather than realistic traffic shape.

**Record-and-replay traffic capture**, also added to `protocol-load-worker-go`:

- `internal/capture/live_traffic_capture_recorder.go`'s
  `LiveTrafficCaptureRecorder` is a `protocolclient.ProtocolClient` decorator
  that passively records request/response pairs flowing through whatever
  client it wraps, redacting sensitive headers
  (`internal/capture/sensitive_header_redactor.go`) before anything is
  retained, and serializing to `internal/capture/replayable_traffic_fixture.go`'s
  `ReplayableTrafficFixture`. `cmd/protocol-load-worker`'s `-capture-mode`
  flag wires this in front of the normal client without changing anything
  downstream's behavior.

**A new service, `traffic-shadow-replay-worker`**, owns the two previously-unowned
bullets:

- `internal/shadowing/traffic_multiplier_replayer.go`'s `TrafficMultiplierReplayer`
  replays a `ReplayableTrafficFixture` against a target environment at a
  configurable multiplier (2x/10x/100x).
- `internal/shadowing/response_shape_differ.go`'s `ResponseShapeDiffer` flags
  schema drift and latency regression between the fixture's original
  baseline responses and the replay's actual responses.
- `internal/shadowing/sanitized_gateway_log_ingester.go` defines the
  `SanitizedGatewayLogIngester` interface plus an `ALBAccessLogIngester`
  sketch adapter, so teams with existing gateway access logs (rather than a
  purpose-run capture) have an equivalent path into the same replay/diff
  pipeline.
- This service's `ReplayableTrafficFixture`/`CapturedExchange` types are a
  deliberate field-for-field duplicate of `protocol-load-worker-go`'s, not an
  import — the two are separate Go modules and, per `ARCHITECTURE.md`,
  execution-fleet workers do not call each other directly; the fixture *file*
  is the only coupling. See the `// NOTE:` in this service's
  `internal/shadowing/replayable_traffic_fixture.go` for when this should
  move to `services/shared-libraries/` instead.

## Consequences

**Positive:**

- Load tests can now measure what actually matters for CDN/LB-fronted
  targets (multiplexed-stream behavior) in addition to what
  `protocol-load-worker-rust` already measures deliberately (raw
  connection-count saturation) — the two are now clearly two different,
  intentional test shapes rather than one being an accidental default.
- "Production Traffic Shadowing" and "Log-to-Test Generation" are no longer
  silently missing from the codebase — every feature-manifest bullet now
  traces to exactly one service, matching every other bullet's mapping.
- Capture and replay stay decoupled: `--capture-mode` produces a plain JSON
  fixture file with no dependency on `traffic-shadow-replay-worker` existing,
  and `traffic-shadow-replay-worker` can equally consume a hand-written
  fixture or an ingested gateway log — neither side requires the other to
  ship in lockstep.

**Negative / trade-offs:**

- `Http2MultiplexedRequestScheduler` speaks REST/GraphQL-shaped HTTP directly
  rather than going through `protocolclient.ProtocolClient`, because
  connection-multiplexing topology needs transport-level control
  (`MaxConnsPerHost`) that interface doesn't expose. This is a second,
  narrower HTTP execution path alongside `protocolclient.RestHttpClient`
  that must be kept behaviorally consistent by convention, not by shared
  code — the same kind of trade-off ADR 0002 already accepted between the Go
  and Rust workers, now recurring *within* one worker.
- `ReplayableTrafficFixture` is hand-duplicated across two Go modules. If
  `traffic-shadow-replay-worker` and `protocol-load-worker-go` ever need
  tighter coupling than "one writes a file, the other reads it," this
  duplication becomes a real maintenance cost and should be resolved by
  extracting a shared Go module — deferred rather than solved now because
  today's coupling genuinely is file-based only.
- `LiveTrafficCaptureRecorder`'s redaction covers headers only, not response/
  request bodies — a fixture captured against traffic with sensitive body
  content (e.g. PII in a JSON payload) is not automatically safe to share
  outside the team that captured it. See the `// NOTE:` on
  `SensitiveHeaderRedactor` for the reasoning and interim mitigation
  (non-production capture, or pairing with `test-data-factory-service`'s
  masked data on the target side).
- `traffic-shadow-replay-worker` loads its entire assigned fixture into
  memory up front and has no checkpoint-and-resume story for a
  crash-mid-replay, unlike `protocol-load-worker-go`'s load phases (Redis
  checkpoint before every phase transition). Acceptable for now because a
  replay run is a bounded, re-runnable pass over a fixed fixture rather than
  an open-ended load phase, but a production deployment replaying very large
  fixtures should add checkpointing — see this service's `SCALING.md`.
