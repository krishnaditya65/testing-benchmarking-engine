# traffic-shadow-replay-worker

A stateless, horizontally scalable **execution-fleet** worker that replays
previously-captured production traffic against a target environment at a
configurable multiplier, and flags where the target's actual responses
diverge from the original baseline — in shape or in latency. Part of the
`execution-fleet` layer (`ARCHITECTURE.md`): it never talks to other workers
directly, and its intended production wiring takes orders from the
orchestration control plane over Redpanda's `dag.node.dispatch.v1` topic and
emits telemetry outward via OTLP, exactly like every other execution-fleet
worker (see the `// NOTE:` at the top of `cmd/traffic-shadow-replay-worker/main.go`
for what that wiring adds — this starter drives one run from flags instead).

## Why this service exists

Two feature-manifest bullets had no owning service anywhere in this repo
before this one:

| Feature-manifest bullet | Implementation |
|---|---|
| **Production Traffic Shadowing** — replay real captured traffic against a target environment at 2x/10x/100x multiplier and compare responses against baseline | `internal/shadowing/traffic_multiplier_replayer.go` (`TrafficMultiplierReplayer`) generates the shadow load; `internal/shadowing/response_shape_differ.go` (`ResponseShapeDiffer`) does the baseline-vs-actual comparison, flagging schema drift and latency regression. |
| **Log-to-Test Generation** — turn existing API-gateway access logs into replayable test traffic, as an alternative to a purpose-built capture | `internal/shadowing/sanitized_gateway_log_ingester.go` — the `SanitizedGatewayLogIngester` interface plus an `ALBAccessLogIngester` sketch adapter, both producing the same `ReplayableTrafficFixture` shape `TrafficMultiplierReplayer` already knows how to replay. |

Neither bullet fit naturally into `protocol-load-worker-go` (which generates
*declarative* concurrency-curve load against *one* request shape per phase)
or `protocol-load-worker-rust` (raw socket saturation, no request-shape
concept at all) — both are about replaying a *fixed, already-real* sequence
of distinct requests and comparing the result, a different job from either
sibling worker's.

## Traffic sources

`cmd/traffic-shadow-replay-worker`'s `-traffic-source` flag selects where a
`ReplayableTrafficFixture` comes from:

- **`fixture`** (default) — read a JSON file with the exact shape
  `protocol-load-worker-go`'s `internal/capture/replayable_traffic_fixture.go`
  writes via its `--capture-mode` (see that service's README.md). This is
  the primary, purpose-built path: capture real traffic on the Go worker,
  replay it here.
- **`alb-access-log`** — ingest an AWS ALB access log via
  `ALBAccessLogIngester`, for teams that already have gateway logs and don't
  want to run a capture sidecar. See the `// NOTE:` on `ALBAccessLogIngester`
  for exactly how much of the ALB format this sketch parses versus a
  production implementation would.

### Why `ReplayableTrafficFixture` is duplicated here, not imported

`protocol-load-worker-go` and this service are separate Go modules
(separate `go.mod` files — every execution-fleet worker is independently
deployable, per `ARCHITECTURE.md` and
`docs/architecture-decision-records/0002-split-execution-fleet-by-language-per-workload-profile.md`),
and per `ARCHITECTURE.md`'s "workers never talk to each other directly"
principle, this service doesn't import `protocol-load-worker-go`'s Go
packages — the only coupling between the two is the on-disk JSON shape one
writes and the other reads. `internal/shadowing/replayable_traffic_fixture.go`
therefore defines its own `ReplayableTrafficFixture`/`CapturedExchange`
types, field-for-field identical to `protocol-load-worker-go`'s. See the
`// NOTE:` on `CapturedExchange` in that file for exactly when this
duplication should be replaced with a real shared package under
`services/shared-libraries/` (the same place `dag-domain-model` and
`protocol-contracts` already hold cross-service shapes) — namely, if these
two services ever need to interoperate through direct Go imports rather than
through a fixture file.

## Replay and diff pipeline

1. `TrafficMultiplierReplayer.Replay` walks every `CapturedExchange` in the
   loaded fixture and issues it against `-target-base-url`
   `Multiplier` times concurrently (bounded overall by
   `-max-concurrent-replays`, independent of `Multiplier`, so a 500-exchange
   fixture replayed at 100x — 50,000 logical replays — never fires all at
   once). Redacted headers (`[REDACTED]`, written by
   `protocol-load-worker-go`'s `SensitiveHeaderRedactor`) are skipped rather
   than replayed literally — a shadow target needs its own valid
   credentials, not the sanitized placeholder a fixture retained.
2. Every completed `ReplayResult` is paired with its originating
   `CapturedExchange` and passed to `ResponseShapeDiffer.Diff`, which flags:
   - **Schema drift** — JSON response bodies are flattened into
     `path -> value-kind` maps and compared; a field present in one but not
     the other, or present in both with a different JSON type, is flagged
     as `field-added`/`field-removed`/`field-type-changed`.
   - **Latency regression** — `actual latency / baseline latency >=`
     `-latency-regression-ratio` (default 1.5, i.e. 50% slower than the
     original capture).
3. Every flagged `DiffFinding` is appended to `-findings-output-path` as
   newline-delimited JSON.

## Explicitly out of scope for this service

- **Capturing** traffic in the first place — owned by
  `protocol-load-worker-go`'s `internal/capture` package
  (`--capture-mode`). This service only ever *consumes* a
  `ReplayableTrafficFixture`; it has no capture-side code.
- **Declarative concurrency-curve load generation** (`TrafficProfile`,
  `LoadGenerationStrategy`) — owned by `protocol-load-worker-go`'s
  `internal/loadgen` package. Shadow replay drives a fixed sequence of real
  requests at a multiplier, not a synthetic concurrency curve.
- **Non-ALB gateway log formats** (Envoy, nginx) — the
  `SanitizedGatewayLogIngester` interface is format-agnostic, but only
  `ALBAccessLogIngester` is implemented; see its doc comment for the
  follow-up.

## Running locally

```bash
cd services/execution-fleet/traffic-shadow-replay-worker

# Replay a fixture captured by protocol-load-worker-go's --capture-mode at 10x:
go run ./cmd/traffic-shadow-replay-worker \
  -input-path ../protocol-load-worker-go/checkout-flow.fixture.json \
  -target-base-url https://staging.internal.example.com \
  -multiplier 10

# Or ingest an ALB access log directly instead of a purpose-built capture:
go run ./cmd/traffic-shadow-replay-worker \
  -traffic-source alb-access-log \
  -input-path ./sample-alb-access.log \
  -target-base-url https://staging.internal.example.com \
  -multiplier 2
```

Flags fall back to environment variables (`TRAFFIC_SOURCE`, `INPUT_PATH`,
`TARGET_BASE_URL`, `FINDINGS_OUTPUT_PATH`) so the same binary runs
unmodified under a Kubernetes Deployment, matching every other
execution-fleet worker's convention.
