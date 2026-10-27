# 0011. Cross-cutting robustness hardening: idempotent dispatch, backpressure feedback, multi-region posture, secret rotation, dogfooded observability

- Status: Accepted
- Date: 2026-08-11
- Owners: dag-workflow-scheduler maintainers, telemetry-lake maintainers, pipeline-api-gateway
  maintainers

## Context

Several early-stage gaps in this platform share a common shape: a component correctly handles
its own local failure mode, but the platform-wide behavior one hop away was left unaddressed —
either as an explicit `TODO`, or as a real-but-unnoticed correctness gap that only shows up
under production conditions (a crash mid-dispatch, a 6-hour load test, a saturated telemetry
lake, a regional outage). None of these individually justified a dedicated ADR, but together
they are the same class of problem — "the happy path works; the platform's resilience story
under partial failure does not yet" — so this ADR covers them as one cross-cutting hardening
pass:

1. **Idempotent dispatch.** `dag-workflow-scheduler`'s `RedpandaExecutionFleetDispatcher`
   (`internal/dispatch/execution_fleet_dispatcher.go`) produces a `NodeDispatchMessage` onto
   `dag.node.dispatch.v1` per Temporal Activity invocation. Temporal's own retry policy
   (`dispatchActivityRetryPolicy` in `internal/workflow/dag_pipeline_workflow.go`) exists
   precisely because dispatch can fail and need retrying — but nothing prevented a retried
   attempt from re-producing a message whose original produce actually succeeded (e.g. the
   scheduler process crashed between a successful Redpanda write and Temporal durably recording
   the activity as complete). A 50,000-VU `LOAD_STEP` node dispatched twice is not a harmless
   no-op; it is a second, real, expensive load-test run.
2. **Upstream backpressure feedback loop.** `redpanda-ingestion-pipeline`'s
   `BackpressureAwareProducer` already sheds load locally once its circuit breaker trips,
   protecting itself and the downstream ClickHouse Kafka-engine consumer group. But that
   protection was entirely local: `dag-workflow-scheduler` had no way to learn the telemetry
   lake was under pressure, so it kept dispatching new nodes that generate proportionally more
   telemetry — the exact opposite of what a saturated ingestion path needs.
3. **Multi-region control-plane failover posture.** `dag-workflow-scheduler` depends on a
   single Temporal cluster (ADR 0001). A regional outage of that cluster currently strands
   every in-flight pipeline run with no documented failover path.
4. **Mid-run secret rotation.** `pipeline-api-gateway`'s `SecretInjectionService` resolves
   `${{secret:...}}` placeholders once, at node-dispatch time. Neither Vault nor AWS Secrets
   Manager guarantee a resolved value stays valid indefinitely; a token valid at pipeline-run
   start can expire hours into a long-running load test with no mechanism to refresh it.
5. **Dogfooded observability.** `shared-libraries/observability-kit` exists so every TypeScript
   service gets identical tracing/logging/correlation conventions "instead of each
   reimplementing its own variant" (per that package's own README) — yet `pipeline-api-gateway`,
   the control plane's own BFF, was not actually calling it, and its entrypoint used a bare
   `console.log` instead of the shared structured logger.

## Decision

Address each gap with a real, testable change scoped to the owning service, plus an explicit
interface/config surface for the piece that's architecturally significant but out of scope to
fully implement in this pass (multi-region failover orchestration).

### 1. Idempotent, exactly-once-effect dispatch (`dag-workflow-scheduler`)

- `internal/dispatch/dispatch_idempotency_key.go` — `DeriveDispatchIdempotencyKey(pipelineRunID,
  nodeID, attemptGeneration)` derives a deterministic SHA-256 key. `attemptGeneration` is fed
  from Temporal's own `activity.GetInfo(ctx).Attempt` in `internal/workflow/activities.go`, so
  re-invocation of the *same* in-flight attempt (the crash-recovery case) derives the same key,
  while a genuinely new attempt (a prior attempt failed outright) derives a different one and is
  still allowed to dispatch.
- `internal/dispatch/idempotent_dispatch_deduplicator.go` — `DispatchDeduplicationStore` port
  (`MarkDispatchedIfAbsent`, Redis-`SETNX`-shaped) plus `InMemoryDispatchDeduplicationStore`, a
  single-replica-correct adapter shaped so a `RedisDispatchDeduplicationStore` implementing the
  same port is a drop-in swap for multi-replica correctness.
- `internal/dispatch/execution_fleet_dispatcher.go` now checks the dedup store before producing;
  a duplicate is suppressed (logged, returns `nil`) rather than re-reaching Redpanda. A
  `redpandaMessageWriter` interface was introduced (satisfied by `*kafka.Writer`) purely so this
  path is unit-testable without a broker.
- Test: `execution_fleet_dispatcher_test.go`'s
  `TestDispatchNode_DuplicateIdempotencyKeySuppressedFromReachingBroker` proves three identical
  `DispatchNode` calls reach the underlying writer exactly once.

### 2. Upstream backpressure feedback loop (`redpanda-ingestion-pipeline` + `dag-workflow-scheduler`)

- `BackpressureAwareProducerOptions.onCircuitStateChange` (backpressure-aware-producer.ts) is a
  new optional hook, fired on every circuit-breaker transition, that existing callers are
  unaffected by.
- `src/backpressure-signal-publisher.ts` — `BackpressureSignalPublisher` observes that hook and
  publishes a `TelemetryBackpressureSignal` (new type in `topic-definitions.ts`) onto the new
  `telemetry.backpressure.v1` topic, on a *separate* producer from the one being observed so the
  signal itself is never at risk of being shed by the condition it's reporting.
- `internal/dispatch/telemetry_backpressure_throttle.go` (`dag-workflow-scheduler`) —
  `TelemetryBackpressureThrottle` implements `PipelineDispatchThrottlePort`, consumed by the
  scheduler's dispatch path to pause/slow new node dispatch while backpressure is reported OPEN,
  self-clearing after the signal's advisory `recommendedThrottleMs` even if a follow-up CLOSED
  signal is missed.
- Full signal path, documented in both services' READMEs:

  ```
  BackpressureAwareProducer trips its circuit breaker (telemetry lake under pressure)
    -> BackpressureSignalPublisher publishes telemetry.backpressure.v1
      -> TelemetryBackpressureThrottle (dag-workflow-scheduler) observes the signal
        -> the scheduler's dispatch loop pauses/slows new node dispatch until CLOSED
  ```

### 3. Multi-region control-plane failover posture (documentation + interface only)

`dag-workflow-scheduler` would run against a Temporal namespace configured for Temporal's
multi-cluster ("Global Namespace") replication, so a regional Temporal cluster outage does not
strand in-flight pipeline runs. The concrete config surface for this,
`internal/config/temporal_region_failover_config.go`'s `TemporalRegionFailoverConfig`
(`PrimaryRegion`, `ReplicaRegions`, `FailoverTimeout`), is real Go — but the struct's own doc
comment is explicit about what remains stubbed: no code watches region health or triggers
failover, `cmd/scheduler/main.go` still dials a single `TEMPORAL_HOST_PORT`, and a production
rollout would need `DispatchDeduplicationStore` (see §1) backed by a store that itself survives
or replicates across the same regions — otherwise a post-failover workflow replay could
re-dispatch a node whose dedup record only existed in the failed region's local store. Full
failover orchestration is out of scope for this pass; this ADR and the struct's doc comment are
the record of the intended shape.

### 4. Mid-run secret rotation handling (`pipeline-api-gateway`)

- `src/secret-injection/secret-freshness-tracker.ts` — `SecretFreshnessTracker` records
  issued-at/predicted-expires-at per secret reference (neither `VaultSecretProvider` nor
  `AwsSecretsManagerProvider` currently report a real backend TTL, so expiry is a conservative
  `issuedAt + DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS` estimate) and reports which references are
  due for proactive rotation within `DEFAULT_ROTATION_LEAD_TIME_MILLIS` of predicted expiry.
- `SecretInjectionService.resolveStringPlaceholders` now calls `recordIssued` on every
  resolution, so the tracker's state reflects real injection activity with no separate
  bookkeeping call required from callers.
- `src/secret-injection/mid-run-secret-rotation-scheduler.ts` — `MidRunSecretRotationScheduler`
  periodically sweeps `SecretFreshnessTracker` for due references, re-fetches each via the
  existing `SecretProviderPort` (no new coupling to a concrete Vault/AWS adapter), and pushes the
  refreshed value out via `RotationNotificationPort` — an interface only in this pass, backed by
  a logging-only adapter (`LoggingOnlyRotationNotificationPort` in `secret-injection.module.ts`)
  until a real transport (most plausibly Redpanda, following the platform's existing
  control-plane-to-workers contract) is built for execution-fleet workers to subscribe to.
- Test: `src/__tests__/secret-freshness-tracker.test.ts` exercises the expiry-prediction math
  (lifetime override, lead-time boundary, past-expiry, multi-secret sweep, `forget`).

### 5. Dogfooded observability (`pipeline-api-gateway`)

`src/main.ts` now calls `initializeOpenTelemetryInstrumentation('pipeline-api-gateway')` before
`AppModule` is constructed, registers `correlationIdPropagationMiddleware` ahead of every
resolver/guard, and replaced its one `console.log` with `createStructuredLogger`. This is the
platform's own control plane visibly using the observability kit it ships to every other
service, per that package's stated purpose, rather than being the one control-plane service that
doesn't.

## Consequences

**Positive:**

- A crash-and-retry in `dag-workflow-scheduler` can no longer double-run a load-test node; this
  closes a real correctness gap, not just a theoretical one, given ADR 0001's premise that
  crashes/restarts are expected and handled.
- Telemetry-lake backpressure now actually reduces the load generating that telemetry, instead
  of only ever protecting the lake in isolation while the source of the problem keeps producing
  more of it.
- Long-running load tests (the platform's core use case) are no longer implicitly bounded by
  whichever injected secret has the shortest real-world lifetime.
- `pipeline-api-gateway` is a running example of this platform's own shared observability
  conventions actually working, which is also the best regression test that
  `observability-kit`'s public API is usable as documented.
- The multi-region config surface gives a concrete, reviewable shape for a future failover
  implementation instead of leaving it entirely undocumented.

**Negative / trade-offs:**

- `InMemoryDispatchDeduplicationStore` and the logging-only `RotationNotificationPort` adapter
  are both explicitly single-replica/no-op stand-ins; running more than one
  `dag-workflow-scheduler` replica, or wanting real worker-side secret-rotation delivery, is
  still an open follow-up (a Redis-backed dedup store and a Redpanda-backed rotation-notification
  adapter respectively — both new adapters implementing already-defined ports, not new
  architecture).
- `TelemetryBackpressureThrottle`'s self-clearing behavior means a scheduler that never received
  the original OPEN signal, or whose clock has drifted significantly from the publisher's, could
  under- or over-throttle; this is judged an acceptable trade-off against the alternative
  (throttle stuck forever on a missed CLOSED message).
- `TemporalRegionFailoverConfig` intentionally ships without the orchestration logic to act on
  it — it documents intended shape, not current capability, and should not be read as "multi-
  region failover is implemented."
- Secret rotation's `DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS` is a conservative guess, not a
  backend-reported lease; a secret backend configured with a shorter real TTL than assumed could
  still expire before `MidRunSecretRotationScheduler`'s sweep catches it — extending
  `VaultSecretProvider`/`AwsSecretsManagerProvider` to report a real lease duration (where the
  backend provides one) is a natural, isolated follow-up.
