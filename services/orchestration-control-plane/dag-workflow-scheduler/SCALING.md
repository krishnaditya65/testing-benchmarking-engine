# Scaling notes

This scheduler is largely stateless at the process level: Temporal itself persists all
workflow and activity state durably (in Temporal's own datastore, backed by Postgres per
`docker-compose.yml`'s `dag-workflow-temporal` service), so `DagPipelineWorkflow` executions do
not depend on any single scheduler process staying alive. A scheduler worker process can crash or
be killed and restarted freely — Temporal replays the affected workflow's event history against
the same deterministic workflow code and resumes exactly where it left off, with no lost or
duplicated node dispatches. Scale horizontally by adding more `worker.Run` replicas (more
`cmd/scheduler` instances) pulling from the same `dag-pipeline-task-queue`; Temporal's server
handles distributing ready activity and workflow tasks across whichever workers are currently
polling, so no manual sharding is needed. The `POST /pipeline-runs` HTTP trigger endpoint is
itself stateless — it only starts a workflow via `client.ExecuteWorkflow` and returns its IDs — so
it can sit behind a standard load balancer with any number of replicas. Two failure modes are
worth calling out: a worker process dying mid-activity is handled by Temporal's activity retry
policy (`dispatchActivityRetryPolicy` in `dag_pipeline_workflow.go`, with bounded backoff and max
attempts), and Redpanda backpressure or unavailability on the `dag.node.dispatch.v1` topic
surfaces as a failed `DispatchNodeToExecutionFleetActivity` attempt, which Temporal likewise
retries. TODO for production hardening: the `RedpandaExecutionFleetDispatcher`'s underlying
`kafka.Writer` should get explicit bounded-retry/backoff and circuit-breaking tuning of its own
(distinct from Temporal's activity-level retries) so sustained broker backpressure degrades
gracefully instead of only relying on activity retry exhaustion.

## Idempotent dispatch (no duplicated node dispatches, for real)

The "no lost or duplicated node dispatches" claim above used to hold only for the *lost* half:
Temporal's replay guarantees a crashed worker resumes correctly, but nothing stopped a retried
activity attempt from re-producing a `NodeDispatchMessage` whose original produce had actually
already reached Redpanda (e.g. the scheduler crashed between a successful write and Temporal
durably recording the activity complete). See ADR
[0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md)
for the full rationale; concretely:

- `internal/dispatch/dispatch_idempotency_key.go`'s `DeriveDispatchIdempotencyKey` derives a
  deterministic key from `(pipelineRunId, nodeId, attemptGeneration)`, where
  `attemptGeneration` is Temporal's own `activity.GetInfo(ctx).Attempt`.
- `internal/dispatch/idempotent_dispatch_deduplicator.go`'s `DispatchDeduplicationStore` port
  (with `InMemoryDispatchDeduplicationStore` as the correct-for-a-single-replica default) is
  checked by `RedpandaExecutionFleetDispatcher.DispatchNode` before every produce; a duplicate
  key short-circuits before the Redpanda write happens at all.
- **Operational note for horizontal scaling:** `InMemoryDispatchDeduplicationStore` is
  per-process. Running more than one `dag-workflow-scheduler` replica (as this document
  recommends above) means two replicas could race to handle the same retried activity attempt
  and each maintain their own, non-shared dedup record. Production multi-replica deployments
  should construct the dispatcher via `NewRedpandaExecutionFleetDispatcherWithDeduplication`
  with a shared, Redis-backed `DispatchDeduplicationStore` implementation instead (the port is
  intentionally shaped like Redis's `SET key val NX PX ttl` so this is a drop-in adapter, not a
  design change).

## Upstream backpressure feedback loop

`internal/dispatch/telemetry_backpressure_throttle.go`'s `TelemetryBackpressureThrottle`
consumes `TelemetryBackpressureSignal` messages published on `telemetry.backpressure.v1` by
`redpanda-ingestion-pipeline`'s `BackpressureSignalPublisher` (see that package's README for the
producer side) and implements `PipelineDispatchThrottlePort`, which the dispatch path consults
before issuing new work. This closes the loop so a saturated telemetry lake actually slows this
scheduler's dispatch rate instead of only ever protecting itself in isolation while the
scheduler keeps dispatching nodes that generate more of the telemetry causing the saturation.

## Multi-region failover posture

`internal/config/temporal_region_failover_config.go`'s `TemporalRegionFailoverConfig`
(`PrimaryRegion`, `ReplicaRegions`, `FailoverTimeout`) documents the configuration surface for
running against a Temporal namespace with multi-cluster replication. See ADR 0011 for what is
and isn't implemented behind it — this is a real config struct, not yet a real failover
mechanism.

## Multi-tenant fair-share scheduling (`internal/scheduling`)

`WeightedFairShareQueue` (see ADR
[0004](../../../docs/architecture-decision-records/0004-dynamic-dag-mutation-and-multi-tenant-scheduling.md))
is, like `InMemoryDispatchDeduplicationStore` above, an in-memory, single-process data structure
— it holds no state in Postgres, Redis, or Temporal. Every `dag-workflow-scheduler` replica that
runs its own `WeightedFairShareQueue` maintains an independent view of each tenant's backlog and
deficit-credit balance; there is no cross-replica coordination. This is fine for a single
replica, but running more than one replica (as this document recommends for `worker.Run`
scaling) with each replica also independently admitting runs from `POST /pipeline-runs` would
let two replicas each grant a tenant its `MaxConcurrentRuns` cap *per replica*, silently
multiplying the effective concurrency cap by replica count — exactly the kind of bug this
feature exists to prevent, just moved up a level. Production multi-replica deployments should
either pin admission control (the `Enqueue`/`DispatchNext` loop) to a single active replica
(leader election), or replace `WeightedFairShareQueue`'s in-memory state with a Redis- or
Postgres-backed `TenantRunQueue` implementation shared across replicas, behind the same
interface.

**Now real, end to end** (see ADR
[0012](../../../docs/architecture-decision-records/0012-priority-queue-dispatch-and-event-driven-approval-resume.md)):
`cmd/scheduler/main.go`'s `run()` constructs one `WeightedFairShareQueue` for the process's
lifetime, the `POST /pipeline-runs` handler enqueues into it (keyed by the request's `tenantId`,
defaulting to `defaultTenantID` if absent), and a `QueueDrainDispatcher` goroutine — also started
in `run()`, alongside the Temporal worker and the HTTP server — continuously calls `DispatchNext`
and starts each admitted run via `client.ExecuteWorkflow`. `QueueDrainDispatcher` starts and
awaits each dispatched run on its own goroutine (never blocking the poll loop on one run's
lifetime), and only calls `MarkCompleted` — freeing the tenant's concurrency slot — once
`AwaitCompletion` observes the workflow reach a terminal state via `client.GetWorkflow(...).Get(...)`,
so `MaxConcurrentRuns` genuinely bounds runs in flight rather than merely enqueue-to-schedule
latency. The single-process caveat above is unchanged by this: `QueueDrainDispatcher` drains
whatever `WeightedFairShareQueue` instance it's given, so the same multi-replica leader-election /
shared-backend follow-up applies to it too.

## Mid-run mutation signal delivery (`internal/workflow`)

`DagMutationSignalHandler` itself requires no scaling notes beyond what already applies to every
`DagPipelineWorkflow` execution (Temporal owns and replays its state; see the top of this
document). `LiveTelemetryThresholdWatcher`, however, is a long-lived goroutine per watched run,
started outside the workflow — if it is spawned inside the same `cmd/scheduler` process that
also runs the Temporal worker (as sketched in this pass), a scheduler process restart kills every
in-flight watcher along with it, silently disabling live-mutation reaction for whatever runs it
was watching until a fresh watcher is started for them. A production hardening would either make
watcher registration itself a durable Temporal child workflow (so it benefits from the same
replay guarantees as everything else in this service), or run watchers in a separate,
independently-scaled worker pool that resubscribes to "runs currently in their ramp-up phase"
from `pipeline-api-gateway`'s run-status projection on startup.
