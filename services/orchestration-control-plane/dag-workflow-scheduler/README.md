# dag-workflow-scheduler

Durable DAG run orchestration for the Orchestration Control Plane layer, built on the
[Temporal Go SDK](https://pkg.go.dev/go.temporal.io/sdk). This service **plans and schedules
only** — it never generates load itself. It dispatches pipeline nodes to the Execution Fleet
layer (e.g. `services/execution-fleet/protocol-load-worker-go`) over the message bus and lets
Temporal durably track each pipeline run's state.

## Feature manifest mapping

### "Mathematical DAG execution with conditional branching"

Implemented in [`internal/workflow/dag_pipeline_workflow.go`](internal/workflow/dag_pipeline_workflow.go),
`DagPipelineWorkflow`:

- Builds an adjacency list and in-degree map from `PipelineDefinition.Edges`.
- Runs a Kahn's-algorithm topological walk: a node becomes eligible to resolve only once every
  edge pointing at it has been accounted for.
- For each node, `resolveNode` first checks `nodeShouldBeSkipped`, which evaluates every incoming
  `PipelineEdge.Condition` (`ON_PARENT_PASSED`, `ON_PARENT_FAILED`, `ALWAYS`) against the parent's
  **real** `NodeExecutionResult.Status`. A child only executes if at least one incoming edge's
  condition is actually satisfied by its parent's outcome — e.g. an `AssertionNode`'s contract
  check failing correctly prevents an `ON_PARENT_PASSED` child from ever dispatching, and that
  child is recorded with `NodeExecutionStatusSkipped` in the final `PipelineRunResult`.
- Eligible nodes are dispatched via `workflow.ExecuteActivity` (with `ActivityOptions` including a
  `StartToCloseTimeout` and a `RetryPolicy`) against
  `DispatchNodeToExecutionFleetActivity` in [`internal/workflow/activities.go`](internal/workflow/activities.go).
- Cycles/dangling edge references are detected: if Kahn's algorithm cannot resolve every node, the
  workflow returns an error instead of a silently partial result.

### "DAG flakiness / root-cause bisecting"

Implemented in [`internal/diagnostics/dag_flakiness_bisector.go`](internal/diagnostics/dag_flakiness_bisector.go),
`DagFlakinessBisector.BisectFlakiness`:

- Computes the full ancestor subgraph of a known-flaky node (`ancestorsOf`, a reverse BFS over
  `PipelineDefinition.Edges`).
- Repeatedly splits the current candidate ancestor set in half and re-executes the flaky node
  alongside each half via the injected `PartialDagReexecutor` port, comparing which half's rerun
  reproduces the failure.
- Shrinks the candidate set toward whichever half cleanly reproduces the failure (or narrows
  heuristically if the signal is ambiguous), terminating with a single suspected ancestor.
- Returns a `FlakinessRootCauseReport` with `SuspectedRootCauseNodeID`, a `ConfidencePercent`
  (higher when the final split was unambiguous), and `RerunsPerformed` for auditability.

### "Dynamic DAG mutation mid-run"

Implemented in [`internal/workflow/dag_mutation_signal_handler.go`](internal/workflow/dag_mutation_signal_handler.go)
and [`internal/workflow/live_telemetry_threshold_watcher.go`](internal/workflow/live_telemetry_threshold_watcher.go):

- `DagMutationSignal` (`MutationType` one of `InjectDiagnosticNode`, `SkipRemainingBranch`,
  `AbortRun`) is delivered to a running `DagPipelineWorkflow` execution over the Temporal signal
  channel `DagMutationSignalChannelName`.
- `DagPipelineWorkflow`'s main loop calls `DagMutationSignalHandler.DrainPendingSignals` **between**
  resolving each node in its Kahn's-algorithm frontier — the "signal-check point" — so a mutation
  always takes effect at a clean DAG-walk boundary and never races an in-flight activity dispatch.
- `InjectDiagnosticNode` splices a new `DIAGNOSTIC_CAPTURE` node in after a named node, extending
  `nodesByID`/`childEdgesByParent`/`inDegree` live; `SkipRemainingBranch` prunes every not-yet-resolved
  descendant of a named node (`expandPrunedNodeIDs`); `AbortRun` halts the walk and returns whatever
  `NodeExecutionResult`s were already recorded.
- `LiveTelemetryThresholdWatcher` is the signal's producer: it polls `telemetry-query-api` for a
  run's live p99 latency during ramp-up and, once the configured threshold is breached for
  `ConsecutiveBreachesRequired` consecutive polls, calls `client.SignalWorkflow` with an
  `INJECT_DIAGNOSTIC_NODE` signal so a trace-capture node is spliced in while the anomaly is still
  happening. It runs outside the workflow (alongside the Temporal worker) since it makes real,
  non-deterministic network calls on a wall-clock timer, which workflow code itself cannot do.

### "Multi-tenant priority run queues"

Implemented in [`internal/scheduling/tenant_priority_run_queue.go`](internal/scheduling/tenant_priority_run_queue.go)
(tests: [`tenant_priority_run_queue_test.go`](internal/scheduling/tenant_priority_run_queue_test.go)):

- `TenantRunQueue` is the port; `WeightedFairShareQueue` is the in-memory implementation, using
  Deficit Round Robin (DRR) — the technique network packet schedulers use for weighted-fair
  bandwidth allocation — adapted to run dispatch: `DispatchNext` grants each tenant
  `TenantSchedulingPolicy.Weight` "credits" per turn and spends one per dispatched run, so a
  Weight-3 tenant dispatches roughly 3 runs for every 1 a Weight-1 tenant dispatches, without
  starving the lighter tenant entirely.
- `TenantSchedulingPolicy.MaxConcurrentRuns` is a hard per-tenant concurrency cap enforced
  independent of `Weight` — this is what actually prevents one bursty high-weight tenant from
  monopolizing every execution-fleet worker at once.
- This queue sits in front of `client.ExecuteWorkflow` **and is now actually wired end to end**
  (see ADR [0012](../../../docs/architecture-decision-records/0012-priority-queue-dispatch-and-event-driven-approval-resume.md)):
  `cmd/scheduler/main.go`'s `POST /pipeline-runs` handler enqueues into a
  process-lifetime `WeightedFairShareQueue` instead of calling `client.ExecuteWorkflow` directly,
  and [`internal/scheduling/queue_drain_dispatcher.go`](internal/scheduling/queue_drain_dispatcher.go)'s
  `QueueDrainDispatcher` is the background loop that continuously calls `DispatchNext` and starts
  each admitted run's `DagPipelineWorkflow` execution — see "HTTP trigger" below for the full flow.

### "Priority-queue-backed dispatch loop"

Implemented in [`internal/scheduling/queue_drain_dispatcher.go`](internal/scheduling/queue_drain_dispatcher.go)
(tests: [`queue_drain_dispatcher_test.go`](internal/scheduling/queue_drain_dispatcher_test.go)):

- `RunStarterPort` splits "start a run" from "await its completion" into two methods precisely so
  `MaxConcurrentRuns` caps runs *truly in flight* (from Temporal-accepted start to terminal state),
  not merely enqueue-to-schedule latency: `QueueDrainDispatcher` only calls `TenantRunQueue.MarkCompleted`
  — freeing the tenant's concurrency slot — after `AwaitCompletion` returns, whether the run
  succeeded, failed, or `StartRun` itself errored.
- `QueueDrainDispatcher.Run` polls `DispatchNext` in a loop (default interval:
  `DefaultPollInterval`, 25ms), draining every currently-eligible run on each sweep before sleeping,
  and starts/awaits each dispatched run on its own goroutine so one run's latency never blocks the
  next sweep or another tenant's turn.
- `cmd/scheduler/main.go`'s `temporalRunStarterAdapter` is the production `RunStarterPort`: it holds
  each run's `PipelineDefinition` (registered by the HTTP handler at enqueue time, since
  `QueuedPipelineRun` itself only carries routing metadata) until `QueueDrainDispatcher` calls
  `StartRun`, then calls `client.ExecuteWorkflow` under a deterministic workflow ID and blocks
  `AwaitCompletion` on `client.GetWorkflow(...).Get(...)`.

### "DAG definition versioning & structural diffing" (Go side)

Implemented in [`internal/domain/pipeline_definition_version.go`](internal/domain/pipeline_definition_version.go)
and [`internal/diagnostics/dag_structural_diff_engine.go`](internal/diagnostics/dag_structural_diff_engine.go):

- `PipelineDefinitionSnapshot` is an immutable, point-in-time DAG capture; `ComputeVersionIdentity`
  derives a canonicalized (sorted, order-independent) SHA-256 content hash so two snapshots with
  identical DAG shape hash identically regardless of slice ordering.
- `DagStructuralDiffEngine.Diff` compares two snapshots of the same pipeline and reports every
  added/removed/modified node and edge as a structured `DagStructuralDiffReport` — this is the
  mechanism that catches "someone silently removed the consistency-assertion node" between two
  versions.
- **Shared contract, not shared logic**: this engine's evaluation is duplicated independently by
  `pipeline-api-gateway`'s `pipeline-definition-diff.service.ts` (see that service's own file
  header and ADR 0002's precedent for why). The type contract both report shapes conform to lives
  in `services/shared-libraries/dag-domain-model/src/diffing/dag-structural-diff.types.ts`.

## Message-bus contract to Execution Fleet workers

Per `ARCHITECTURE.md`'s "Control plane -> workers" contract, this service **only ever produces**
onto Redpanda (Kafka-API compatible) — it never calls execution-fleet workers directly:

- **Topic**: `dag.node.dispatch.v1` (see `dispatch.NodeDispatchTopic` in
  [`internal/dispatch/execution_fleet_dispatcher.go`](internal/dispatch/execution_fleet_dispatcher.go)).
- **Producer**: `RedpandaExecutionFleetDispatcher`, built on
  [`github.com/segmentio/kafka-go`](https://github.com/segmentio/kafka-go)'s `kafka.Writer`,
  keyed by `RunID` so all dispatches for one pipeline run stay ordered relative to each other.
- **Payload**: `dispatch.NodeDispatchMessage` (`RunID`, `Node`, `DispatchedAtUnixMillis`,
  `AttemptGeneration`), currently JSON-encoded. `AttemptGeneration` (Temporal's own activity
  attempt number) feeds `DeriveDispatchIdempotencyKey` so a crash-and-retry cannot double-produce
  the same dispatch — see "Idempotent dispatch" in `SCALING.md`. This is a **local stand-in** —
  the shared contract is meant to live at
  `shared-libraries/protocol-contracts/proto/dag_dispatch.proto`, which does not exist in this
  repo yet. Once that proto lands, `NodeDispatchMessage` should be replaced with generated
  protobuf types and a binary payload.
- Execution-fleet workers (e.g. `protocol-load-worker-go`) are stateless consumers of this topic;
  they take orders over the bus and never call back into this scheduler synchronously.

## Durability by design

Per `ARCHITECTURE.md`'s "Durability by design" principle, `DagPipelineWorkflow` executions are
Temporal workflows: all workflow state (which nodes have resolved, their results, where the
topological walk currently is) is persisted by Temporal itself, not held only in this process's
memory. If a scheduler worker process crashes or restarts mid-run, Temporal replays the workflow's
event history against the same deterministic workflow code and resumes exactly where it left off —
no in-flight pipeline run is lost. See `SCALING.md` for how this shapes horizontal scaling.

## Layout

```
cmd/scheduler/main.go                                       entrypoint: Temporal client+worker, Redpanda producer, HTTP trigger
internal/domain/pipeline.go                                 DAG data shapes shared across packages
internal/domain/pipeline_definition_version.go               PipelineDefinitionSnapshot + hash-based version identity
internal/workflow/dag_pipeline_workflow.go                  DagPipelineWorkflow: topological walk + conditional branching + mid-run mutation
internal/workflow/activities.go                             DispatchNodeToExecutionFleetActivity
internal/workflow/dag_mutation_signal_handler.go              DagMutationSignal handling: inject/skip/abort mid-run
internal/workflow/live_telemetry_threshold_watcher.go         Polls telemetry-query-api, emits mutation signals on threshold breach
internal/dispatch/execution_fleet_dispatcher.go             Redpanda producer for dag.node.dispatch.v1
internal/dispatch/dispatch_idempotency_key.go                deterministic (runId, nodeId, attempt) idempotency key
internal/dispatch/idempotent_dispatch_deduplicator.go        DispatchDeduplicationStore port + in-memory adapter
internal/dispatch/telemetry_backpressure_throttle.go         consumes telemetry.backpressure.v1, throttles dispatch
internal/config/temporal_region_failover_config.go          multi-region Temporal namespace failover config (posture only)
internal/diagnostics/dag_flakiness_bisector.go               DagFlakinessBisector: ancestor-subgraph bisection
internal/diagnostics/dag_structural_diff_engine.go            DagStructuralDiffEngine: node/edge structural diffing
internal/scheduling/tenant_priority_run_queue.go              TenantRunQueue / WeightedFairShareQueue: fair-share run dispatch
internal/scheduling/queue_drain_dispatcher.go                  QueueDrainDispatcher: drains WeightedFairShareQueue, starts runs
```

## Robustness hardening (idempotent dispatch, backpressure throttling, multi-region posture)

See `SCALING.md`'s "Idempotent dispatch", "Upstream backpressure feedback loop", and
"Multi-region failover posture" sections, and
[ADR 0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md)
for the full rationale behind `internal/dispatch/dispatch_idempotency_key.go`,
`internal/dispatch/idempotent_dispatch_deduplicator.go`,
`internal/dispatch/telemetry_backpressure_throttle.go`, and
`internal/config/temporal_region_failover_config.go`.

## HTTP trigger

`POST /pipeline-runs` (called by the sibling `pipeline-api-gateway` NestJS service) accepts
`{"definition": <PipelineDefinition>, "tenantId": <string>}` (`tenantId` is optional — it falls
back to `defaultTenantID` so older callers that don't send it yet still work) and **enqueues** the
run into the process-lifetime `WeightedFairShareQueue` constructed in `main.go`'s `run()`, rather
than starting a `DagPipelineWorkflow` execution synchronously. It responds `202 Accepted` with
`{"workflowId", "runId"}` immediately: `workflowId` is the deterministic Temporal workflow ID this
run will execute under once dispatched (`dagPipelineWorkflowID(pipelineId, runId)`), and `runId` is
this scheduler's own queued-run identifier.

The full flow, end to end:

1. `POST /pipeline-runs` decodes the request, registers its `PipelineDefinition` with
   `temporalRunStarterAdapter`, and calls `WeightedFairShareQueue.Enqueue` keyed by `tenantId`.
2. `QueueDrainDispatcher.Run` (started as a goroutine in `main.go`'s `run()`, alongside the Temporal
   worker and the HTTP server) continuously calls `DispatchNext`, which applies the Deficit Round
   Robin fairness algorithm across every tenant with a pending run.
3. Once a run is selected, `QueueDrainDispatcher` calls `temporalRunStarterAdapter.StartRun`, which
   looks up the registered `PipelineDefinition` and starts its `DagPipelineWorkflow` execution via
   `client.ExecuteWorkflow`.
4. `DagPipelineWorkflow` (see above) durably orchestrates the DAG walk, dispatching each eligible
   node to the Execution Fleet over `dag.node.dispatch.v1` via `RedpandaExecutionFleetDispatcher`.
5. `QueueDrainDispatcher` blocks on `temporalRunStarterAdapter.AwaitCompletion` until the workflow
   reaches a terminal state, then calls `WeightedFairShareQueue.MarkCompleted` to free the tenant's
   concurrency slot for its next queued run.
