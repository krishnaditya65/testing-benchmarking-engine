# 0004. Dynamic DAG mutation via Temporal signals, and multi-tenant fair-share run queues

- Status: Accepted
- Date: 2025-03-11
- Owners: dag-workflow-scheduler maintainers, orchestration-control-plane team

## Context

Two related gaps showed up as the platform moved from single-tenant, single-shot pipeline runs
toward real multi-team, always-on usage.

**Gap 1 — pipelines are static once started.** `DagPipelineWorkflow` (ADR 0001) executes exactly
the node/edge graph it was started with, resolved once at trigger time. In practice, engineers
diagnosing a live incident want to react to what a run is *currently* showing: if p99 latency
during a `LOAD_TEST` node's ramp-up phase blows past a threshold, they want a detailed trace/
profile snapshot captured right then, while the anomaly is still happening — not reconstructed
after the fact from `DagFlakinessBisector`'s post-hoc bisection (ADR 0001's sibling feature),
which can only ever re-run *after* the original run has already finished and the transient state
that caused the anomaly is gone. Equally, once telemetry has already confirmed a branch's outcome
is moot (e.g. the target service has already tipped over), continuing to dispatch that branch's
remaining nodes only wastes execution-fleet capacity that other runs need. Static, trigger-time-only
DAGs cannot express either reaction.

**Gap 2 — one tenant's runs can starve every other tenant's runs.** `dag-workflow-scheduler`
serves every tenant on the platform out of the same Temporal namespace and the same
execution-fleet capacity. `POST /pipeline-runs` (ADR 0001) starts a `DagPipelineWorkflow`
execution unconditionally the moment it's called — there is no admission control layer, and
Temporal itself has no notion of "tenant." A tenant submitting a burst of runs (a CI pipeline
retrying in a loop, a misconfigured cron) can consume disproportionate scheduler and
execution-fleet capacity, delaying or effectively blocking every other tenant's runs behind it,
with no platform-level fairness guarantee to prevent it.

## Decision

**Dynamic DAG mutation** is modeled as Temporal signals delivered to the already-running
`DagPipelineWorkflow` execution, not as a new workflow type or an out-of-band mutation API:

- `internal/workflow/dag_mutation_signal_handler.go` defines `DagMutationSignal` (`MutationType`
  one of `InjectDiagnosticNode`, `SkipRemainingBranch`, `AbortRun`) delivered over Temporal's
  named signal channel `DagMutationSignalChannelName`, and `DagMutationSignalHandler`, which
  drains pending signals and exposes the resulting mutation decisions.
- `DagPipelineWorkflow`'s main loop calls `DrainPendingSignals` **between** resolving each node in
  its Kahn's-algorithm frontier — never mid-dispatch — so a mutation always lands at a clean DAG-walk
  boundary and can never race an in-flight activity.
- `internal/workflow/live_telemetry_threshold_watcher.go`'s `LiveTelemetryThresholdWatcher` is the
  signal's producer: a goroutine, running outside workflow code (since it makes real,
  non-deterministic network calls on a wall-clock timer), that polls `telemetry-query-api` for a
  run's live p99 latency and calls `client.SignalWorkflow` once a threshold is breached for
  several consecutive polls.

**Multi-tenant fair-share scheduling** is implemented as an admission-control layer sitting in
front of `client.ExecuteWorkflow`, not as a change to Temporal's own task-queue behavior (which
has no tenant concept to hook into):

- `internal/scheduling/tenant_priority_run_queue.go` defines the `TenantRunQueue` port and
  `WeightedFairShareQueue`, an in-memory implementation using Deficit Round Robin (DRR) — the
  technique network packet schedulers use for weighted bandwidth allocation, adapted here to run
  dispatch: each tenant is granted `TenantSchedulingPolicy.Weight` credits per turn and spends one
  per dispatched run, so weight ratios hold proportionally over time without ever fully starving a
  lower-weight tenant.
- `TenantSchedulingPolicy.MaxConcurrentRuns` is a hard per-tenant concurrency cap enforced
  independent of weight, closing the specific starvation case a weight-only scheme cannot: a very
  high-weight, very bursty tenant still cannot monopolize every execution-fleet worker at once.

## Consequences

**Positive:**

- Mid-run reaction to live telemetry becomes possible without inventing a second workflow
  execution model — it reuses Temporal's existing signal mechanism, which is already
  durable/replay-safe by construction, so a scheduler crash mid-mutation-application replays
  correctly like everything else in `DagPipelineWorkflow`.
- The signal-check-point design (drain only between node executions) means mutation logic never
  has to reason about partially-in-flight activity dispatches — a substantial simplification.
- `WeightedFairShareQueue`'s port/implementation split (`TenantRunQueue` interface) means the
  in-memory fairness algorithm can be swapped for a distributed (Redis/Postgres-backed)
  implementation later without touching whatever calls `Enqueue`/`DispatchNext`.
- DRR is a well-understood, proven algorithm for exactly this class of problem (proportional
  fairness with a hard cap), rather than a bespoke priority heuristic invented for this feature.

**Negative / trade-offs:**

- `LiveTelemetryThresholdWatcher` runs outside Temporal's durability guarantees: a scheduler
  process restart silently kills every in-flight watcher, disabling live-mutation reaction for
  whatever runs it was watching until a new watcher is (re-)started for them. See `SCALING.md`
  for the two follow-up options considered (durable child-workflow-based watching, or a
  separately-scaled watcher pool that resubscribes on startup) — neither is implemented in this
  pass.
- `WeightedFairShareQueue` is in-memory and single-process, exactly like the existing
  `InMemoryDispatchDeduplicationStore` (ADR 0011) — running more than one `dag-workflow-scheduler`
  replica without a shared queue backend means each replica enforces `MaxConcurrentRuns`
  independently, silently multiplying the effective per-tenant concurrency cap by replica count.
  This is an explicitly documented limitation (see `SCALING.md`), not resolved here.
- The queue and its fairness algorithm are complete and unit-tested in this pass, but
  `cmd/scheduler/main.go`'s `POST /pipeline-runs` handler is not yet wired to enqueue through it —
  it still calls `client.ExecuteWorkflow` directly. Wiring the handler to `Enqueue` and adding a
  `DispatchNext` dispatch loop is the next integration step, not yet done.
- Mid-run mutation adds a second way the DAG walk's node/edge state can change beyond the original
  static `PipelineDefinition` — engineers debugging a run's history now need to account for
  mutation signals as a possible explanation for why a run's actual node set differs from its
  authored definition, which structural-diffing (ADR 0005) helps make visible after the fact but
  does not eliminate as a source of complexity.
