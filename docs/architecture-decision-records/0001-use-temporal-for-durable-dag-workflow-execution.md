# 0001. Use Temporal for durable DAG workflow execution

- Status: Accepted
- Date: 2024-02-12
- Owners: dag-workflow-scheduler maintainers, orchestration-control-plane team

## Context

`dag-workflow-scheduler` (`services/orchestration-control-plane/dag-workflow-scheduler`)
is responsible for driving a test pipeline's DAG from node to node — dispatching each
node to the execution fleet over `dag.node.dispatch.v1`, waiting for completion or
failure, evaluating `dependsOn` edges, and deciding what runs next. Pipelines like
`examples/pipelines/checkout-load-test.yaml` can run for 30+ minutes end to end
(provision → smoke test → seed 1M rows → 50k-VU load test → assertions → teardown),
and a single load-test node alone can run for tens of minutes.

If DAG execution state (which nodes have completed, which are in flight, accumulated
quality-gate results) lives only in the scheduler process's memory or a plain
database row updated ad hoc, a scheduler pod restart — a routine deploy, an OOM kill,
a node drain during a cluster upgrade — either loses in-flight run state or requires
building a bespoke, hand-rolled recovery/replay mechanism. That mechanism would need
to solve: at-least-once dispatch without duplicate side effects, timeout handling per
node, retries with backoff, and a consistent view of "what already happened" after a
crash. This is a well-known distributed-systems problem with mature existing
solutions; building it from scratch inside `dag-workflow-scheduler` was assessed as
high risk for a component the entire platform depends on.

## Decision

`dag-workflow-scheduler` models each pipeline run as a Temporal Workflow, with each
DAG node dispatch modeled as a Temporal Activity. Workflow execution history is
persisted by Temporal's own durable event log, not by `dag-workflow-scheduler`'s
application state. The service embeds a Temporal worker and talks to a
self-hosted Temporal cluster (Temporal frontend service referenced as
`TESTFORGE_TEMPORAL_ADDRESS` in `infrastructure/kubernetes/base/control-plane-services-deployment.yaml`
and the `pipeline-api-gateway` Helm chart).

Concretely:

- A pipeline run's DAG traversal (dependency resolution, fan-out/fan-in, node
  timeouts, retries) is expressed as workflow code — deterministic, replayable from
  history.
- Each node dispatch (publish to `dag.node.dispatch.v1`, await completion signal) is
  an Activity, so Temporal handles Activity-level retry/backoff and heartbeating
  independent of the workflow's own logic.
- `pipeline-api-gateway`'s `PipelineRun` GraphQL type is a read-through projection of
  Temporal workflow state, not a separately maintained state machine.

## Consequences

**Positive:**

- A `dag-workflow-scheduler` pod crash or rolling deploy has zero effect on in-flight
  pipeline runs — a new worker picks up exactly where the workflow history left off.
  This is the mechanism referenced in `ARCHITECTURE.md`'s "Durability by design"
  section.
- Retries, timeouts, and backoff for node dispatch are configuration, not hand-rolled
  code, which reduces the surface area for a whole class of orchestration bugs.
- Temporal's Web UI and CLI give operators workflow-level observability (event
  history, pending activities) essentially for free, ahead of the platform's own
  `telemetry-query-api` dashboards.

**Negative / trade-offs:**

- Adds an operational dependency: the platform now runs (or depends on) a Temporal
  cluster, which itself needs to be provisioned, upgraded, and monitored — one more
  stateful system alongside Postgres, Redpanda, and ClickHouse.
- Workflow code must stay deterministic (no direct network calls, no unguarded
  `time.Now()`/randomness inside workflow functions) — a real constraint the
  `dag-workflow-scheduler` team has to actively review for in every change, since
  violating it silently breaks replay.
- Introduces a new mental model (Workflows vs. Activities) that engineers touching
  `dag-workflow-scheduler` need to learn; mitigated by Go being the implementation
  language, keeping it in the same language and toolchain as the rest of the
  execution-fleet Go workers.
