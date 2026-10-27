# 0012. Wiring the priority run queue to real dispatch, and event-driven approval-to-resume

- Status: Accepted
- Date: 2026-08-11
- Owners: dag-workflow-scheduler maintainers, pipeline-api-gateway maintainers

## Context

ARCHITECTURE.md's "Known scope boundaries" note (and ADR 0004's and the approval-workflow
module's own Consequences/NOTE sections) documented two integration gaps left open on purpose in
earlier passes, both of the same shape: the individual building block was complete and
unit-tested, but the last wire connecting it to the rest of the system was deliberately left
undone, either for schedule reasons (Gap 1) or to avoid a structural problem that needed its own
decision (Gap 2).

**Gap 1 — the fair-share queue was never drained.** ADR 0004 built `WeightedFairShareQueue`
(`dag-workflow-scheduler/internal/scheduling/tenant_priority_run_queue.go`) as a complete,
unit-tested Deficit Round Robin admission-control data structure — but `cmd/scheduler/main.go`'s
`POST /pipeline-runs` handler still called `client.ExecuteWorkflow` directly, never once calling
`Enqueue`. A fairness algorithm that nothing ever calls `DispatchNext` against enforces nothing:
every tenant was still served strict FIFO-on-arrival by Temporal itself, with no per-tenant weight
or concurrency cap in effect — exactly the starvation scenario ADR 0004 set out to prevent.

**Gap 2 — approving a held run didn't resume it.** `ApprovalWorkflowService.approve`
(`pipeline-api-gateway/src/approval-workflow/approval-workflow.service.ts`) flipped an
`ApprovalRequestEntity`'s status to `APPROVED` and stopped there. The `PipelineRunEntity` it was
gating stayed parked in `AWAITING_APPROVAL` forever unless something else independently resumed
it — nothing did. The straightforward fix (`ApprovalWorkflowService` calling
`PipelineRunService.resumeHeldRun` directly) was rejected at the time because it would create a
NestJS module import cycle:

```
PipelineRunModule -> PolicyGateModule -> ApprovalWorkflowModule -> PipelineRunModule
```

(`PipelineRunModule` imports `PolicyGateModule` so `PipelineRunService.triggerRun` can gate a run
before dispatch; `PolicyGateModule` imports `ApprovalWorkflowModule` so `PolicyGateService` can
create an approval request instead of hard-failing on a `REQUIRES_APPROVAL` violation. Closing the
loop with `ApprovalWorkflowModule -> PipelineRunModule` is a cycle Nest's module graph cannot
resolve.)

## Decision

**Gap 1** is closed by making `WeightedFairShareQueue` the real admission path, with a new
draining component behind it:

- `cmd/scheduler/main.go`'s `run()` constructs one `WeightedFairShareQueue` for the process's
  lifetime. `POST /pipeline-runs` now enqueues into it (keyed by an optional `tenantId` request
  field, defaulting to `defaultTenantID` for backward compatibility with callers that don't send
  one yet) instead of calling `client.ExecuteWorkflow` synchronously.
- `internal/scheduling/queue_drain_dispatcher.go`'s `QueueDrainDispatcher` is the background loop:
  it polls `TenantRunQueue.DispatchNext`, and for each admitted run starts it (and awaits its
  completion) on its own goroutine via a new `RunStarterPort` — splitting "start" from "await
  completion" specifically so `MaxConcurrentRuns` bounds runs *truly in flight* (Temporal-accepted
  through terminal state) rather than merely enqueue-to-schedule latency. `MarkCompleted` — which
  frees the tenant's concurrency slot — is only called once `AwaitCompletion` returns.
- `cmd/scheduler/main.go`'s `temporalRunStarterAdapter` is the production `RunStarterPort`,
  holding each run's `PipelineDefinition` (registered by the HTTP handler at enqueue time) until
  `QueueDrainDispatcher` calls `StartRun`, at which point it calls `client.ExecuteWorkflow` under a
  deterministic workflow ID (`dagPipelineWorkflowID(pipelineId, runId)`, computed once so the HTTP
  handler can hand it back to the caller immediately, before the run is necessarily dispatched) and
  blocks `AwaitCompletion` on `client.GetWorkflow(...).Get(...)`.
- `tenant_priority_run_queue.go` and its existing test suite are untouched — `QueueDrainDispatcher`
  only calls the already-defined `TenantRunQueue` port (`Enqueue`, `DispatchNext`, `MarkCompleted`),
  so none of ADR 0004's fairness guarantees needed to change to be actually exercised.

**Gap 2** is closed with an event, not a direct call, per NestJS's standard pattern for breaking a
module cycle while still reacting to a state change owned by another module:

- `pipeline-api-gateway/src/approval-workflow/run-approval-decision.event.ts` defines
  `RunApprovalDecisionEvent` (`approvalRequestId`, `pipelineRunId`, `decision`,
  `resolvedByUserId`, `resolutionNote?`) and the event name constant
  `RUN_APPROVAL_DECISION_EVENT`.
- `ApprovalWorkflowService.resolve` (the shared implementation behind `approve`/`reject`) emits
  this event via `EventEmitter2.emit` (`@nestjs/event-emitter`) once a request's status is
  durably saved as `APPROVED` or `REJECTED` — it never imports or calls `PipelineRunService`.
- `pipeline-api-gateway/src/pipeline-run/run-approval-event.listener.ts`'s
  `RunApprovalEventListener`, registered as a provider in `PipelineRunModule` (not
  `ApprovalWorkflowModule`), subscribes via `@OnEvent(RUN_APPROVAL_DECISION_EVENT)` and calls two
  new `PipelineRunService` methods: `resumeHeldRun(runId)` on `APPROVED` (re-runs the
  fetch-definition/estimate-cost/trigger-scheduler sequence `triggerRun` uses for a fresh run,
  deliberately skipping `PolicyGateService.evaluateBeforeRun` a second time since the human
  approval *is* this run's resolution of that gate) and `failHeldRun(runId, reason)` on
  `REJECTED` (transitions the run straight to `FAILED`).
- `EventEmitterModule.forRoot()` is registered once, globally, in `app.module.ts`, so
  `EventEmitter2` is injectable everywhere (including inside `ApprovalWorkflowService`) and
  `@OnEvent` listeners anywhere in the app are wired up automatically, without either
  `ApprovalWorkflowModule` or `PipelineRunModule` needing to import the other. The module graph
  stays exactly as it was before this change — only a plain TypeScript import of an event
  class/constant crosses the `approval-workflow` / `pipeline-run` boundary, which carries no
  `@Module`-graph weight at all.

## Consequences

**Positive:**

- `WeightedFairShareQueue`'s DRR fairness and per-tenant concurrency cap are now real constraints
  on `dag-workflow-scheduler`'s actual dispatch behavior, not only on a data structure exercised
  solely by its own unit tests.
- `RunStarterPort`'s start/await split means `MaxConcurrentRuns` means what its doc comment always
  said it meant — runs genuinely in flight — rather than accidentally only bounding how many
  `client.ExecuteWorkflow` calls are in flight at once (which would have under-protected the
  execution fleet, since a started-but-still-running workflow keeps dispatching nodes long after
  `ExecuteWorkflow` itself returns).
- The event-driven approval-to-resume flow generalizes: any future module that needs to react to
  an approval decision (e.g. an audit-log service, a Slack notifier) can add its own
  `@OnEvent(RUN_APPROVAL_DECISION_EVENT)` listener without `ApprovalWorkflowModule` needing to
  know it exists, let alone import it.
- Neither change touched `WeightedFairShareQueue`'s or `ApprovalWorkflowService`'s existing public
  behavior/tests — both gaps were closed purely by adding new integration code around
  already-correct, already-tested components.

**Negative / trade-offs:**

- `POST /pipeline-runs`'s response contract changed subtly: `runId` is now this scheduler's own
  queued-run identifier assigned at enqueue time, not Temporal's run ID (which isn't known until
  `QueueDrainDispatcher` actually starts the workflow, and the whole point of admission control is
  that this may happen well after the HTTP response is sent). Callers that were relying on the
  returned `runId` being Temporal's own run ID need to instead track by the deterministic
  `workflowId` also returned, or query run status through `pipeline-api-gateway`'s existing
  `PipelineRunEntity`/webhook-status path, which was never coupled to Temporal's run ID scheme
  in the first place.
- `EventEmitter2`'s dispatch is in-process and fire-and-forget from `ApprovalWorkflowService`'s
  point of view: `resolve` does not await `RunApprovalEventListener`'s handler, and a
  `pipeline-api-gateway` process crash between the event firing and the listener's
  `resumeHeldRun`/`failHeldRun` completing would leave the run stuck in `AWAITING_APPROVAL` with
  no automatic retry. This mirrors the same "in-memory, single-process, no delivery guarantee
  beyond the current process's lifetime" caveat `pipeline-run.pubsub.ts` and
  `WeightedFairShareQueue` both already carry — see each service's `SCALING.md` — and is not
  solved differently here. A production hardening pass would persist the pending decision (or
  publish it to Redpanda, consistent with `ARCHITECTURE.md`'s general message-bus posture) rather
  than relying purely on an in-memory event bus surviving until the listener runs.
- `QueueDrainDispatcher` is, like `WeightedFairShareQueue` itself, single-process: running more
  than one `dag-workflow-scheduler` replica means each replica runs its own drain loop against its
  own in-memory queue, inheriting the exact same multi-replica caveat ADR 0004 already documented
  for `WeightedFairShareQueue` alone (see `SCALING.md`) — this pass does not add a new limitation,
  but does not remove the existing one either.
