# Scaling notes

`pipeline-api-gateway` is horizontally scalable and stateless at the process
level: authentication is JWT-based (no server-side session store), so any
replica can serve any request without sticky sessions. The one real caveat
in this starter is GraphQL subscriptions — `pipeline-run.module.ts` currently
registers the in-memory `PubSub` from `graphql-subscriptions`, which only
fans out `pipelineRunStatusUpdated` events to clients connected to the same
process. Running more than one replica requires swapping that provider for a
Redis-backed PubSub (e.g. `graphql-redis-subscriptions`) so a status update
published on one pod reaches subscribers connected to another; this is
called out explicitly in `pipeline-run.pubsub.ts`.

All durable state lives in Postgres (`pipeline_definition` and
`pipeline_run` tables), so a crashed or rescheduled pod loses nothing beyond
in-flight, unpublished subscription events. Failure isolation matters most
at the `dag-workflow-scheduler` boundary: `dag-workflow-scheduler-http-client.adapter.ts`
catches and logs HTTP failures rather than letting them corrupt run state,
and `pipeline-run.service.ts` marks a run `FAILED` (rather than leaving it
stuck `PENDING`) if the trigger call errors — a production hardening would
add idempotency keys on `POST /pipeline-runs` (already modelled as
`controlPlaneRunId` in the request body) so a retried trigger after a
timeout doesn't double-start a run on the scheduler side.

Scale-out is standard Kubernetes: run multiple `pipeline-api-gateway`
replicas behind a Service/Ingress, drive a Horizontal Pod Autoscaler off CPU
and/or GraphQL request latency, and size the Postgres connection pool
(TypeORM's default `pg` pool) per replica so `max_connections` on
`test-definition-postgres` isn't exhausted as replica count grows.

## Mid-run secret rotation across replicas

`secret-injection/secret-freshness-tracker.ts`'s `SecretFreshnessTracker` and
`mid-run-secret-rotation-scheduler.ts`'s `MidRunSecretRotationScheduler` are, like the GraphQL
`PubSub` caveat above, in-memory and per-process. In a multi-replica deployment, whichever
replica happens to resolve a given secret reference is the one that starts tracking its
rotation clock, and only that replica's periodic sweep will proactively re-fetch it — there is
no cross-replica coordination. This is an acceptable starting point because
`SecretProviderPort.resolveSecret` is safe to call redundantly (each replica independently
re-resolving the same reference from Vault/AWS is correct, just not deduplicated across
replicas), but a production hardening pass should either pin secret-rotation sweeping to a
single leader replica or move `SecretFreshnessTracker`'s state to a shared store, the same way
`internal/dispatch/idempotent_dispatch_deduplicator.go`'s `DispatchDeduplicationStore` in
`dag-workflow-scheduler` documents an in-memory-to-Redis migration path for the same class of
problem — see ADR
[0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md).

## Policy gate, approval workflow, and cost estimation failure isolation

Unlike the two in-memory caveats above, `policy-gate/` and `approval-workflow/` introduce no new
per-process state: `PolicyGateService` is stateless (every evaluation is a fresh HTTP round trip
to the OPA sidecar) and `ApprovalRequestEntity` is Postgres-backed like every other durable
record in this gateway, so both scale horizontally the same way the rest of the service does.
Two external-dependency failure modes are worth calling out instead:

- **OPA sidecar unavailable**: `OpaRegoPolicyEvaluatorAdapter` throws
  `ServiceUnavailableException` rather than failing open — a run trigger fails loudly (visible to
  the caller) instead of silently skipping policy evaluation. This is a deliberate fail-closed
  choice: skipping policy enforcement on infrastructure failure would be a worse outcome than a
  temporarily-unavailable "Run" button. See the NOTE in `opa-rego-policy-evaluator.adapter.ts` for
  what actually running that sidecar requires.
- **run-cost-estimator unavailable**: the opposite choice — `PipelineRunService.attachCostEstimate`
  catches and logs any `CostEstimatorClientPort` failure and lets the run proceed without a cost
  estimate attached. A missing dollar figure is a UX gap; blocking every run trigger on a
  best-effort budgeting service being reachable would not be a reasonable trade-off.

`ApprovalWorkflowModule`'s intentional lack of a `PipelineRunModule` dependency (see the file
header in `run-approval-decision.event.ts`) is now bridged with an event instead of a direct call
(see ADR [0012](../../../docs/architecture-decision-records/0012-priority-queue-dispatch-and-event-driven-approval-resume.md)):
`ApprovalWorkflowService.resolve` emits a `RunApprovalDecisionEvent` on NestJS's `EventEmitter2`
once a request is durably saved as `APPROVED`/`REJECTED`, and `PipelineRunModule`'s
`RunApprovalEventListener` reacts by calling `PipelineRunService.resumeHeldRun`/`failHeldRun`.
This keeps the module graph acyclic without needing a new piece of infrastructure (a Redpanda
topic, a Redis-backed pub/sub) — `EventEmitter2` is in-process and already bundled with Nest.

That in-process nature is this approach's own scaling caveat, and it's the same one
`pipeline-run.pubsub.ts`'s `PubSub` and `dag-workflow-scheduler`'s `WeightedFairShareQueue` both
already carry: `EventEmitter2.emit` is fire-and-forget from `ApprovalWorkflowService`'s point of
view (the emitting request does not await the listener), and if this `pipeline-api-gateway`
process crashes between the event firing and `RunApprovalEventListener`'s handler completing, the
gated run is left stuck in `AWAITING_APPROVAL` with no automatic retry — nothing re-publishes the
decision on restart, because nothing durable recorded that the event was ever emitted in the first
place (only that the `ApprovalRequestEntity` row itself reached `APPROVED`/`REJECTED`, which is
durable, but the *resume/fail side effect* is not). A production hardening pass would either
persist the pending decision so a reconciliation job can retry `resumeHeldRun`/`failHeldRun` for
any `ApprovalRequestEntity` whose status is terminal but whose `PipelineRunEntity` is still
`AWAITING_APPROVAL`, or publish the decision onto a durable bus (a Redpanda topic, consistent with
`ARCHITECTURE.md`'s general control-plane messaging posture) instead of `EventEmitter2` — neither
is implemented in this pass. This is also, deliberately, single-replica-safe only in the same
sense the rest of this section already documents: running more than one `pipeline-api-gateway`
replica means `EventEmitter2` events only reach listeners registered in the *same* process that
emitted them, which is fine here since both `ApprovalWorkflowService` and
`RunApprovalEventListener` live in the same Nest application instance regardless of replica count.
