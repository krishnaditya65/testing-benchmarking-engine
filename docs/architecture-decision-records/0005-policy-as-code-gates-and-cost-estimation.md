# 0005. Policy-as-code quality/security gates, human-in-the-loop approval, and pre-run cost estimation

- Status: Accepted
- Date: 2025-03-18
- Owners: pipeline-api-gateway maintainers, orchestration-control-plane team

## Context

`pipeline-api-gateway` already enforces two kinds of gates: structural DAG-shape validation at
save time (`TestDefinitionService.assertDagShapeIsValid`, cycle/dangling-reference checks) and
automated SLA quality gates *after* a run completes (`quality-gate/`, failing a run on p99/error-rate/
throughput breach against telemetry). Neither addresses two classes of problem that surfaced as
usage grew:

**Un-reviewable risky runs.** Some pipeline definitions are structurally valid but operationally
dangerous to run unattended — a `CHAOS_INJECTION` node targeting a selector outside a pre-approved
chaos-safe allowlist can kill processes, starve CPU, or throttle disk I/O against real
infrastructure (`chaos-ebpf-sidecar`); a pipeline that provisions production-namespace resources
via `environment-lifecycle-manager` but omits an `ENVIRONMENT_TEARDOWN` node leaves those
resources orphaned. Both are policy violations knowable *before* the run starts, from the DAG
definition and run context alone — but hand-coding this class of rule directly into
`pipeline-run.service.ts` as ad hoc `if` statements does not scale as the rule set grows, mixes
policy concerns into run-orchestration code, and gives platform/security teams no way to change or
audit a rule without a code change and redeploy of the gateway itself.

**No visibility into run cost before committing to it.** Nothing in the platform today tells a
pipeline author what a run will cost — in dollars, or in rough environmental impact — before they
click "Run." A pipeline author authoring a 50,000-VU, 4-hour load test has no signal, before
triggering it, that it will consume meaningfully more execution-fleet capacity (and cloud spend)
than a 500-VU, 4-minute smoke test. `workflow-canvas-frontend`'s canvas is the natural place to
surface that, but nothing computes it.

## Decision

**Policy-as-code**, evaluated by an external OPA (Open Policy Agent) sidecar rather than
hand-coded TypeScript rules:

- `src/policy-gate/policy-evaluation.port.ts` defines `PolicyEvaluationPort.evaluate(pipelineDefinition,
  policyBundle)`, matching this gateway's existing hexagonal port/adapter pattern
  (`SecretProviderPort`, `TelemetryQueryApiClientPort`).
- `opa-rego-policy-evaluator.adapter.ts` implements it against a real OPA sidecar's Data API
  (`POST /v1/data/testforge/policies/violations`); the policy bundle itself is authored as Rego
  (`example-policies/no-chaos-injection-without-approval.rego`,
  `example-policies/production-namespace-requires-teardown-node.rego`), not TypeScript — so
  platform/security teams can add, change, or review policy rules independent of a gateway
  deploy, and evaluate them consistently regardless of which service calls OPA.
- `policy-gate.service.ts`'s `PolicyGateService.evaluateBeforeRun` is called by
  `pipeline-run.service.ts`'s `triggerRun` as the *first* step, before the run is ever offered to
  `dag-workflow-scheduler` — mirroring where structural DAG-shape validation already runs (at the
  earliest point malformed/risky input can be rejected), not where quality-gate runs (after the
  fact).

**Human-in-the-loop approval**, as the resolution path for policy violations that shouldn't be a
hard block but also shouldn't run unattended:

- Every policy violation carries a `PolicyViolationSeverity` (`BLOCKING`, `REQUIRES_APPROVAL`,
  `WARNING`). `BLOCKING` fails the run trigger outright (`ForbiddenException`, run persisted
  `FAILED`); `WARNING` is logged only; `REQUIRES_APPROVAL` is the new middle path.
- `src/approval-workflow/`'s `ApprovalWorkflowService.createApprovalRequest` is called by
  `PolicyGateService` instead of hard-failing when a violation is `REQUIRES_APPROVAL` severity —
  the run is persisted `PipelineRunStatus.AWAITING_APPROVAL` and an `ApprovalRequestEntity` is
  created, with `approvePipelineRun`/`rejectPipelineRun` GraphQL mutations
  (`approval-workflow.resolver.ts`) restricted to `pipeline-admin`, deliberately a stronger bar
  than whatever role was allowed to trigger the run in the first place.

**Pre-run cost/carbon estimation**, as a new, separately-scaled service rather than logic embedded
in the gateway:

- A new `run-cost-estimator` TypeScript service (`services/orchestration-control-plane/run-cost-estimator/`)
  computes `LoadTestNode` worker-hours x static cloud-instance pricing (`cloud-instance-pricing-catalog.ts`)
  for a dollar estimate, and a rough CO2e estimate (`carbon-footprint-estimator.ts`) from the same
  instance-hours, exposed as `estimateCost(pipelineDefinition)` and over `POST /cost-estimate`.
- `pipeline-api-gateway`'s `src/cost-estimation/cost-estimator-client.port.ts` +
  `cost-estimator-http-client.adapter.ts` is the hexagonal port/adapter pair to it, matching
  `telemetry-query-api-client.port.ts`'s shape exactly. `pipeline-run.service.ts` calls it
  best-effort (failures are logged, never block a run) and persists the result on
  `PipelineRunEntity` so the canvas can show projected cost before "Run" is clicked.
- Kept as its own service (not a module inside `pipeline-api-gateway`) because it is a pure,
  stateless computation with a fundamentally different scaling profile (CPU-only, no database, no
  I/O beyond the request itself) — bundling it into the gateway would coscale an
  effectively-free-to-run computation with a stateful, database-backed service for no benefit,
  matching `ARCHITECTURE.md`'s "independent scaling" principle for the same reason
  `execution-fleet` is split from `orchestration-control-plane` at all.

## Consequences

**Positive:**

- Policy rules live as data (Rego), not code — a security team can add a new gate without a
  TypeScript change, code review of gateway internals, or a gateway redeploy (beyond
  reloading/redeploying the OPA sidecar's bundle).
- The `BLOCKING`/`REQUIRES_APPROVAL`/`WARNING` severity model gives policy authors a proportional
  response instead of a binary allow/deny, which is what makes chaos-injection governance usable
  in practice — an outright block would just get bypassed by pipeline authors routing around the
  gate; a silent warning would be ignored; approval-gating is the middle path that actually works.
- `run-cost-estimator`'s complete isolation from `pipeline-api-gateway` means its failure modes
  (and its independent scaling/deploy cadence) never risk destabilizing run-triggering — the
  "runs must never be blocked by a budgeting nice-to-have" design goal is structurally enforced,
  not just documented.

**Negative / trade-offs:**

- Adds an operational dependency: an OPA sidecar must be running and loaded with the
  `example-policies/*.rego` bundle before policy evaluation can succeed at all — this repo does
  not (yet) wire that sidecar into `infrastructure/kubernetes/base/control-plane-services-deployment.yaml`
  or the `pipeline-api-gateway` Helm chart; `OpaRegoPolicyEvaluatorAdapter`'s NOTE comment calls
  this out explicitly. Until that's wired, policy evaluation fails closed
  (`ServiceUnavailableException`), which blocks every run trigger, not just risky ones — a
  deliberate fail-closed choice (see `SCALING.md`), but one that raises the operational bar for
  running this gateway at all.
- `ApprovalWorkflowModule` is intentionally decoupled from `PipelineRunModule` to avoid a module
  import cycle (`PipelineRunModule` -> `PolicyGateModule` -> `ApprovalWorkflowModule`); the
  consequence is that approving a request today does not automatically resume the gated run — a
  human or a follow-up integration still has to re-trigger it. This is a real, currently-open gap
  documented in `approval-workflow.service.ts` and `SCALING.md`, not a hidden one.
- Cost/carbon estimates are approximations by design (static pricing table, global-average grid
  carbon intensity, no region-awareness) — good enough for a budgeting signal, explicitly not
  good enough to reconcile against an actual cloud bill or for sustainability reporting. Both
  `run-cost-estimator` files carry NOTE comments to this effect so the approximation is never
  mistaken for a certified figure downstream.
- Two more services in the request path of triggering a run (OPA sidecar, `run-cost-estimator`)
  means more moving parts to operate, even though one of them (cost estimation) is explicitly
  non-blocking and the other (policy) is intentionally blocking by design.
