# pipeline-api-gateway

BFF + GraphQL server for the Orchestration Control Plane layer. Owns DAG
test definitions, run history, RBAC, and orchestrates (without itself
executing) DAG test runs by triggering the sibling `dag-workflow-scheduler`
service.

## Communication contracts (see ../../../ARCHITECTURE.md)

- **Control plane -> workers**: this gateway never talks to workers or the
  `dag.node.dispatch.v1` Redpanda topic directly. That topic is owned and
  published by `dag-workflow-scheduler` (a sibling Go service). This gateway
  is a thin REST client to that scheduler — see
  `src/pipeline-run/dag-workflow-scheduler-client.port.ts` (the hexagonal
  port) and `dag-workflow-scheduler-http-client.adapter.ts` (the real HTTP
  adapter, `POST /pipeline-runs`, `POST /pipeline-runs/:id/cancel`).
- **Control plane <-> frontend**: GraphQL over HTTP for CRUD/queries, and a
  GraphQL `Subscription` over `graphql-ws` for live run status. Both are
  served BY this gateway — see `src/test-definition/test-definition.resolver.ts`
  and `src/pipeline-run/pipeline-run.resolver.ts` (`pipelineRunStatusUpdated`).
- **Control plane -> telemetry-lake**: this gateway reads run telemetry
  aggregates from the sibling `telemetry-query-api` service to evaluate
  quality-gate SLAs — see `src/quality-gate/telemetry-query-api-client.port.ts`.

## Feature-manifest mapping

- **Automated quality gates** — `src/quality-gate/`. `quality-gate.evaluator.ts`
  is the pure pass/fail comparison logic (p99 latency, error rate, minimum
  throughput vs. configured thresholds); `quality-gate.service.ts` orchestrates
  fetching telemetry via `TelemetryQueryApiClientPort`, evaluating it, and
  calling into `PipelineRunService.markRunFailed(...)` to fail a run on SLA
  breach.
- **Secure secret injection (Vault / AWS Secrets Manager)** — `src/secret-injection/`,
  built hexagonally around `SecretProviderPort` (`resolveSecret(reference)`).
  `vault-secret-provider.adapter.ts` reads Vault's KV v2 endpoint
  (`X-Vault-Token` header, `data.data` unwrapping); `aws-secrets-manager-provider.adapter.ts`
  uses `@aws-sdk/client-secrets-manager`'s `GetSecretValueCommand`. The active
  adapter is selected at DI-registration time in `secret-injection.module.ts`
  via the `SECRET_PROVIDER_KIND` env var (`vault` | `aws-secrets-manager`).
  `secret-injection.service.ts` walks a pipeline node's `config` object and
  replaces `${{secret:<reference>}}` placeholders with resolved values.
  - **Mid-run secret rotation.** A secret resolved at pipeline-run start can expire hours into a
    long-running load test. `secret-freshness-tracker.ts`'s `SecretFreshnessTracker` records
    issued-at/predicted-expires-at per resolved secret reference (every resolution in
    `secret-injection.service.ts` now calls `recordIssued`), and
    `mid-run-secret-rotation-scheduler.ts`'s `MidRunSecretRotationScheduler` periodically
    re-fetches whichever references are due, pushing the refreshed value out via
    `RotationNotificationPort` — an interface only in this pass (see ADR
    [0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md)),
    backed today by a logging-only adapter until a real transport for execution-fleet workers to
    subscribe to is built.
- **Headless CLI & GitOps native** — this gateway's REST/GraphQL surface
  (mutations `createPipelineDefinition`/`updatePipelineDefinition`/
  `triggerPipelineRun`, queries `pipelineDefinition(s)`/`pipelineRun`) is the
  same surface the sibling `testforge-cli` tool (not built in this service)
  talks to for headless, scriptable, GitOps-driven pipeline management —
  there is no separate CLI-only API.
- **Auth / RBAC** — `src/auth/`. `jwt.strategy.ts` verifies bearer JWTs
  (signature, expiry, and issuer pinning) via passport-jwt; `rbac.guard.ts`
  is a real `CanActivate` that reads roles declared via the
  `@RequiredRoles(...)` decorator (`roles.decorator.ts`) and checks them
  against the authenticated user's `roles` claim, throwing
  `ForbiddenException` on mismatch. Applied to every resolver via
  `@UseGuards(AuthGuard('jwt'), RbacGuard)`.
- **DAG definition versioning & structural diffing** — `src/test-definition/`.
  `pipeline-definition-version-history.entity.ts` is an append-only snapshot
  table (`pipeline_definition_version_history`), populated by
  `test-definition.service.ts` on every `create`/`update` via
  `PipelineDefinitionDiffService.captureVersionSnapshot`.
  `pipeline-definition-diff.service.ts` computes the structural diff between
  any two captured versions (added/removed/modified nodes and edges — the
  "someone silently removed the consistency-assertion node" case) and is
  exposed over GraphQL as `pipelineDefinitionDiff(pipelineId, fromVersion,
  toVersion)` by `pipeline-definition-diff.resolver.ts`. The diff *report
  shape* (not the evaluation logic — see that service's file header) is
  imported from `@shared-libraries/dag-domain-model`'s
  `src/diffing/dag-structural-diff.types.ts`, shared with
  `dag-workflow-scheduler`'s independent Go implementation
  (`internal/diagnostics/dag_structural_diff_engine.go`).
- **Policy-as-code quality/security gate** — `src/policy-gate/`, built
  hexagonally around `PolicyEvaluationPort.evaluate(pipelineDefinition,
  policyBundle)`. `opa-rego-policy-evaluator.adapter.ts` calls an OPA
  sidecar's Data API (`POST /v1/data/testforge/policies/violations`); the
  loaded policy bundle lives in `example-policies/*.rego`
  (`no-chaos-injection-without-approval.rego`,
  `production-namespace-requires-teardown-node.rego`). `policy-gate.service.ts`
  is called by `pipeline-run.service.ts`'s `triggerRun` **before** the
  scheduler is ever contacted: a `BLOCKING` violation fails the run trigger
  outright; a `REQUIRES_APPROVAL` violation creates an `ApprovalRequest`
  instead (see below) rather than hard-failing.
- **Human-in-the-loop approval workflow** — `src/approval-workflow/`.
  `approval-request.entity.ts` is the durable `approval_request` record;
  `approval-workflow.service.ts` creates one whenever `PolicyGateService`
  encounters a `REQUIRES_APPROVAL` violation, and exposes `approve`/`reject`,
  surfaced as the `approvePipelineRun`/`rejectPipelineRun` GraphQL mutations
  in `approval-workflow.resolver.ts` (restricted to `pipeline-admin` — a
  stronger bar than whatever role triggered the run). The gated run is
  persisted as `PipelineRunStatus.AWAITING_APPROVAL` in the meantime.
  **Approving/rejecting now actually resumes/fails the held run** (see ADR
  [0012](../../../docs/architecture-decision-records/0012-priority-queue-dispatch-and-event-driven-approval-resume.md)):
  `ApprovalWorkflowService.resolve` emits a `RunApprovalDecisionEvent` on
  NestJS's `EventEmitter2` (`@nestjs/event-emitter`, registered globally via
  `EventEmitterModule.forRoot()` in `app.module.ts`) rather than calling
  `PipelineRunService` directly. `src/pipeline-run/run-approval-event.listener.ts`'s
  `RunApprovalEventListener` — a provider registered in `PipelineRunModule`,
  not `ApprovalWorkflowModule` — subscribes via `@OnEvent` and calls the new
  `PipelineRunService.resumeHeldRun`/`failHeldRun` methods.
  **Why an event instead of a direct service call**: `PipelineRunModule`
  already imports `PolicyGateModule`, which imports `ApprovalWorkflowModule`
  (so `PolicyGateService` can create an approval request instead of
  hard-failing a `REQUIRES_APPROVAL` trigger). Having `ApprovalWorkflowModule`
  import `PipelineRunModule` back to call `resumeHeldRun` directly would close
  that into a module cycle
  (`PipelineRunModule -> PolicyGateModule -> ApprovalWorkflowModule -> PipelineRunModule`),
  which Nest's module graph cannot resolve. Publishing an event keeps
  `ApprovalWorkflowModule` fully unaware `PipelineRunModule` exists — only a
  plain TypeScript import of the event class/name crosses that boundary, which
  carries no `@Module`-graph weight. See
  `src/approval-workflow/run-approval-decision.event.ts`'s file header for the
  full rationale, and `src/__tests__/run-approval-event.listener.test.ts` /
  `src/__tests__/approval-workflow.service.test.ts` for the unit tests
  covering both the emit and the listener side of this flow.
- **Pre-run cost/carbon estimation** — `src/cost-estimation/`, built
  hexagonally around `CostEstimatorClientPort.estimateRunCost`, matching the
  `TelemetryQueryApiClientPort` pattern. `cost-estimator-http-client.adapter.ts`
  calls the sibling `run-cost-estimator` service's `POST /cost-estimate`.
  `pipeline-run.service.ts`'s `triggerRun` calls this (best-effort — a
  cost-estimator outage never blocks a run) and persists the result on
  `PipelineRunEntity.estimatedCostUsd`/`estimatedCarbonKgCo2e`, so
  `workflow-canvas-frontend` can show projected cost before "Run" is
  clicked.

## Observability (dogfooding `shared-libraries/observability-kit`)

`src/main.ts` calls `initializeOpenTelemetryInstrumentation('pipeline-api-gateway')` before
`AppModule` is constructed, registers `correlationIdPropagationMiddleware` ahead of every
resolver/guard, and logs through `createStructuredLogger('pipeline-api-gateway', ...)` instead
of `console.log`. This gateway is the control plane's own BFF — it dogfoods the same
`@shared-libraries/observability-kit` package every other TypeScript service is expected to use,
rather than being the one service that doesn't. See ADR
[0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md)
and `services/shared-libraries/observability-kit/README.md`.

## DAG-shape validation

`src/test-definition/test-definition.service.ts` rejects, at write time, any
pipeline definition whose edges reference a non-existent node id, contains
duplicate node ids, self-loops, or a cycle — this is the structural
"quality gate" that keeps malformed DAGs from ever reaching
`dag-workflow-scheduler`.

## Shared domain types

`src/test-definition/dag-domain-types.ts` defines `PipelineNode`/`PipelineEdge`/
`PipelineStepType` locally rather than importing them from
`@shared-libraries/dag-domain-model` — that package's own `PipelineNode` union
(`kind`-discriminated) predates and differs field-for-field from this gateway's
`stepType`-based shape, and reconciling the two is out of scope here. This
gateway does, however, now depend on `@shared-libraries/dag-domain-model` for
the pieces that *are* shared: the DAG structural-diff report contract
(`src/diffing/dag-structural-diff.types.ts`, used by
`pipeline-definition-diff.service.ts`) and, transitively via
`run-cost-estimator`'s HTTP contract, its `LoadTestNode`/`PipelineDefinition`
shapes.

## Local development

```bash
npm install
npm run start:dev
```

Requires a reachable Postgres (`control_plane` database / `control_plane`
user, matching docker-compose's `test-definition-postgres` service) and,
for full functionality, reachable `dag-workflow-scheduler` and
`telemetry-query-api` instances. See `.env`-style variables referenced
throughout `src/**/*.module.ts` and `src/app.module.ts`.

### Why `strictPropertyInitialization` is off

`tsconfig.json` sets `"strict": true` but explicitly overrides
`"strictPropertyInitialization": false`. Every `*.entity.ts` in this service
(`pipeline-run.entity.ts`, `approval-request.entity.ts`,
`pipeline-definition.entity.ts`, `pipeline-definition-version-history.entity.ts`)
declares its `@Column()`-decorated properties without a constructor or
field initializer, by design — TypeORM populates them when hydrating a row
from the database, not at construction time. That's the standard TypeORM
entity idiom (and what TypeORM's own NestJS integration docs recommend), but
it's fundamentally incompatible with `strictPropertyInitialization`, which
has no way to know an ORM will fill the field in later. The alternative —
adding a `!:` definite-assignment assertion or a redundant constructor to
every column on every entity — is pure boilerplate that would obscure the
real nullability information already carried by TypeORM's `nullable: true`
and each field's own `?:` marker. This override keeps every other `strict`
check (including `strictNullChecks`) on.
