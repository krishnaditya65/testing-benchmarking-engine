# canary-rollout-authorizer

Implements the authorization half of the feature-manifest bullet **"A/B & Canary Differential
Benchmarking"** (section 7, AI-Assisted QA & Analytics): `telemetry-lake/telemetry-query-api`'s
`AbCanaryDifferentialBenchmarkComparator` only *reports* whether a canary looks statistically
regressed relative to control. This service consumes that report and actually **gates the
rollout** — approving, holding, or rolling back a live canary deployment via Argo
Rollouts/Flagger — rather than leaving a human to read a dashboard and act manually.

See `docs/architecture-decision-records/0009-ai-assisted-qa-service-group.md` for why this lives
under `services/ai-assisted-qa/` rather than folded into `telemetry-lake` or
`orchestration-control-plane`.

## Pipeline

```
telemetry-query-api's AbCanaryDifferentialBenchmarkComparator
        │  CanaryComparisonResult (p50/p99/errorRate deltas, overallVerdict)
        ▼  CanaryDifferentialReportClientPort (authorization/canary-differential-report-client.port.ts)
        │  + CanaryDifferentialReportHttpClientAdapter (…-http-client.adapter.ts)
        ▼
CanaryRolloutDecisionEngine        (authorization/canary-rollout-decision-engine.ts)
        │  CanaryAuthorizationDecision (APPROVE_FULL_ROLLOUT | HOLD_AT_CURRENT_WEIGHT | ROLLBACK)
        ▼  RolloutControllerPort (rollout-integration/rollout-controller.port.ts)
        │  + ArgoRolloutsWebhookAdapter (argo-rollouts-webhook-adapter.ts)
        ▼
   Argo Rollouts / Flagger webhook — promotes / pauses / aborts the live canary
```

## Why this service reads `ab-canary-differential-benchmark-comparator.ts` first

`src/authorization/canary-differential-report-client.port.ts` defines `CanaryComparisonRequest`/
`CanaryComparisonResult`/`CanaryMetricDelta` as field-for-field copies of
`telemetry-lake/telemetry-query-api/src/analytics/ab-canary-differential-benchmark-comparator.ts`'s
same-named types. These are necessarily a *separate* declaration, not an import — the two
services are independently deployable processes talking over HTTP, unlike
`@shared-libraries/dag-domain-model`, which multiple services link into the same process
boundary. If `telemetry-query-api`'s comparator output shape changes, this port's types need
hand-updating to match (see that file's doc comment for the full trade-off discussion).

## CanaryRolloutDecisionEngine

Pure, stateless, deterministic (see `src/authorization/canary-rollout-decision-engine.ts`).
Default policy (`DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS`):

| Signal | Threshold | On breach |
|---|---|---|
| p99 latency relative delta | `> +10%` | ROLLBACK |
| p50 latency relative delta | `> +20%` | ROLLBACK |
| Error rate **absolute** delta (percentage points) | `> +2pp` | ROLLBACK |
| N consecutive clean windows | `>= 3` | APPROVE_FULL_ROLLOUT |
| Clean, but streak not long enough | — | HOLD_AT_CURRENT_WEIGHT |

Error rate deliberately uses an **absolute percentage-point delta**, not a relative-fraction
delta like p50/p99 — a service going from a 0.1% to 0.2% error rate is a 100% relative increase
but operationally near-noise, while the same 0.1pp absolute jump on a service already at 5%
baseline error rate is comparatively unremarkable. Percentage points is the metric that actually
tracks "how many more requests are failing in absolute terms," which is what this policy cares
about. See the file's top comment for the full reasoning and `pickTrigger`'s multi-breach
priority order (p99 > error rate > p50).

Consecutive-clean-window tracking is NOT internal state on the class — callers pass
`priorConsecutiveCleanWindowCount` in and read the new count back out of the returned
`CanaryAuthorizationDecision`, the same way `dag-workflow-scheduler` keeps workflow state in
Temporal rather than in an in-process object (see ADR 0001), so this engine stays trivially
horizontally scalable and restart-safe.

## RolloutControllerPort / ArgoRolloutsWebhookAdapter

Hexagonal port (`rollout-integration/rollout-controller.port.ts`), same shape as
`contract-healing-service`'s `SourceControlPort`: the decision engine's output maps to exactly
one of `promote` (APPROVE_FULL_ROLLOUT) / `pause` (HOLD_AT_CURRENT_WEIGHT) / `abort` (ROLLBACK)
against whatever cluster-native controller is actually managing canary traffic weighting.
`ArgoRolloutsWebhookAdapter` is real-shaped (correct webhook path convention, auth header, JSON
body) but has no live cluster to call in this environment — see its NOTE comment block for
exactly what a real deployment additionally needs: a reachable in-cluster webhook URL, a
bearer token/mTLS cert (sourced from the same secret-injection mechanism as
`pipeline-api-gateway`'s `SecretProviderPort`, not a plain constructor arg), and RBAC scoped to
`rollouts.argoproj.io` `Rollout` resources in the target namespace. Swapping in Flagger instead
of Argo Rollouts means writing a sibling adapter behind the same port, not touching the decision
engine.

## Running locally

```bash
npm install
npm test    # vitest run — decision engine, all three verdict branches + edge cases
npm run build
```
