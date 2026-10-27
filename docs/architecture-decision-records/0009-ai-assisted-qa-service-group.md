# 0009. New top-level `services/ai-assisted-qa/` service group

- Status: Accepted
- Date: 2026-08-11
- Owners: ai-assisted-qa maintainers, orchestration-control-plane team (integration boundary),
  telemetry-lake team (integration boundary)

## Context

The feature manifest's section 7, "AI-Assisted QA & Analytics", covers three distinct
capabilities: **Autonomous Contract Healing** (detect backend schema drift, heal broken JSONPath
data-extraction expressions in pipeline definitions), **Log-to-Test Generation** (mine production
APM/session telemetry into candidate DAG workflows), and **A/B & Canary Differential
Benchmarking authorization** (turn `telemetry-query-api`'s statistical canary comparison into a
binding rollout gate). Before this ADR, none of these had owning code anywhere in the monorepo.

The obvious alternative to a new top-level group was folding all three into existing services —
specifically `orchestration-control-plane/pipeline-api-gateway` (since all three ultimately
produce or gate `PipelineDefinition`s that gateway owns) or `telemetry-lake/telemetry-query-api`
(since two of the three, log mining and canary authorization, consume telemetry that service
already reads). Both were rejected for the same reasons `ARCHITECTURE.md`'s "Why services are
split this way" section already gives for the three existing layers:

- **Different failure domain and blast radius.** `pipeline-api-gateway` is on the write path for
  every pipeline definition and run in the platform — an RBAC-guarded, latency-sensitive
  GraphQL/WebSocket surface real users hit interactively. `contract-healing-service`'s schema
  diffing and `log-to-test-generator-service`'s log mining are both batch/offline analysis jobs
  with no request-latency requirement and no business writing to them directly. Coupling their
  deploy lifecycle to the gateway's means a bug in, say, the Levenshtein-distance rename
  heuristic can block or destabilize a deploy of core pipeline CRUD — exactly the kind of
  cross-failure-domain coupling ARCHITECTURE.md's "Independent failure domains" principle exists
  to prevent.
- **`canary-rollout-authorizer` is an authority boundary, not a read boundary.** Folding it into
  `telemetry-query-api` — a service whose whole reason for existing is "read-optimized query
  path... that dashboards and quality gates read from" (ARCHITECTURE.md) — would mean the one
  service the rest of the platform trusts as a read-only reporting layer also holds live
  cluster-mutating credentials (the Argo Rollouts/Flagger webhook token). That is a materially
  larger and more dangerous credential/blast-radius profile than "answer a percentile-latency
  query", and deserves its own deploy/RBAC boundary rather than inheriting telemetry-query-api's.
- **Independent, faster iteration.** All three capabilities are genuinely new, likely to change
  shape quickly (new schema-diff heuristics, new pattern-mining strategies, new rollout-policy
  tuning) as they're used in practice — bundling that churn into two services the rest of the
  platform depends on for stable, foundational behavior (pipeline CRUD, telemetry reads) would
  force those services' release cadence to absorb AI-assisted-QA churn they have no stake in.
- **They share a theme, not a runtime dependency graph.** The three services under
  `services/ai-assisted-qa/` do not call each other and share no runtime state; what they share
  is that all three consume outputs from other layers (schema specs, telemetry, comparison
  reports) and produce *advisory or gated actions* back into those layers, rather than owning
  primary state themselves. That's a distinct architectural role — "AI-assisted decision layer
  sitting across the platform" — that doesn't map onto any single existing layer in
  ARCHITECTURE.md's three-layer model (Orchestration Control Plane / Execution Fleet / Telemetry
  Lake), which is itself the same justification `shared-libraries/` already uses for being a
  fourth, cross-cutting grouping rather than being split across the three layers it serves.

## Decision

Introduce `services/ai-assisted-qa/` as a fourth top-level service group, alongside
`orchestration-control-plane/`, `execution-fleet/`, and `telemetry-lake/` (with
`shared-libraries/` remaining the separate cross-cutting group it already is). It hosts three
independently deployable TypeScript services:

- `contract-healing-service` — Autonomous Contract Healing. Reads OpenAPI spec versions,
  produces `SchemaDiff`s, and heals/proposes fixes to pipeline `DynamicVariableExtraction`
  JSONPath expressions via a `SourceControlPort` (drafts a PR; only auto-applies above a
  documented high-confidence threshold — see that service's README).
- `log-to-test-generator-service` — Log-to-Test Generation. Reads APM/session telemetry (modeled
  as a ClickHouse-backed port, same physical store as `telemetry-query-api`) and mines it into
  candidate `PipelineDefinition`s via `@shared-libraries/dag-domain-model`.
- `canary-rollout-authorizer` — A/B & Canary Differential Benchmarking authorization. Reads
  `telemetry-query-api`'s `AbCanaryDifferentialBenchmarkComparator` reports over HTTP and gates a
  live canary rollout via a `RolloutControllerPort` (Argo Rollouts/Flagger webhook adapter).

Each service is hexagonal (ports/adapters) and integrates with existing services purely as a
**client** — over HTTP to `telemetry-query-api`, by importing `@shared-libraries/dag-domain-model`
for DAG shapes, and (for contract healing) by drafting pull requests against the repo
`pipeline-api-gateway`'s pipeline definitions live in. None of the three services gain write
access to `pipeline-api-gateway`'s Postgres store or telemetry-lake's ClickHouse store directly;
all mutation happens through those services' own owned surfaces (PR review, or
`pipeline-api-gateway`'s GraphQL mutations, for a caller wiring this service group's output back
in) or through cluster-native controllers (Argo Rollouts) that were never owned by any existing
service to begin with.

## Consequences

**Positive:**

- Each service in the group can be deployed, scaled, and iterated on independently of
  `pipeline-api-gateway` and `telemetry-query-api`, matching those services' own "Independent
  scaling"/"Independent failure domains" rationale in ARCHITECTURE.md.
- `canary-rollout-authorizer`'s cluster-mutating credentials (Argo Rollouts webhook token) are
  isolated to a service whose only job is gating rollouts, not bundled into a read-optimized
  telemetry API's credential surface.
- The naming convention stays legible: `<domain>-<role>` per ARCHITECTURE.md ("Services are
  named `<domain>-<role>`... so the name alone tells a new engineer what it owns") —
  `contract-healing-service`, `log-to-test-generator-service`, `canary-rollout-authorizer` each
  say exactly what they do without needing this ADR to explain it.

**Negative / trade-offs:**

- Three more independently deployable services means three more sets of CI pipelines,
  containers, and on-call surface area than folding this logic into existing services would have
  required — a real operational cost, accepted here because the failure-domain isolation this
  buys back is worth more than the marginal deploy-topology savings, consistent with the same
  trade-off ADR 0002 made for the execution fleet.
- `canary-rollout-authorizer`'s `CanaryComparisonRequest`/`CanaryComparisonResult` types are a
  hand-maintained copy of `telemetry-query-api`'s comparator types rather than a shared import
  (see `canary-differential-report-client.port.ts`'s doc comment) — an HTTP service boundary
  necessarily gives up the "shared types are imported, never copy-pasted" guarantee that
  `@shared-libraries/dag-domain-model` provides for same-process consumers. This is a known,
  documented drift risk, not an oversight; keeping `telemetry-query-api`'s comparator output
  shape stable (or bumping this port's types in lockstep on change) is an explicit maintenance
  obligation for both teams going forward.
- `log-to-test-generator-service` and `contract-healing-service` produce *candidates* only —
  neither writes directly to `pipeline-api-gateway`'s store. This is intentional (keeps
  `DagStructuralValidator` as the single authoritative save-time gate, per that package's own
  README), but it means a human or a separate orchestration script must exist to actually wire
  generated/healed pipeline definitions back into the gateway; that wiring is explicitly out of
  scope for this service group and not yet built anywhere.
