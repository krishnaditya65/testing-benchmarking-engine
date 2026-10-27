# 0010. A new `distributed-systems-validation` service group, gated into the environment lifecycle

- Status: Accepted
- Date: 2026-08-11
- Owners: distributed-systems-validation maintainers, environment-lifecycle-manager maintainers

## Context

Feature-manifest section 5, "Distributed Systems & Infrastructure Validation," names four
capabilities with no owning code anywhere in the monorepo: **Data Consistency Benchmarking**
(testing CockroachDB/Cassandra/ScyllaDB under induced network partitions for ACID/eventual-
consistency guarantees), **Service Mesh Route Verification** (confirming Istio circuit breakers
and Linkerd retry budgets/fallbacks actually degrade gracefully under simulated load rather than
cascading), and **Infrastructure Drift Detection** (Kubernetes state and IAM permissions vs. an
expected baseline). None of these fit an existing service's job:

- `services/execution-fleet/chaos-ebpf-sidecar` *causes* network partitions and other kernel-
  level faults; it has no notion of what a "correct" read/write outcome looks like for a
  specific downstream database, nor any concept of a service mesh or IAM policy at all.
- `services/telemetry-lake/telemetry-query-api` reads pre-aggregated latency/error-rate
  telemetry; it has no read path for "did this specific replica return a stale value" or "does
  this IAM policy document differ from baseline," which requires driving new reads/writes and
  fetching live infra state, not querying already-ingested telemetry.
- `services/orchestration-control-plane/environment-lifecycle-manager` provisions and tracks the
  lifecycle of environments; it has no domain logic for interpreting a database's consistency
  claims or a mesh's resilience config, even though its environments are exactly what these
  checks need to run against.

These four capabilities also split cleanly along the same "workload profile decides the
language" axis `0002-split-execution-fleet-by-language-per-workload-profile.md` used for the
execution fleet: driving concurrent quorum writes and comparing replica-level commit tokens
across a partition is a throughput/concurrency-sensitive workload much closer to
`chaos-ebpf-sidecar`'s profile than to a control-plane CRUD service, while verifying a
`DestinationRule`/`ServiceProfile` config and diffing Kubernetes/IAM JSON/YAML documents is
config-shaped, iteration-speed-sensitive logic much closer to `environment-lifecycle-manager`'s.

Separately, nothing today stops a DAG pipeline run from starting against an environment whose
mesh resilience policy is misconfigured or whose IAM role has drifted to over-permissioned —
`EnvironmentLifecycleStateMachine` moves an environment straight from `READY` to `TEST_RUNNING`
with no checkpoint in between.

## Decision

1. **New top-level service group** `services/distributed-systems-validation/`, sibling to
   `execution-fleet`, `orchestration-control-plane`, and `telemetry-lake`, not nested inside any
   of them — its checks read live infrastructure/mesh/database state that none of the three
   existing layers own, so it does not belong under any of them per `ARCHITECTURE.md`'s "Three
   Layers" rationale.
2. **`consistency-verification-worker` (Go)** owns Data Consistency Benchmarking:
   `DatabaseConsistencyDriver` (with `CockroachDbDriver`/`CassandraDriver` implementations),
   `PartitionInducedReadVerifier` (coordinating with `chaos-ebpf-sidecar`'s partitions via the
   same `chaos.fault-injection-event.v1` Redpanda topic contract, never a direct call to that
   service), and `AcidPropertyAssertionSuite`. Go, for the same reason
   `protocol-load-worker-go` is Go: concurrent quorum-write benchmarking against a real database
   client benefits from Go's concurrency model and its ecosystem's `pgx`/`gocql` drivers, and it
   stays consistent with the control-plane services this worker is dispatched by.
3. **`service-mesh-policy-verifier` (TypeScript)** owns Service Mesh Route Verification and
   Infrastructure Drift Detection: `IstioCircuitBreakerVerifier`, `LinkerdFallbackResponseVerifier`,
   `KubernetesStateBaselineComparator`, and `IamPermissionDriftDetector`. TypeScript, because
   these checks are fundamentally config-diffing and API-response-interpreting logic —
   `@kubernetes/client-node` and `@aws-sdk/client-iam` calls, YAML/JSON structural comparison —
   the same "developer velocity over raw throughput" trade-off `ARCHITECTURE.md` already assigns
   to the control-plane side, not a throughput-sensitive workload needing Go/Rust.
4. **A new `POLICY_VERIFYING` state** in `environment-lifecycle-manager`'s
   `EnvironmentLifecycleStateMachine`, sitting between `READY` and `TEST_RUNNING`: every
   environment must pass through it, calling out through a new
   `src/provisioning/policy-verification-gate.port.ts` port to both new services, before a DAG
   pipeline run is allowed to start. A passing verdict transitions to `TEST_RUNNING`; a real
   failing verdict transitions to a new terminal `FAILED_POLICY_CHECK` state (distinct from
   `DESTROYED`, so "the environment is fine but failed policy" stays distinguishable from "the
   infrastructure itself broke" in transition history and metrics); a transport-level failure
   calling the gate follows the existing infra-error failure path to `DESTROYED`.

## Consequences

**Positive:**

- Every feature-manifest section 5 bullet now has owning code, structured the same way every
  other service group in this repo is: real domain logic behind ports, `README.md`/`SCALING.md`
  per service, stub adapters with NOTE comments documenting exactly what real wiring requires
  (matching `chaos-ebpf-sidecar`'s established pattern for privileged/external-dependency seams).
- Policy verification is now structurally impossible to skip — there is no `READY ->
  TEST_RUNNING` transition left in the state machine, so a pipeline cannot start against an
  environment whose mesh/consistency/drift posture was never checked.
- Each new service's language matches its workload the same way `0002` already established for
  the execution fleet, rather than defaulting both to TypeScript for monorepo-wide convention.
- `consistency-verification-worker` depends on `chaos-ebpf-sidecar` only through the shared
  `chaos.fault-injection-event.v1` topic contract, preserving `ARCHITECTURE.md`'s "no synchronous
  RPC that could cascade a failure backward" between chaos injection and validation.

**Negative / trade-offs:**

- `EphemeralEnvironmentProvisioner.beginTestExecution` is now asynchronous and can fail for a
  third reason (policy verification) beyond the two that already existed (unknown environment,
  infra error) — every caller must be updated to handle `PolicyVerificationFailedError`
  distinctly from a plain thrown error if it wants to surface "why" to an operator.
  `policy-verification-gate.port.ts` currently ships only a `NotWiredPolicyVerificationGateAdapter`
  stub that always throws; a real HTTP adapter to `service-mesh-policy-verifier` and
  `consistency-verification-worker` is required before `POLICY_VERIFYING` can pass in a real
  deployment, and this ADR does not specify that transport contract beyond "HTTP, since neither
  service currently exposes anything else" — a future ADR should fix that concretely once one of
  the two services grows an HTTP surface.
- `consistency-verification-worker`'s `CockroachDbDriver`/`CassandraDriver` and
  `service-mesh-policy-verifier`'s Kubernetes/IAM API clients are all stubs today (see each
  service's own README.md) — this ADR establishes where the real logic and interfaces live, not
  that the checks can run against a real cluster/database yet.
- One more language surface (this ADR adds no new language, but widens Go's footprint to a
  domain — distributed-database client libraries — the execution fleet's Go worker doesn't
  touch), and one more per-service toolchain for platform engineers already juggling Go, Rust,
  and TypeScript per `0002`.
