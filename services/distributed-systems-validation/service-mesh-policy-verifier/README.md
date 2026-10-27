# service-mesh-policy-verifier

Distributed Systems & Infrastructure Validation service that verifies service-mesh resilience
policies (Istio circuit breakers, Linkerd retry budgets/fallbacks) actually hold under simulated
failure, and detects drift between live Kubernetes/IAM state and an expected baseline.

## Feature manifest mapping

**"Service Mesh Route Verification"** (feature-manifest section 5, "Distributed Systems &
Infrastructure Validation")

- `src/mesh/istio-circuit-breaker-verifier.ts` — `IstioCircuitBreakerVerifier`. Given a target
  service's real `DestinationRule.spec.trafficPolicy.outlierDetection` config
  (`consecutiveErrors`, `interval`, `baseEjectionTime`, `maxEjectionPercent`), replays a
  simulated request timeline through the same ejection rule Istio's Envoy sidecars run and
  reports `circuitTripped` (did any endpoint actually get ejected) and `degradedGracefully` (did
  the ejected fraction stay within `maxEjectionPercent` without ever ejecting the whole pool).
  `KubernetesMeshApiClientPort` is the seam to the live cluster; `InMemoryKubernetesMeshApiClientAdapter`
  is its local-dev/test stub — see the port's doc comment for the exact
  `@kubernetes/client-node` `CustomObjectsApi` call real wiring needs.
- `src/mesh/linkerd-fallback-response-verifier.ts` — `LinkerdFallbackResponseVerifier`, the
  analogous check for Linkerd, whose resilience model is a retry budget
  (`ServiceProfile.spec.retryBudget`) plus a `TrafficSplit` fallback rather than an
  outlier-detection circuit breaker. Verifies the retry budget actually throttles (no unbounded
  retry storm against a failing primary) and that a configured fallback backend absorbs traffic
  during primary failures.

**"Infrastructure Drift Detection"**

- `src/drift/kubernetes-state-baseline-comparator.ts` — `KubernetesStateBaselineComparator`.
  Real structural diff (not a stub) between an expected baseline manifest — authored as YAML via
  `parseBaselineManifestYaml` — and live cluster state for namespaces, RBAC roles, and
  NetworkPolicies. Reports `ADDED`/`REMOVED`/`MODIFIED` per resource, with `MODIFIED` including
  exact `fieldDiffs`.
- `src/drift/iam-permission-drift-detector.ts` — `IamPermissionDriftDetector`. Compares a
  live-fetched IAM policy document against an expected baseline (both AWS-style `{ Version,
  Statement[] }` JSON), expanding each statement's `actions × resources` into individual grants
  before diffing so a single-action addition inside an otherwise-unchanged statement is never
  missed. **Added `Allow` permissions are flagged `HIGH` severity** (`hasSecurityRelevantAddition`);
  removed permissions and added `Deny` statements are `INFORMATIONAL` — see the file's top
  doc comment for why the two drift directions are treated asymmetrically.

## Not yet wired to a live cluster/AWS account

Every `*ApiClientPort` above ships only an `InMemory*Adapter` stub — this is a starter/offline
repository. Each port's doc comment shows the exact `@kubernetes/client-node` or
`@aws-sdk/client-iam` calls a real adapter needs; the comparison/verification logic itself
(`compare`, `detect`, `verifyAgainstConfig`) is real and fully unit-tested against those stubs,
so wiring a real adapter requires no change to that logic — only implementing the port.

## Layout

```
src/
  mesh/
    istio-circuit-breaker-verifier.ts
    linkerd-fallback-response-verifier.ts
  drift/
    kubernetes-state-baseline-comparator.ts
    iam-permission-drift-detector.ts
  __tests__/
    kubernetes-state-baseline-comparator.test.ts
    iam-permission-drift-detector.test.ts
  index.ts   # composition root: createServiceMeshPolicyVerifier()
```

## Development

```
npm install
npm run build   # tsc
npm run dev      # tsx watch src/index.ts
npm run lint
npm test         # jest
```

`src/index.ts` exports `createServiceMeshPolicyVerifier()` rather than running anything at
import time, mirroring `environment-lifecycle-manager/src/index.ts`'s composition-root pattern —
call it from a real entry point (the `POLICY_VERIFYING`-state consumer in
`environment-lifecycle-manager`, a CLI, or an HTTP bootstrap) once real adapters are wired.
