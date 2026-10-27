# Scaling notes

## Stateless, invocation-scoped

Every class in this service (`IstioCircuitBreakerVerifier`, `LinkerdFallbackResponseVerifier`,
`KubernetesStateBaselineComparator`, `IamPermissionDriftDetector`) is a pure function of its
inputs plus one read through its port adapter — none hold mutable state across calls. This means
horizontal scaling has none of `environment-lifecycle-manager`'s "concurrent operations against
the same environmentId" hazard: any number of instances can run any number of verification/drift
checks concurrently, including against the same target cluster or IAM role, with no coordination
needed. There is nothing to lock.

## Invocation model: on-demand from POLICY_VERIFYING, not a long-lived server

This service's primary caller is `environment-lifecycle-manager`'s `POLICY_VERIFYING` state (see
its `src/provisioning/policy-verification-gate.port.ts`): once an environment reaches `READY`,
the gate calls into this service's mesh verifiers and drift detectors, and the state machine
transitions to `TEST_RUNNING` or `FAILED_POLICY_CHECK` based on the aggregate result. That call
pattern — synchronous, one-shot, "check now and get an answer" — fits a request/response
invocation (an HTTP endpoint, or an in-process call if colocated in the same deployable) far
better than a queue-consuming worker; there is no notion of a "pending policy check" that needs
to survive this service's own restart, unlike `environment-lifecycle-manager`'s spot-instance
checkpoint state.

## Cost profile of the checks themselves

- **Mesh verification (`IstioCircuitBreakerVerifier`, `LinkerdFallbackResponseVerifier`) is
  CPU-bound and cheap** once the `SimulatedRequestOutcome[]`/`SimulatedRouteRequestOutcome[]`
  timeline is in hand — the ejection/retry-budget simulation is a single pass over that array,
  O(n) in the number of simulated requests. The expensive part in a real deployment is
  *generating* that simulated load against the actual mesh (driving real traffic through
  `execution-fleet` workers at the target service and recording real outcomes), which is
  properly `protocol-load-worker-go`'s job, not this service's — this service only interprets
  the resulting outcome timeline against the policy config.
- **Drift detection (`KubernetesStateBaselineComparator`, `IamPermissionDriftDetector`) is
  dominated by the live-state fetch**, not the diff itself (also O(n) in resource count via
  the keyed-map comparison in `diffKeyedResources`/`expandStatementsToGrants`). A real
  `@kubernetes/client-node` "list every namespace/role/networkpolicy" call or `@aws-sdk/client-iam`
  policy fetch is the actual latency/rate-limit concern; this service should apply the same
  API-server-friendly pagination/backoff conventions any Kubernetes/AWS API client needs,
  independent of anything specific to this codebase.

## Failure mode: a check that cannot complete must not silently pass

Every port adapter's stub (`InMemoryKubernetesMeshApiClientAdapter.fetchIstioDestinationRule`,
etc.) throws rather than returning an empty/default config when asked for something it was never
seeded with — deliberately, so a real adapter that fails to reach the cluster/AWS API (network
partition, expired credentials, RBAC denial) propagates that failure to the caller instead of
this service ever reporting "no drift found" or "circuit breaker verified" when it actually
couldn't check anything. A `POLICY_VERIFYING` gate consuming this service must treat a thrown
error the same as a failed check (route to `FAILED_POLICY_CHECK`), not as an ambiguous "skip
this check" signal — an environment that could not be verified is not a verified environment.
