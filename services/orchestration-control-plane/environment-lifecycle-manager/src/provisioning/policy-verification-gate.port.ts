/**
 * Port for the POLICY_VERIFYING state's out-of-process checks. When an environment reaches
 * POLICY_VERIFYING (see ../domain/environment-lifecycle-state-machine.ts), the state machine
 * itself does not know how to run a mesh or consistency check — it only knows the transition is
 * legal. EphemeralEnvironmentProvisioner (ephemeral-environment-provisioner.ts) calls out through
 * this port to get a verdict, then drives the state machine to TEST_RUNNING or
 * FAILED_POLICY_CHECK based on the result.
 *
 * A real implementation calls two independently-deployed services in
 * services/distributed-systems-validation/, over whatever RPC contract this repo's other
 * cross-service calls use (this control plane has no synchronous-RPC precedent to a
 * TypeScript/Go pair other than HTTP, so a real adapter is expected to be a plain HTTP client —
 * unlike the Redpanda-topic-mediated calls between control plane and execution fleet, this is a
 * synchronous "give me a verdict before I transition state" call, which is exactly the kind of
 * interaction ARCHITECTURE.md's "no synchronous RPC that could cascade a failure backward" rule
 * is scoped to control-plane <-> execution-fleet, not to this in-control-plane policy gate):
 *
 *   - service-mesh-policy-verifier (TypeScript): POST the environment's Istio DestinationRule /
 *     Linkerd ServiceProfile configs plus a simulated-load fixture; read back
 *     CircuitBreakerVerificationResult / LinkerdFallbackVerificationResult, and separately a
 *     KubernetesDriftReport / IamDriftReport for the environment's namespace.
 *   - consistency-verification-worker (Go): trigger a scenario run (drive writes, wait for a
 *     chaos.fault-injection-event.v1 partition, read back) and read the resulting
 *     ConsistencyVerificationReport / AcidAssertionSuiteReport once it completes.
 *
 * Neither of those HTTP surfaces exists yet in this starter repo — both services currently
 * expose only their internal library classes (see each service's own README.md) — so this port
 * is the named, typed seam a real adapter fills in once those HTTP surfaces are added, without
 * EphemeralEnvironmentProvisioner needing to change.
 */

/** One named check this gate ran, independent of which underlying service produced it. */
export interface PolicyVerificationCheckResult {
  readonly checkName: string;
  readonly passed: boolean;
  /** Human-readable evidence, matching the `evidence`/`description` fields the underlying
   *  service-mesh-policy-verifier and consistency-verification-worker result types already
   *  return (e.g. IstioCircuitBreakerVerifier's `CircuitBreakerVerificationResult.evidence`,
   *  IamPermissionDriftDetector's `IamPermissionDrift.description`). */
  readonly evidence: string;
}

/** Everything POLICY_VERIFYING needs from the caller to know what to check and against what. */
export interface PolicyVerificationRequest {
  readonly environmentId: string;
  readonly pipelineRunId: string;
  /** Kubernetes namespace the environment was provisioned into, scoping mesh/drift checks. */
  readonly kubernetesNamespace: string;
}

/**
 * The aggregate outcome of every mesh, drift, and consistency check run for one environment.
 * `passed` is true only if every entry in `checks` passed — EphemeralEnvironmentProvisioner
 * reads only this field to decide TEST_RUNNING vs. FAILED_POLICY_CHECK; `checks` exists for
 * surfacing to operators/dashboards, not for the state-machine decision itself.
 */
export interface PolicyVerificationVerdict {
  readonly environmentId: string;
  readonly passed: boolean;
  readonly checks: readonly PolicyVerificationCheckResult[];
  readonly verifiedAt: Date;
}

export interface PolicyVerificationGatePort {
  /**
   * Runs every configured mesh/drift/consistency check for the environment named in request and
   * returns their aggregated verdict. Implementations should let a transport-level failure
   * (the underlying service unreachable, a timeout) propagate as a rejected promise rather than
   * synthesizing a passing or failing PolicyVerificationVerdict — EphemeralEnvironmentProvisioner
   * treats a thrown error as an infra-error failure (POLICY_VERIFYING -> DESTROYED), distinct
   * from a real failing verdict (POLICY_VERIFYING -> FAILED_POLICY_CHECK). Synthesizing a verdict
   * here on transport failure would hide that distinction from the state machine.
   */
  verifyEnvironmentPolicies(request: PolicyVerificationRequest): Promise<PolicyVerificationVerdict>;
}

/**
 * Default PolicyVerificationGatePort until a real HTTP adapter to
 * services/distributed-systems-validation exists (see this file's top doc comment). Always
 * rejects rather than synthesizing a passing or failing verdict — deliberately louder than a
 * no-op, the same reasoning chaos-ebpf-sidecar's `refusingPodKillBackend` documents: a caller
 * driving POLICY_VERIFYING against this adapter should see an explicit "not wired up" error (and
 * the environment routed to DESTROYED via EphemeralEnvironmentProvisioner's infra-error path),
 * not a silent, meaningless TEST_RUNNING transition.
 */
export class NotWiredPolicyVerificationGateAdapter implements PolicyVerificationGatePort {
  async verifyEnvironmentPolicies(request: PolicyVerificationRequest): Promise<PolicyVerificationVerdict> {
    throw new Error(
      `NotWiredPolicyVerificationGateAdapter: no real service-mesh-policy-verifier / ` +
        `consistency-verification-worker HTTP client configured (environment "${request.environmentId}") — ` +
        `see this port's doc comment for what real wiring requires.`,
    );
  }
}
