import { IstioCircuitBreakerVerifier, InMemoryKubernetesMeshApiClientAdapter } from './mesh/istio-circuit-breaker-verifier';
import {
  LinkerdFallbackResponseVerifier,
  InMemoryKubernetesLinkerdApiClientAdapter,
} from './mesh/linkerd-fallback-response-verifier';
import {
  KubernetesStateBaselineComparator,
  InMemoryKubernetesLiveStateApiClientAdapter,
  type KubernetesStateBaseline,
} from './drift/kubernetes-state-baseline-comparator';
import { IamPermissionDriftDetector, InMemoryIamApiClientAdapter } from './drift/iam-permission-drift-detector';

export interface ServiceMeshPolicyVerifier {
  readonly istioCircuitBreakerVerifier: IstioCircuitBreakerVerifier;
  readonly linkerdFallbackResponseVerifier: LinkerdFallbackResponseVerifier;
  readonly kubernetesStateBaselineComparator: KubernetesStateBaselineComparator;
  readonly iamPermissionDriftDetector: IamPermissionDriftDetector;
}

const EMPTY_KUBERNETES_STATE_BASELINE: KubernetesStateBaseline = {
  namespaces: [],
  rbacRoles: [],
  networkPolicies: [],
};

/**
 * Composition root: wires every mesh-verification and drift-detection class together against
 * their in-memory stub adapters. Performs no cluster/AWS I/O at module load time — matching
 * `environment-lifecycle-manager/src/index.ts`'s pattern, this must be explicitly called by a
 * real entry point (a POLICY_VERIFYING-state consumer in `environment-lifecycle-manager`, a CLI,
 * or an HTTP server bootstrap) which then swaps each `InMemory*Adapter` for a real one per the
 * NOTE comment on that adapter's port interface.
 */
export function createServiceMeshPolicyVerifier(): ServiceMeshPolicyVerifier {
  const istioCircuitBreakerVerifier = new IstioCircuitBreakerVerifier(new InMemoryKubernetesMeshApiClientAdapter());
  const linkerdFallbackResponseVerifier = new LinkerdFallbackResponseVerifier(
    new InMemoryKubernetesLinkerdApiClientAdapter(),
  );
  const kubernetesStateBaselineComparator = new KubernetesStateBaselineComparator(
    new InMemoryKubernetesLiveStateApiClientAdapter(EMPTY_KUBERNETES_STATE_BASELINE),
  );
  const iamPermissionDriftDetector = new IamPermissionDriftDetector(new InMemoryIamApiClientAdapter());

  return {
    istioCircuitBreakerVerifier,
    linkerdFallbackResponseVerifier,
    kubernetesStateBaselineComparator,
    iamPermissionDriftDetector,
  };
}

export * from './mesh/istio-circuit-breaker-verifier';
export * from './mesh/linkerd-fallback-response-verifier';
export * from './drift/kubernetes-state-baseline-comparator';
export * from './drift/iam-permission-drift-detector';
