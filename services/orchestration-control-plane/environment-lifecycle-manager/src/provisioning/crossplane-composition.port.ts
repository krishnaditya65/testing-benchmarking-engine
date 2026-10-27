/**
 * Port for provisioning ephemeral test environments backed by Crossplane
 * compositions running on a Kubernetes control plane. Adapters (see
 * crossplane-kubectl-execution.adapter.ts) implement this against a
 * concrete apply/delete mechanism (kubectl, a k8s client library, ...).
 */

export interface CrossplaneCompositionConfig {
  readonly claimName: string;
  readonly compositionRef: string;
  readonly namespace: string;
  readonly parameters: Record<string, unknown>;
}

export interface CrossplaneApplyResult {
  readonly claimName: string;
  readonly outputs: Record<string, string>;
  readonly applyDurationMs: number;
}

export interface CrossplaneCompositionPort {
  applyComposition(compositionConfig: CrossplaneCompositionConfig): Promise<CrossplaneApplyResult>;
  deleteComposition(claimName: string): Promise<void>;
}
