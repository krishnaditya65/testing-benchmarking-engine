/**
 * rollout-controller.port.ts
 *
 * Hexagonal port through which `canary-rollout-decision-engine.ts`'s output is turned into a
 * real action against whatever actually controls canary traffic weighting in the cluster (Argo
 * Rollouts, Flagger, or a custom controller). Kept separate from the decision engine itself so
 * the engine stays pure/testable and the "make this real" boundary is exactly one adapter file
 * (`argo-rollouts-webhook-adapter.ts`) — same separation as `SourceControlPort` in the sibling
 * `contract-healing-service`.
 */
import type { CanaryAuthorizationDecision } from "../domain/canary-authorization-decision.types";

export interface RolloutControlActionResult {
  actionTaken: "PROMOTED" | "ABORTED" | "PAUSED" | "NO_OP";
  /** The controller's own identifier for the rollout resource acted on, e.g.
   * "checkout-service-rollout" (an Argo Rollouts `Rollout` object name). */
  rolloutResourceName: string;
  /** True if the underlying webhook/API call succeeded; `false` means the decision was computed
   * but the cluster-side action failed and needs operator attention — callers should alert
   * loudly on this rather than silently swallowing it, since a failed ROLLBACK action left
   * un-retried is the worst-case outcome this whole service exists to prevent. */
  succeeded: boolean;
  /** Present when `succeeded` is false. */
  errorMessage?: string;
}

/**
 * RolloutControllerPort
 *
 * `applyDecision` maps a `CanaryAuthorizationDecision.verdict` to a concrete controller call:
 *  - APPROVE_FULL_ROLLOUT -> promote (shift 100% of traffic to the canary / retire the old
 *    control version).
 *  - HOLD_AT_CURRENT_WEIGHT -> pause (freeze the current canary traffic percentage, take no
 *    further action this evaluation cycle).
 *  - ROLLBACK -> abort (immediately shift traffic back to the control version).
 */
export interface RolloutControllerPort {
  applyDecision(decision: CanaryAuthorizationDecision, rolloutResourceName: string): Promise<RolloutControlActionResult>;
}
