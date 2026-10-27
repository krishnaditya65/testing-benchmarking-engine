/**
 * argo-rollouts-webhook-adapter.ts
 *
 * Real-shaped `RolloutControllerPort` adapter targeting Argo Rollouts' webhook-based promotion
 * API (an `AnalysisRun`/webhook step configured on the target `Rollout` resource to call out to
 * this service's decision, rather than Argo Rollouts' own built-in metric providers — this
 * service's `CanaryRolloutDecisionEngine` policy logic is the source of truth instead). Flagger
 * exposes an equivalent webhook contract (`confirm-rollout`/`rollback` hooks); swapping
 * providers means writing a sibling `flagger-webhook-adapter.ts` behind the same
 * `RolloutControllerPort`, not touching the decision engine.
 *
 * NOTE on what a real deployment needs that this stub does not have:
 *  - `argoRolloutsWebhookBaseUrl` pointing at the in-cluster Argo Rollouts webhook receiver
 *    (typically reached via the cluster's internal DNS, e.g.
 *    "http://argo-rollouts-webhook.argo-rollouts.svc.cluster.local").
 *  - A bearer token or mTLS client cert authorizing this service to call that webhook — in a
 *    real deployment this comes from the same secret-injection mechanism
 *    `pipeline-api-gateway`'s `SecretProviderPort` uses (Vault / AWS Secrets Manager), not a
 *    plain constructor argument; left as a config field here since this service doesn't yet own
 *    a secret-injection module of its own (same caveat noted in `contract-healing-service`'s
 *    `GitHubHealingPullRequestAdapter`).
 *  - Cluster RBAC: the identity calling this webhook needs permission scoped to `rollouts.argoproj.io`
 *    `Rollout` resources in the target namespace (typically a dedicated ServiceAccount + Role,
 *    provisioned by `environment-lifecycle-manager`/`infrastructure/kubernetes`, not by this
 *    service).
 */
import type { CanaryAuthorizationDecision } from "../domain/canary-authorization-decision.types";
import type { RolloutControlActionResult, RolloutControllerPort } from "./rollout-controller.port";

export interface ArgoRolloutsWebhookAdapterConfig {
  argoRolloutsWebhookBaseUrl: string;
  /** NOTE: see module doc comment — should come from secret injection in a real deployment. */
  webhookAuthToken: string;
  requestTimeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;

const VERDICT_TO_ARGO_ACTION: Record<CanaryAuthorizationDecision["verdict"], "promote" | "pause" | "abort"> = {
  APPROVE_FULL_ROLLOUT: "promote",
  HOLD_AT_CURRENT_WEIGHT: "pause",
  ROLLBACK: "abort",
};

const ARGO_ACTION_TO_RESULT: Record<"promote" | "pause" | "abort", RolloutControlActionResult["actionTaken"]> = {
  promote: "PROMOTED",
  pause: "PAUSED",
  abort: "ABORTED",
};

export class ArgoRolloutsWebhookAdapter implements RolloutControllerPort {
  constructor(private readonly config: ArgoRolloutsWebhookAdapterConfig) {}

  async applyDecision(
    decision: CanaryAuthorizationDecision,
    rolloutResourceName: string,
  ): Promise<RolloutControlActionResult> {
    const action = VERDICT_TO_ARGO_ACTION[decision.verdict];
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      this.config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
    );

    try {
      // Real Argo Rollouts webhook call, e.g.:
      //   POST {argoRolloutsWebhookBaseUrl}/api/v1/rollouts/{namespace}/{rolloutResourceName}/{action}
      //   Authorization: Bearer {webhookAuthToken}
      // (Flagger's equivalent is a POST to the canary's configured webhook URL with
      // `{ name, namespace, phase }` in the body.) Kept as a real fetch call shape below so the
      // wiring is unambiguous once a live `argoRolloutsWebhookBaseUrl`/token are available; only
      // the actual network dependency is what makes this a "stub" in this environment.
      const response = await fetch(
        `${this.config.argoRolloutsWebhookBaseUrl.replace(/\/$/, "")}/api/v1/rollouts/${rolloutResourceName}/${action}`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${this.config.webhookAuthToken}`,
          },
          body: JSON.stringify({
            reason: decision.rationale,
            trigger: decision.trigger,
            evaluatedAtIso: decision.evaluatedAtIso,
          }),
          signal: controller.signal,
        },
      ).catch((error: unknown) => {
        // No live Argo Rollouts endpoint is reachable in this environment — surface as a
        // structured failure result rather than an unhandled rejection, per
        // RolloutControlActionResult's contract ("callers should alert loudly on this").
        throw error instanceof Error ? error : new Error(String(error));
      });

      if (!response.ok) {
        return {
          actionTaken: ARGO_ACTION_TO_RESULT[action],
          rolloutResourceName,
          succeeded: false,
          errorMessage: `Argo Rollouts webhook returned ${response.status} ${response.statusText}`,
        };
      }

      return {
        actionTaken: ARGO_ACTION_TO_RESULT[action],
        rolloutResourceName,
        succeeded: true,
      };
    } catch (error) {
      return {
        actionTaken: ARGO_ACTION_TO_RESULT[action],
        rolloutResourceName,
        succeeded: false,
        errorMessage: error instanceof Error ? error.message : String(error),
      };
    } finally {
      clearTimeout(timeout);
    }
  }
}
