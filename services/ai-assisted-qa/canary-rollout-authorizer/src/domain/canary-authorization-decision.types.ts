/**
 * canary-authorization-decision.types.ts
 *
 * Domain types for what `canary-rollout-decision-engine.ts` actually decides. This is the
 * "authorization" half of "A/B & Canary Differential Benchmarking": `telemetry-lake/telemetry-query-api`'s
 * `AbCanaryDifferentialBenchmarkComparator` (see `canary-differential-report-client.port.ts`)
 * only *reports* whether a canary looks regressed; this service turns that report into a binding
 * rollout action, gated through `rollout-controller.port.ts`.
 */

export type CanaryAuthorizationVerdict = "APPROVE_FULL_ROLLOUT" | "HOLD_AT_CURRENT_WEIGHT" | "ROLLBACK";

/** Which policy threshold (if any) drove a non-APPROVE decision, so operators/audit logs know
 * *why* without re-deriving it from raw deltas. `null` only when the verdict is
 * `APPROVE_FULL_ROLLOUT` off consecutive clean windows, i.e. no single breach triggered it. */
export type CanaryAuthorizationTrigger =
  | "P99_LATENCY_REGRESSION"
  | "ERROR_RATE_REGRESSION"
  | "P50_LATENCY_REGRESSION"
  | "CONSECUTIVE_CLEAN_WINDOWS"
  | null;

/** One metric's contribution to a decision — mirrors the shape of
 * `telemetry-query-api`'s `MetricDelta` (`controlValue`/`canaryValue`/`relativeDeltaFraction`)
 * but adds the policy threshold it was compared against, since that threshold is this service's
 * own policy config, not something the comparator report carries. */
export interface AuthorizationMetricDelta {
  metricName: "p50LatencyMs" | "p99LatencyMs" | "errorRate";
  controlValue: number;
  canaryValue: number;
  relativeDeltaFraction: number;
  /** The configured threshold this delta was evaluated against (a fraction for latency
   * metrics, e.g. 0.10 for "p99 must not be >10% worse"; a percentage-point fraction for error
   * rate, e.g. 0.02 for "+2pp"). See `canary-rollout-decision-engine.ts`'s
   * `CanaryRolloutPolicyThresholds`. */
  thresholdFraction: number;
  breachedThreshold: boolean;
}

export interface CanaryAuthorizationDecision {
  serviceName: string;
  canaryVersionDagNodeId: string;
  controlVersionDagNodeId: string;
  verdict: CanaryAuthorizationVerdict;
  trigger: CanaryAuthorizationTrigger;
  /** Human-readable explanation, safe to surface directly in a Slack alert / audit log entry —
   * see `canary-rollout-decision-engine.ts`'s `buildRationale`. */
  rationale: string;
  metricDeltas: AuthorizationMetricDelta[];
  /** How many consecutive evaluation windows (including this one) have been clean
   * (non-regressed) for this canary — carried in the decision so `APPROVE_FULL_ROLLOUT`'s
   * "N consecutive clean windows" trigger is auditable after the fact, and so callers persisting
   * decision history don't need to separately reconstruct the streak. */
  consecutiveCleanWindowCount: number;
  evaluatedAtIso: string;
}
