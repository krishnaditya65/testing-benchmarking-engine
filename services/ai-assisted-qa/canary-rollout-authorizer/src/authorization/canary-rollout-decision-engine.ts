/**
 * canary-rollout-decision-engine.ts
 *
 * Applies configurable policy thresholds to a `CanaryComparisonResult` (fetched via
 * `CanaryDifferentialReportClientPort`) to produce a binding `CanaryAuthorizationDecision`. This
 * is the actual "gate" the manifest bullet calls for — `telemetry-query-api`'s comparator only
 * reports deltas and a PASS/REGRESSED verdict; this engine is what turns that report into
 * ROLLBACK / HOLD / APPROVE and, via `rollout-controller.port.ts`, an action against the real
 * rollout controller.
 *
 * Policy (see `CanaryRolloutPolicyThresholds` for the exact tunables):
 *  - p99 latency relative delta > `p99LatencyRegressionThresholdFraction` (default +10%) =>
 *    ROLLBACK.
 *  - Error rate ABSOLUTE delta (canary - control, in percentage-point terms, e.g. 1% -> 3% is a
 *    +2pp delta) > `errorRateRegressionThresholdPercentagePoints` (default +2pp) => ROLLBACK.
 *    Deliberately an absolute-percentage-point check, not a relative-fraction check like p50/p99:
 *    a service with a 0.1% baseline error rate doubling to 0.2% is a 100% *relative* increase but
 *    operationally near-irrelevant, whereas the same 0.1pp absolute increase on a service already
 *    at 5% baseline is comparatively less alarming — percentage points is the metric that
 *    actually tracks "how many more requests are failing", which is what this policy cares about.
 *  - p50 latency relative delta > `p50LatencyRegressionThresholdFraction` (default +20%, looser
 *    than p99 since p50 is expected to move more with normal load variance) => ROLLBACK.
 *  - Otherwise: if this is the `consecutiveCleanWindowsRequiredForApproval`-th consecutive clean
 *    (non-regressed) evaluation window in a row => APPROVE_FULL_ROLLOUT.
 *  - Otherwise (clean now, but streak not long enough yet) => HOLD_AT_CURRENT_WEIGHT.
 *
 * When multiple metrics breach simultaneously, `trigger` reports p99 first, then error rate,
 * then p50 (latency tail regressions are the strongest single signal of a bad canary; error rate
 * next; p50 last since it's the loosest threshold) — see `pickTrigger`.
 */
import type {
  AuthorizationMetricDelta,
  CanaryAuthorizationDecision,
  CanaryAuthorizationTrigger,
} from "../domain/canary-authorization-decision.types";
import type { CanaryComparisonResult } from "./canary-differential-report-client.port";

export interface CanaryRolloutPolicyThresholds {
  p99LatencyRegressionThresholdFraction: number;
  p50LatencyRegressionThresholdFraction: number;
  errorRateRegressionThresholdPercentagePoints: number;
  /** How many consecutive clean (non-regressed) evaluation windows are required before
   * authorizing a full rollout. Guards against approving off a single lucky window. */
  consecutiveCleanWindowsRequiredForApproval: number;
}

export const DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS: CanaryRolloutPolicyThresholds = {
  p99LatencyRegressionThresholdFraction: 0.1,
  p50LatencyRegressionThresholdFraction: 0.2,
  errorRateRegressionThresholdPercentagePoints: 0.02,
  consecutiveCleanWindowsRequiredForApproval: 3,
};

/**
 * CanaryRolloutDecisionEngine
 *
 * Pure — no I/O, no clock dependency beyond stamping `evaluatedAtIso` (mirrors
 * `telemetry-query-api`'s `StatisticalBaselineDeviationDetector`: deterministic given its
 * inputs, so it's cheap to unit test against fixed reports). Consecutive-window tracking is
 * intentionally NOT internal state on this class — callers (typically a scheduled evaluation job)
 * persist `consecutiveCleanWindowCount` between calls and pass it back in, the same way
 * `dag-workflow-scheduler` keeps workflow state in Temporal rather than in a stateful engine
 * object — so this engine stays trivially horizontally-scalable and restart-safe.
 */
export class CanaryRolloutDecisionEngine {
  constructor(private readonly thresholds: CanaryRolloutPolicyThresholds = DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS) {}

  evaluate(report: CanaryComparisonResult, priorConsecutiveCleanWindowCount: number): CanaryAuthorizationDecision {
    const p99Delta = buildLatencyDelta("p99LatencyMs", report.p99, this.thresholds.p99LatencyRegressionThresholdFraction);
    const p50Delta = buildLatencyDelta("p50LatencyMs", report.p50, this.thresholds.p50LatencyRegressionThresholdFraction);
    const errorRateDelta = buildErrorRateDelta(report, this.thresholds.errorRateRegressionThresholdPercentagePoints);

    const metricDeltas = [p99Delta, errorRateDelta, p50Delta];
    const trigger = pickTrigger(p99Delta, errorRateDelta, p50Delta);

    if (trigger !== null) {
      return {
        serviceName: report.serviceName,
        canaryVersionDagNodeId: report.canaryVersionDagNodeId,
        controlVersionDagNodeId: report.controlVersionDagNodeId,
        verdict: "ROLLBACK",
        trigger,
        rationale: buildRationale("ROLLBACK", trigger, metricDeltas, 0),
        metricDeltas,
        consecutiveCleanWindowCount: 0,
        evaluatedAtIso: new Date().toISOString(),
      };
    }

    const consecutiveCleanWindowCount = priorConsecutiveCleanWindowCount + 1;
    const hasEnoughCleanWindows = consecutiveCleanWindowCount >= this.thresholds.consecutiveCleanWindowsRequiredForApproval;
    const verdict = hasEnoughCleanWindows ? "APPROVE_FULL_ROLLOUT" : "HOLD_AT_CURRENT_WEIGHT";
    const decisionTrigger: CanaryAuthorizationTrigger = hasEnoughCleanWindows ? "CONSECUTIVE_CLEAN_WINDOWS" : null;

    return {
      serviceName: report.serviceName,
      canaryVersionDagNodeId: report.canaryVersionDagNodeId,
      controlVersionDagNodeId: report.controlVersionDagNodeId,
      verdict,
      trigger: decisionTrigger,
      rationale: buildRationale(verdict, decisionTrigger, metricDeltas, consecutiveCleanWindowCount),
      metricDeltas,
      consecutiveCleanWindowCount,
      evaluatedAtIso: new Date().toISOString(),
    };
  }
}

function buildLatencyDelta(
  metricName: "p50LatencyMs" | "p99LatencyMs",
  delta: CanaryComparisonResult["p50"],
  thresholdFraction: number,
): AuthorizationMetricDelta {
  return {
    metricName,
    controlValue: delta.controlValue,
    canaryValue: delta.canaryValue,
    relativeDeltaFraction: delta.relativeDeltaFraction,
    thresholdFraction,
    breachedThreshold: delta.relativeDeltaFraction > thresholdFraction,
  };
}

function buildErrorRateDelta(
  report: CanaryComparisonResult,
  thresholdPercentagePoints: number,
): AuthorizationMetricDelta {
  const absoluteDeltaPercentagePoints = report.errorRate.canaryValue - report.errorRate.controlValue;
  return {
    metricName: "errorRate",
    controlValue: report.errorRate.controlValue,
    canaryValue: report.errorRate.canaryValue,
    // Reported as the absolute percentage-point delta, not the relative fraction, so it's
    // directly comparable to `thresholdFraction` here (both are "percentage points" in this
    // row) — see module doc comment for why error rate uses absolute rather than relative delta.
    relativeDeltaFraction: absoluteDeltaPercentagePoints,
    thresholdFraction: thresholdPercentagePoints,
    breachedThreshold: absoluteDeltaPercentagePoints > thresholdPercentagePoints,
  };
}

function pickTrigger(
  p99Delta: AuthorizationMetricDelta,
  errorRateDelta: AuthorizationMetricDelta,
  p50Delta: AuthorizationMetricDelta,
): CanaryAuthorizationTrigger {
  if (p99Delta.breachedThreshold) return "P99_LATENCY_REGRESSION";
  if (errorRateDelta.breachedThreshold) return "ERROR_RATE_REGRESSION";
  if (p50Delta.breachedThreshold) return "P50_LATENCY_REGRESSION";
  return null;
}

function buildRationale(
  verdict: CanaryAuthorizationDecision["verdict"],
  trigger: CanaryAuthorizationTrigger,
  metricDeltas: AuthorizationMetricDelta[],
  consecutiveCleanWindowCount: number,
): string {
  if (verdict === "ROLLBACK") {
    const breached = metricDeltas.find((d) => d.breachedThreshold);
    const percent = breached ? Math.round(breached.relativeDeltaFraction * 100) : 0;
    return (
      `ROLLBACK: ${trigger} breached its policy threshold (observed delta ${percent}%, ` +
      `threshold ${Math.round((breached?.thresholdFraction ?? 0) * 100)}%). Canary ` +
      `${breached?.metricName} moved from ${breached?.controlValue} (control) to ` +
      `${breached?.canaryValue} (canary).`
    );
  }
  if (verdict === "APPROVE_FULL_ROLLOUT") {
    return (
      `APPROVE_FULL_ROLLOUT: ${consecutiveCleanWindowCount} consecutive evaluation windows ` +
      `stayed within all configured p50/p99/error-rate thresholds.`
    );
  }
  return (
    `HOLD_AT_CURRENT_WEIGHT: this window is clean (${consecutiveCleanWindowCount} consecutive ` +
    `clean window(s) so far), but more consecutive clean windows are required before ` +
    `authorizing a full rollout.`
  );
}
