import { describe, expect, it } from "vitest";
import { CanaryRolloutDecisionEngine, DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS } from "./canary-rollout-decision-engine";
import type { CanaryComparisonResult } from "./canary-differential-report-client.port";

function baseReport(overrides: Partial<CanaryComparisonResult> = {}): CanaryComparisonResult {
  return {
    serviceName: "checkout-service",
    controlVersionDagNodeId: "node-control",
    canaryVersionDagNodeId: "node-canary",
    window: { start: "2026-08-11T00:00:00.000Z", end: "2026-08-11T01:00:00.000Z" },
    p50: { controlValue: 100, canaryValue: 105, relativeDeltaFraction: 0.05, isRegression: false },
    p99: { controlValue: 300, canaryValue: 310, relativeDeltaFraction: 0.033, isRegression: false },
    errorRate: { controlValue: 0.01, canaryValue: 0.011, relativeDeltaFraction: 0.1, isRegression: false },
    overallVerdict: "PASS",
    ...overrides,
  };
}

describe("CanaryRolloutDecisionEngine", () => {
  it("ROLLBACKs when p99 latency delta exceeds the configured threshold", () => {
    const report = baseReport({
      p99: { controlValue: 300, canaryValue: 340, relativeDeltaFraction: 0.1334, isRegression: true },
    });

    const decision = new CanaryRolloutDecisionEngine().evaluate(report, 0);

    expect(decision.verdict).toBe("ROLLBACK");
    expect(decision.trigger).toBe("P99_LATENCY_REGRESSION");
    expect(decision.consecutiveCleanWindowCount).toBe(0);
    expect(decision.rationale).toContain("P99_LATENCY_REGRESSION");
  });

  it("ROLLBACKs when error rate delta exceeds +2 percentage points, using absolute not relative delta", () => {
    // control 1% -> canary 4%: +3pp absolute, but only a 3x (200%) relative increase either way —
    // this must trip on the ABSOLUTE 2pp threshold regardless of the relative framing.
    const report = baseReport({
      errorRate: { controlValue: 0.01, canaryValue: 0.04, relativeDeltaFraction: 3, isRegression: true },
    });

    const decision = new CanaryRolloutDecisionEngine().evaluate(report, 0);

    expect(decision.verdict).toBe("ROLLBACK");
    expect(decision.trigger).toBe("ERROR_RATE_REGRESSION");
  });

  it("does NOT rollback on a large relative error-rate increase that stays under the absolute pp threshold", () => {
    // control 0.1% -> canary 0.5%: 400% relative increase, but only +0.4pp absolute — should pass.
    const report = baseReport({
      errorRate: { controlValue: 0.001, canaryValue: 0.005, relativeDeltaFraction: 4, isRegression: false },
    });

    const decision = new CanaryRolloutDecisionEngine().evaluate(report, 0);

    expect(decision.verdict).not.toBe("ROLLBACK");
  });

  it("HOLDs at current weight when clean but the consecutive-window streak isn't long enough yet", () => {
    const report = baseReport();
    const decision = new CanaryRolloutDecisionEngine().evaluate(report, 0);

    expect(decision.verdict).toBe("HOLD_AT_CURRENT_WEIGHT");
    expect(decision.trigger).toBeNull();
    expect(decision.consecutiveCleanWindowCount).toBe(1);
  });

  it("APPROVE_FULL_ROLLOUTs once the consecutive clean window count reaches the threshold", () => {
    const report = baseReport();
    const engine = new CanaryRolloutDecisionEngine();
    const priorCount = DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS.consecutiveCleanWindowsRequiredForApproval - 1;

    const decision = engine.evaluate(report, priorCount);

    expect(decision.verdict).toBe("APPROVE_FULL_ROLLOUT");
    expect(decision.trigger).toBe("CONSECUTIVE_CLEAN_WINDOWS");
    expect(decision.consecutiveCleanWindowCount).toBe(DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS.consecutiveCleanWindowsRequiredForApproval);
  });

  it("resets the consecutive clean window count to 0 on ROLLBACK regardless of prior streak", () => {
    const report = baseReport({
      p99: { controlValue: 300, canaryValue: 400, relativeDeltaFraction: 0.333, isRegression: true },
    });
    const decision = new CanaryRolloutDecisionEngine().evaluate(report, 5);

    expect(decision.verdict).toBe("ROLLBACK");
    expect(decision.consecutiveCleanWindowCount).toBe(0);
  });

  it("respects custom policy thresholds passed at construction time", () => {
    const strictEngine = new CanaryRolloutDecisionEngine({
      ...DEFAULT_CANARY_ROLLOUT_POLICY_THRESHOLDS,
      p99LatencyRegressionThresholdFraction: 0.02,
    });
    const report = baseReport({
      p99: { controlValue: 300, canaryValue: 310, relativeDeltaFraction: 0.033, isRegression: true },
    });

    const decision = strictEngine.evaluate(report, 0);

    expect(decision.verdict).toBe("ROLLBACK");
    expect(decision.trigger).toBe("P99_LATENCY_REGRESSION");
  });
});
