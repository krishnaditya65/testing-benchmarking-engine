import { describe, expect, it } from "vitest";
import type { BaselineFeatureVector } from "../statistical-baseline-deviation-detector";
import { StreamingIsolationForestAnomalyDetector } from "../streaming-isolation-forest";

/** A mildly-varying but tight cluster around p50=100ms/p99=210ms/errorRate=0.01 — representative
 * of "normal" traffic for a service under steady load, with just enough per-bucket jitter that
 * the forest isn't fit against literally-identical points. */
function buildNormalBaseline(count: number, startOffsetMinutes = 0): BaselineFeatureVector[] {
  const start = new Date("2026-08-01T00:00:00Z");
  return Array.from({ length: count }, (_, i) => ({
    bucketMinute: new Date(start.getTime() + (startOffsetMinutes + i) * 60_000),
    p50LatencyMs: 100 + Math.sin(i / 10) * 5 + (i % 7),
    p99LatencyMs: 210 + Math.sin(i / 10) * 8 + (i % 5),
    errorRate: 0.01 + (i % 3) * 0.001,
  }));
}

const NORMAL_POINT: BaselineFeatureVector = {
  bucketMinute: new Date("2026-08-01T05:00:00Z"),
  p50LatencyMs: 108,
  p99LatencyMs: 215,
  errorRate: 0.012,
};

const INJECTED_ANOMALY_POINT: BaselineFeatureVector = {
  bucketMinute: new Date("2026-08-01T05:00:00Z"),
  p50LatencyMs: 900,
  p99LatencyMs: 2000,
  errorRate: 0.35,
};

describe("StreamingIsolationForestAnomalyDetector", () => {
  it("throws when scoring or updating before fit()", () => {
    const detector = new StreamingIsolationForestAnomalyDetector();
    expect(() => detector.fit([])).toThrow();
    expect(() => detector.score([NORMAL_POINT])).toThrow();
    expect(() => detector.update([NORMAL_POINT])).toThrow();
  });

  it("scores an in-distribution point as normal and a wildly different point as anomalous", () => {
    const detector = new StreamingIsolationForestAnomalyDetector({ randomSeed: 42 });
    detector.fit(buildNormalBaseline(300));

    const [normalScore, anomalyScore] = detector.score([NORMAL_POINT, INJECTED_ANOMALY_POINT]);

    expect(normalScore.isAnomalous).toBe(false);
    expect(anomalyScore.isAnomalous).toBe(true);
    expect(anomalyScore.score).toBeGreaterThan(normalScore.score);
  });

  it("flags errorRate as a contributing feature when only errorRate is anomalous", () => {
    const detector = new StreamingIsolationForestAnomalyDetector({ randomSeed: 42 });
    detector.fit(buildNormalBaseline(300));

    const errorRateOnlySpike: BaselineFeatureVector = {
      bucketMinute: new Date("2026-08-01T05:00:00Z"),
      p50LatencyMs: 102,
      p99LatencyMs: 212,
      errorRate: 0.9,
    };
    const [scored] = detector.score([errorRateOnlySpike]);

    expect(scored.contributingFeatures[0]).toBe("errorRate");
  });

  it("remains stable in score for a repeated normal point after streaming in more of the same distribution", () => {
    const detector = new StreamingIsolationForestAnomalyDetector({ randomSeed: 42 });
    detector.fit(buildNormalBaseline(300));
    const [beforeUpdate] = detector.score([NORMAL_POINT]);

    detector.update(buildNormalBaseline(300, 300));
    const [afterUpdate] = detector.score([NORMAL_POINT]);

    expect(afterUpdate.isAnomalous).toBe(false);
    // Scores shouldn't swing wildly just because the reference window rolled over to a
    // differently-sized chunk of still-identically-distributed data.
    expect(Math.abs(afterUpdate.score - beforeUpdate.score)).toBeLessThan(0.15);
  });

  it("adapts to a sustained regime shift: yesterday's normal becomes anomalous, the new normal does not", () => {
    const detector = new StreamingIsolationForestAnomalyDetector({ randomSeed: 42, streamingWindowSize: 60 });
    detector.fit(buildNormalBaseline(300));

    // Stream in a sustained, persistent shift — e.g. a real steady-state regression after a bad
    // deploy — long enough to roll the reference window over several times.
    const start = new Date("2026-08-01T05:00:00Z");
    const shifted: BaselineFeatureVector[] = Array.from({ length: 600 }, (_, i) => ({
      bucketMinute: new Date(start.getTime() + i * 60_000),
      p50LatencyMs: 220 + Math.sin(i / 10) * 5 + (i % 7),
      p99LatencyMs: 430 + Math.sin(i / 10) * 8 + (i % 5),
      errorRate: 0.01 + (i % 3) * 0.001,
    }));
    detector.update(shifted);

    const newNormalPoint: BaselineFeatureVector = {
      bucketMinute: new Date(),
      p50LatencyMs: 225,
      p99LatencyMs: 435,
      errorRate: 0.011,
    };
    const [staleOldNormal, newNormal] = detector.score([NORMAL_POINT, newNormalPoint]);

    expect(newNormal.isAnomalous).toBe(false);
    expect(staleOldNormal.score).toBeGreaterThan(newNormal.score);
  });
});
