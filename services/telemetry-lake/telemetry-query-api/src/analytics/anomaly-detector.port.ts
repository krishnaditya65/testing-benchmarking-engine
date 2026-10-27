/**
 * anomaly-detector.port.ts
 *
 * Shared contract implemented by every anomaly-detection strategy this service ships, so
 * callers (the controller, quality-gate integrations, `IncidentRootCauseCorrelator`) can depend
 * on "an anomaly detector" without caring whether it's the batch, historical-window
 * `StatisticalBaselineDeviationDetector` (isolation-forest-over-a-fixed-baseline) or the
 * incremental `StreamingIsolationForestAnomalyDetector` (half-space-trees, updated online as new
 * buckets land). Both consume/produce the same `BaselineFeatureVector`/`AnomalyScore` shapes
 * defined in `statistical-baseline-deviation-detector.ts` so scores from either detector are
 * directly comparable and interchangeable at call sites.
 */
import type { AnomalyScore, BaselineFeatureVector } from "./statistical-baseline-deviation-detector";

export interface AnomalyDetectorPort {
  /**
   * Trains/initializes the detector against a historical (or initial reference-window) set of
   * feature vectors. Must be called before `score()`.
   */
  fit(baseline: BaselineFeatureVector[]): void;

  /** Scores each point independently against whatever model `fit()` produced. */
  score(points: BaselineFeatureVector[]): AnomalyScore[];
}
