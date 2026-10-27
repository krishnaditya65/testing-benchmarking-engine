/**
 * seasonal-baseline-normalizer.ts
 *
 * Both anomaly detectors in this directory (`StatisticalBaselineDeviationDetector`,
 * `StreamingIsolationForestAnomalyDetector`) score a point by how far it sits from "normal" —
 * but a load-testing platform's traffic is not stationary: Monday-morning p99 latency and
 * Sunday-3am p99 latency are both "normal," just for different reasons. Scoring either detector
 * directly against raw latency/error-rate values means the trough-to-peak swing of ordinary
 * weekly/daily traffic shape gets misread as anomalous deviation, producing false positives that
 * erode trust in quality-gate failures.
 *
 * This module decomposes a single metric's time series into three additive components —
 * `observed = trend + seasonal + residual` — *before* any anomaly scoring happens, so detectors
 * score the residual (what's left after removing both slow drift and expected weekly/daily
 * shape) rather than the raw value. This is a from-scratch, dependency-free STL-style
 * decomposition (Cleveland et al., "STL: A Seasonal-Trend Decomposition Procedure Based on
 * Loess," 1990) sized for this service's per-minute buckets:
 *
 * - **Trend**: an exponentially-weighted moving average (EWMA) of the raw value. EWMA is used
 *   instead of a centered moving average specifically because it's causal — it only looks
 *   backward, so it can be extended point-by-point into a live evaluation window without needing
 *   "future" data the way a centered window would, which matters for scoring the last 5-15
 *   minutes for a quality gate.
 * - **Seasonal**: the detrended value averaged per (day-of-week, hour-of-day) bucket over the
 *   fitted history — 168 buckets, the same granularity a human on-call engineer implicitly uses
 *   when they say "that's normal for a Monday morning." Bucket averages are centered (their mean
 *   subtracted back out) so the seasonal component nets to ~0 across a full weekly cycle and
 *   doesn't silently absorb part of the trend.
 * - **Residual**: whatever's left — `observed - trend - seasonal`. This is what should look like
 *   stationary noise for normal traffic, and is what gets handed to the anomaly detectors.
 */

export interface TimestampedValue {
  bucketMinute: Date;
  value: number;
}

export interface SeasonalDecomposition {
  bucketMinute: Date;
  observedValue: number;
  trendComponent: number;
  seasonalComponent: number;
  /** observedValue - trendComponent - seasonalComponent; feed this into an AnomalyDetectorPort
   * instead of the raw value to avoid seasonality-driven false positives. */
  residualComponent: number;
}

export interface SeasonalBaselineNormalizerOptions {
  /** EWMA smoothing factor in (0, 1]. Higher = trend reacts faster to recent points but absorbs
   * more of what should be seasonal/residual signal; lower = smoother trend but slower to track
   * genuine sustained drift (e.g. a real capacity regression that persists for days). 0.1 means
   * the trend's effective look-back is roughly 1/0.1 = 10 buckets. */
  trendSmoothingFactor: number;
  /** A (day-of-week, hour-of-day) bucket needs at least this many historical samples during
   * fit() before its averaged seasonal offset is trusted; buckets with fewer samples (e.g. a
   * baseline window shorter than 2 weeks) fall back to a seasonal offset of 0 rather than a
   * noisy single-sample average. */
  minimumSamplesPerSeasonalBucket: number;
}

export const DEFAULT_SEASONAL_BASELINE_NORMALIZER_OPTIONS: SeasonalBaselineNormalizerOptions = {
  trendSmoothingFactor: 0.1,
  minimumSamplesPerSeasonalBucket: 3,
};

/** Key is "<dayOfWeek 0-6>-<hourOfDay 0-23>", giving 168 possible buckets — enough resolution to
 * separate "Monday 9am" traffic from "Sunday 9am" traffic without so many buckets that a
 * multi-week baseline can't populate them meaningfully. */
function seasonalBucketKey(date: Date): string {
  return `${date.getUTCDay()}-${date.getUTCHours()}`;
}

/**
 * SeasonalBaselineNormalizer
 *
 * `fit()` against a historical baseline window (2+ weeks recommended so every day-of-week is
 * represented multiple times per hour-of-day bucket), then `decompose()` new points — typically
 * the same evaluation window that would otherwise be scored raw — to get seasonality-adjusted
 * residuals. One instance normalizes one metric; the platform's feature vectors have three
 * metrics (p50, p99, error rate), so callers fit three instances and recombine the residuals —
 * see `telemetry-query-controller.ts`'s `getSeasonallyAdjustedBaselineDeviation` for the wiring.
 */
export class SeasonalBaselineNormalizer {
  private readonly options: SeasonalBaselineNormalizerOptions;
  private seasonalOffsetByBucketKey = new Map<string, number>();
  private lastFittedTrend = 0;
  private fitted = false;

  constructor(options: Partial<SeasonalBaselineNormalizerOptions> = {}) {
    this.options = { ...DEFAULT_SEASONAL_BASELINE_NORMALIZER_OPTIONS, ...options };
  }

  /**
   * Builds the trend (final EWMA value, carried forward into `decompose()`) and the centered
   * per-seasonal-bucket offsets from a historical window. Input must be chronologically ordered.
   */
  fit(history: TimestampedValue[]): void {
    if (history.length === 0) {
      throw new Error("SeasonalBaselineNormalizer.fit() requires a non-empty history");
    }

    const detrendedSumByBucketKey = new Map<string, number>();
    const sampleCountByBucketKey = new Map<string, number>();

    let trend = history[0].value;
    for (let i = 0; i < history.length; i += 1) {
      const point = history[i];
      trend =
        i === 0
          ? point.value
          : this.options.trendSmoothingFactor * point.value + (1 - this.options.trendSmoothingFactor) * trend;

      const detrended = point.value - trend;
      const key = seasonalBucketKey(point.bucketMinute);
      detrendedSumByBucketKey.set(key, (detrendedSumByBucketKey.get(key) ?? 0) + detrended);
      sampleCountByBucketKey.set(key, (sampleCountByBucketKey.get(key) ?? 0) + 1);
    }

    const rawSeasonalAverages = new Map<string, number>();
    for (const [key, sum] of detrendedSumByBucketKey) {
      const count = sampleCountByBucketKey.get(key) ?? 0;
      if (count >= this.options.minimumSamplesPerSeasonalBucket) {
        rawSeasonalAverages.set(key, sum / count);
      }
    }

    // Center the seasonal averages so they net to ~0 across a full cycle: otherwise a baseline
    // window that happens to skew (e.g. mostly weekday samples) would let the seasonal component
    // silently absorb part of what should be trend.
    const seasonalValues = Array.from(rawSeasonalAverages.values());
    const seasonalMean = seasonalValues.length === 0 ? 0 : mean(seasonalValues);
    this.seasonalOffsetByBucketKey = new Map(
      Array.from(rawSeasonalAverages.entries()).map(([key, value]) => [key, value - seasonalMean]),
    );

    this.lastFittedTrend = trend;
    this.fitted = true;
  }

  /**
   * Decomposes new points against the fitted model. Continues the EWMA trend forward from where
   * `fit()` left off (on a local copy — repeated calls are pure/idempotent, this does not mutate
   * the fitted model; call `fit()` again with an extended history to actually re-baseline).
   */
  decompose(points: TimestampedValue[]): SeasonalDecomposition[] {
    if (!this.fitted) {
      throw new Error("SeasonalBaselineNormalizer.decompose() called before fit()");
    }

    let trend = this.lastFittedTrend;
    return points.map((point) => {
      trend = this.options.trendSmoothingFactor * point.value + (1 - this.options.trendSmoothingFactor) * trend;
      const seasonal = this.seasonalOffsetByBucketKey.get(seasonalBucketKey(point.bucketMinute)) ?? 0;
      const residual = point.value - trend - seasonal;
      return {
        bucketMinute: point.bucketMinute,
        observedValue: point.value,
        trendComponent: trend,
        seasonalComponent: seasonal,
        residualComponent: residual,
      };
    });
  }
}

function mean(values: number[]): number {
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}
