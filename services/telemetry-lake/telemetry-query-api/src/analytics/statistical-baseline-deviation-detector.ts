/**
 * statistical-baseline-deviation-detector.ts
 *
 * Flags when a service's current latency/error behavior deviates from its own historical
 * baseline, using an Isolation-Forest-style scoring approach: anomalies are "few and
 * different," so instead of modeling a dense probability distribution we isolate each point by
 * recursive random partitioning and score it by how *few* partitions it took to isolate.
 * Points that isolate quickly (short average path length across many trees) are anomalous;
 * points that take many splits to separate from the rest of the data look normal.
 *
 * This is a from-scratch, dependency-free implementation sized for the low-dimensional feature
 * vectors (p50, p99, error rate) this service scores per time bucket — not a general-purpose ML
 * library. It implements the same interface a future swap to a real isolation-forest package
 * could satisfy.
 *
 * This is the *batch* member of the `AnomalyDetectorPort` family (see
 * `anomaly-detector.port.ts`): it scores a fixed historical baseline window and is re-`fit()`
 * whenever the caller wants to re-baseline. For online/incremental scoring as new buckets
 * arrive without a full re-fit, see the complementary `streaming-isolation-forest.ts`.
 */

export interface BaselineFeatureVector {
  bucketMinute: Date;
  p50LatencyMs: number;
  p99LatencyMs: number;
  errorRate: number;
}

export interface AnomalyScore {
  bucketMinute: Date;
  /** Normalized anomaly score in [0, 1]; conventionally >= 0.6 is "investigate", >= 0.75 is
   * "likely regression" — see README for how these thresholds were chosen. */
  score: number;
  isAnomalous: boolean;
  /** Which raw feature(s) most influenced the isolation path, for human-readable alerting. */
  contributingFeatures: Array<keyof Omit<BaselineFeatureVector, "bucketMinute">>;
}

import type { AnomalyDetectorPort } from "./anomaly-detector.port";

export interface IsolationForestOptions {
  /** Number of randomized isolation trees to average path length over. More trees = more
   * stable scores at the cost of more compute; 100 is the canonical default from the original
   * Isolation Forest paper (Liu, Ting, Zhou 2008). */
  numberOfTrees: number;
  /** Points sampled (with replacement) per tree; keeping this well below the dataset size is
   * what makes isolation forests cheap even on large baselines. */
  subsampleSize: number;
  /** Max tree depth; the paper shows path lengths beyond log2(subsampleSize) stop being
   * informative, so we cap there for efficiency. */
  maxTreeDepth: number;
  /** Score threshold above which a point is flagged anomalous. */
  anomalyScoreThreshold: number;
  /** Deterministic seed so scoring is reproducible in tests and CI quality gates. */
  randomSeed: number;
}

export const DEFAULT_ISOLATION_FOREST_OPTIONS: IsolationForestOptions = {
  numberOfTrees: 100,
  subsampleSize: 256,
  maxTreeDepth: Math.ceil(Math.log2(256)),
  anomalyScoreThreshold: 0.6,
  randomSeed: 1337,
};

type FeatureKey = "p50LatencyMs" | "p99LatencyMs" | "errorRate";
const FEATURE_KEYS: FeatureKey[] = ["p50LatencyMs", "p99LatencyMs", "errorRate"];

interface IsolationTreeNode {
  size: number;
  splitFeature?: FeatureKey;
  splitValue?: number;
  left?: IsolationTreeNode;
  right?: IsolationTreeNode;
}

/** Mulberry32 — small, fast, deterministic PRNG so scoring is reproducible given a seed
 * without pulling in a random-number-generation dependency. */
function createSeededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Average external-node path length for unsuccessful BST search of n points — the
 * normalization constant c(n) from the Isolation Forest paper, used to turn a raw average path
 * length into a score in [0, 1]. */
function averagePathLengthNormalizer(n: number): number {
  if (n <= 1) return 1;
  const EULER_MASCHERONI = 0.5772156649;
  return 2 * (Math.log(n - 1) + EULER_MASCHERONI) - (2 * (n - 1)) / n;
}

/**
 * StatisticalBaselineDeviationDetector
 *
 * Builds an isolation forest over a service's historical (baseline) feature vectors, then
 * scores new/current feature vectors against it. Intended usage: pull `getLatencySeriesForBaseline`
 * + `getErrorRateRollup` from `ClickHouseQueryRepository` for a long lookback window (e.g. 14
 * days) to `fit()`, then score the last N minutes to detect regressions for quality gates and
 * A/B canary evaluation.
 */
export class StatisticalBaselineDeviationDetector implements AnomalyDetectorPort {
  private readonly options: IsolationForestOptions;
  private trees: IsolationTreeNode[] = [];
  private fittedSampleSize = 0;

  constructor(options: Partial<IsolationForestOptions> = {}) {
    this.options = { ...DEFAULT_ISOLATION_FOREST_OPTIONS, ...options };
  }

  /** Trains the forest against a historical baseline window. Must be called before scoring. */
  fit(baseline: BaselineFeatureVector[]): void {
    if (baseline.length === 0) {
      throw new Error("StatisticalBaselineDeviationDetector.fit() requires a non-empty baseline");
    }
    const random = createSeededRandom(this.options.randomSeed);
    this.fittedSampleSize = Math.min(this.options.subsampleSize, baseline.length);
    this.trees = Array.from({ length: this.options.numberOfTrees }, () => {
      const subsample = sampleWithReplacement(baseline, this.fittedSampleSize, random);
      return this.buildIsolationTree(subsample, 0, random);
    });
  }

  /** Scores each point independently against the fitted forest. */
  score(points: BaselineFeatureVector[]): AnomalyScore[] {
    if (this.trees.length === 0) {
      throw new Error("StatisticalBaselineDeviationDetector.score() called before fit()");
    }
    const normalizer = averagePathLengthNormalizer(this.fittedSampleSize);
    return points.map((point) => {
      const pathLengths = this.trees.map((tree) => this.pathLength(tree, point, 0));
      const averagePathLength = mean(pathLengths);
      const score = Math.pow(2, -averagePathLength / normalizer);
      return {
        bucketMinute: point.bucketMinute,
        score,
        isAnomalous: score >= this.options.anomalyScoreThreshold,
        contributingFeatures: this.rankContributingFeatures(point),
      };
    });
  }

  private buildIsolationTree(
    points: BaselineFeatureVector[],
    depth: number,
    random: () => number,
  ): IsolationTreeNode {
    if (depth >= this.options.maxTreeDepth || points.length <= 1) {
      return { size: points.length };
    }

    const splittableFeatures = FEATURE_KEYS.filter((key) => {
      const values = points.map((p) => p[key]);
      return Math.min(...values) !== Math.max(...values);
    });
    if (splittableFeatures.length === 0) {
      return { size: points.length };
    }

    const splitFeature = splittableFeatures[Math.floor(random() * splittableFeatures.length)];
    const values = points.map((p) => p[splitFeature]);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const splitValue = min + random() * (max - min);

    const left = points.filter((p) => p[splitFeature] < splitValue);
    const right = points.filter((p) => p[splitFeature] >= splitValue);

    return {
      size: points.length,
      splitFeature,
      splitValue,
      left: this.buildIsolationTree(left, depth + 1, random),
      right: this.buildIsolationTree(right, depth + 1, random),
    };
  }

  private pathLength(node: IsolationTreeNode, point: BaselineFeatureVector, depth: number): number {
    if (!node.splitFeature || node.splitValue === undefined || !node.left || !node.right) {
      // External node: add the average-path-length correction for the points still bundled
      // here, per the Isolation Forest paper, instead of treating size-1 leaves as depth 0.
      return depth + averagePathLengthNormalizer(node.size);
    }
    const branch = point[node.splitFeature] < node.splitValue ? node.left : node.right;
    return this.pathLength(branch, point, depth + 1);
  }

  /** Heuristic explanation: which feature(s) sit furthest (in standard-deviation terms) from
   * the mean of the fitted trees' root-level split values — cheap, human-readable "why" for
   * an alert without re-running the full isolation logic per feature. */
  private rankContributingFeatures(point: BaselineFeatureVector): FeatureKey[] {
    const rootSplits = this.trees
      .map((tree) => (tree.splitFeature ? { feature: tree.splitFeature, value: tree.splitValue! } : null))
      .filter((v): v is { feature: FeatureKey; value: number } => v !== null);

    const deviationByFeature = new Map<FeatureKey, number>();
    for (const key of FEATURE_KEYS) {
      const splitsForFeature = rootSplits.filter((s) => s.feature === key).map((s) => s.value);
      if (splitsForFeature.length === 0) continue;
      const avgSplit = mean(splitsForFeature);
      deviationByFeature.set(key, Math.abs(point[key] - avgSplit));
    }

    return Array.from(deviationByFeature.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([feature]) => feature);
  }
}

function sampleWithReplacement<T>(items: T[], count: number, random: () => number): T[] {
  const result: T[] = [];
  for (let i = 0; i < count; i += 1) {
    result.push(items[Math.floor(random() * items.length)]);
  }
  return result;
}

function mean(values: number[]): number {
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}
