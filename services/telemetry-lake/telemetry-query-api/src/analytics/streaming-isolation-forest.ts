/**
 * streaming-isolation-forest.ts
 *
 * `StatisticalBaselineDeviationDetector` is a *batch* detector: it needs a full historical
 * baseline window materialized up front, builds a fresh forest via `fit()`, and only then scores
 * points. That's the right shape for "evaluate the last 15 minutes against 14 days of history
 * before failing a quality gate," but it's the wrong shape for a live dashboard that wants a
 * per-minute anomaly score updated continuously as new buckets land — re-running `fit()` over a
 * growing multi-day window on every new minute is wasted work and doesn't let the model react to
 * gradual regime shifts (e.g. genuinely worse steady-state latency after a bad deploy) without
 * an explicit re-baseline.
 *
 * This module implements Half-Space Trees (Tan, Ting & Liu, "Fast Anomaly Detection for
 * Streaming Data," IJCAI 2011) — the standard streaming counterpart to Isolation Forest. Instead
 * of Isolation Forest's "isolate a point by counting random splits" scoring, Half-Space Trees
 * builds an ensemble of trees with random split points fixed once at `fit()` time, then counts
 * how many points ("mass") have landed in each node as data streams past via `update()`. A point
 * that lands somewhere with little historical mass, especially high up the tree (shallow depth),
 * is in a sparse/empty region of the feature space and is scored anomalous. Critically, the tree
 * *structure* never needs rebuilding to incorporate new data — only the per-node mass counts do
 * — which is what makes this cheap enough to update on every incoming bucket instead of only
 * batch-scoring a fixed window.
 *
 * This is a from-scratch, dependency-free implementation sized the same way
 * `statistical-baseline-deviation-detector.ts` is: small, low-dimensional feature vectors
 * (p50, p99, error rate), not a general streaming-ML framework. It satisfies the same
 * `AnomalyDetectorPort` as the batch detector so callers can pick either.
 */
import type { AnomalyDetectorPort } from "./anomaly-detector.port";
import type { AnomalyScore, BaselineFeatureVector } from "./statistical-baseline-deviation-detector";

type FeatureKey = "p50LatencyMs" | "p99LatencyMs" | "errorRate";
const FEATURE_KEYS: FeatureKey[] = ["p50LatencyMs", "p99LatencyMs", "errorRate"];

export interface StreamingIsolationForestOptions {
  /** Number of randomized half-space trees to average mass-profile scores over. Same
   * more-trees-is-more-stable/more-compute tradeoff as `IsolationForestOptions.numberOfTrees`. */
  numberOfTrees: number;
  /** Max tree depth. Half-Space Trees splits are structural (fixed at `fit()` time), so this
   * bounds both memory (2^depth nodes per tree) and how finely mass can localize a point. */
  maxTreeDepth: number;
  /** Number of `update()` calls that make up one streaming window. At the end of each window the
   * mass accumulated during that window becomes the new "reference" mass profile used for
   * scoring, and accumulation restarts at zero — this is how the model adapts to a sustained
   * regime shift (e.g. a real, persistent regression) without ever needing a full re-`fit()`. */
  streamingWindowSize: number;
  /** How far beyond the observed min/max of the fitting window each tree's random split range
   * extends, as a fraction of that window's (max - min). Half-Space Trees deliberately splits
   * over a *larger* range than what's been observed so that genuinely novel future values (not
   * just interpolations within already-seen ranges) still land somewhere in the tree and can be
   * scored, rather than falling outside every split boundary. */
  workspaceExpansionFactor: number;
  /** Score threshold above which a point is flagged anomalous. Same [0, 1] convention as
   * `IsolationForestOptions.anomalyScoreThreshold`. */
  anomalyScoreThreshold: number;
  /** Deterministic seed so tree structure (and therefore scoring) is reproducible in tests. */
  randomSeed: number;
}

export const DEFAULT_STREAMING_ISOLATION_FOREST_OPTIONS: StreamingIsolationForestOptions = {
  numberOfTrees: 50,
  maxTreeDepth: 8,
  streamingWindowSize: 60,
  workspaceExpansionFactor: 0.5,
  anomalyScoreThreshold: 0.6,
  randomSeed: 1337,
};

/** Floor for `baselineRawScoreNormalizer` so a degenerate baseline (e.g. every point identical,
 * making every raw score 0) can't produce a divide-by-near-zero blowup that makes every future
 * point score as maximally anomalous regardless of how it compares to the baseline. */
const MINIMUM_RAW_SCORE_NORMALIZER = 1e-6;

interface FeatureRange {
  min: number;
  max: number;
}

interface HalfSpaceTreeNode {
  depth: number;
  splitFeature?: FeatureKey;
  splitValue?: number;
  left?: HalfSpaceTreeNode;
  right?: HalfSpaceTreeNode;
  /** Mass profile from the most recently *completed* streaming window — this is what scoring
   * reads from, so a point is scored against "what recent traffic looked like," not against
   * whatever partial window is still accumulating. */
  referenceMass: number;
  /** Mass accumulated so far in the streaming window currently in progress. Rolled into
   * `referenceMass` (replacing it) once `streamingWindowSize` points have been seen. */
  latestWindowMass: number;
}

/** Mulberry32 — same deterministic PRNG as `statistical-baseline-deviation-detector.ts`, reused
 * here rather than centralized because each file is meant to stay independently readable/
 * copy-paste-portable, matching this codebase's existing precedent of duplicating this exact
 * generator rather than adding an internal shared-random dependency for two call sites. */
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

/**
 * StreamingIsolationForestAnomalyDetector
 *
 * `fit()` once against an initial reference window to fix tree structure and seed the first mass
 * profile, then call `update()` per new bucket as telemetry arrives to keep the mass profile
 * current, and `score()` at any time (typically after each `update()`) to get the latest
 * per-point anomaly scores. `fit()` and `update()`/`score()` are intentionally separable, unlike
 * the batch detector where every re-baseline requires a full `fit()`.
 */
export class StreamingIsolationForestAnomalyDetector implements AnomalyDetectorPort {
  private readonly options: StreamingIsolationForestOptions;
  private trees: HalfSpaceTreeNode[] = [];
  private pointsSeenInCurrentWindow = 0;
  /** Mean raw mass-profile score of the *fitted baseline itself*, captured once right after
   * `fit()` seeds the initial reference mass profile. Raw mass-profile scores (`mass * 2^depth`
   * summed along a path) have no natural upper bound — they scale with window size and tree
   * depth — so a fixed threshold like 0.6 is only meaningful once scores are normalized relative
   * to "what a typical in-baseline point scores." Dividing by this normalizer means a point as
   * typical as the baseline lands near 0.5 (the same convention the batch detector's isolation
   * paper normalizer produces), and a point in a much sparser region than anything in the
   * baseline pushes the score toward 1. */
  private baselineRawScoreNormalizer = 1;
  /** Feature ranges from the fitting baseline, kept only to scale-normalize
   * `rankContributingFeatures`'s deviation comparison — without this, a feature with a
   * naturally large numeric range (p99 latency, tens to thousands of ms) would always dominate
   * a feature with a naturally small range (error rate, 0-1) regardless of which one actually
   * moved further from what's normal in relative terms. */
  private featureRanges = new Map<FeatureKey, FeatureRange>();

  constructor(options: Partial<StreamingIsolationForestOptions> = {}) {
    this.options = { ...DEFAULT_STREAMING_ISOLATION_FOREST_OPTIONS, ...options };
  }

  /**
   * Builds the (structurally fixed) half-space trees from an initial reference window's feature
   * ranges, then runs that same window through the trees once to seed the initial mass profile —
   * so `score()` is meaningful immediately after `fit()`, before any `update()` has happened.
   */
  fit(baseline: BaselineFeatureVector[]): void {
    if (baseline.length === 0) {
      throw new Error("StreamingIsolationForestAnomalyDetector.fit() requires a non-empty baseline");
    }
    const random = createSeededRandom(this.options.randomSeed);
    const featureRanges = this.computeExpandedFeatureRanges(baseline);
    this.featureRanges = featureRanges;

    this.trees = Array.from({ length: this.options.numberOfTrees }, () =>
      this.buildTree(featureRanges, 0, random),
    );
    this.pointsSeenInCurrentWindow = 0;

    // Seed the reference mass profile directly from the *entire* fitting baseline in one pass,
    // rather than routing it through `accumulateMass`'s windowed rollover logic — the baseline
    // can be (and typically is) many multiples of `streamingWindowSize` long, and periodic
    // rollovers mid-baseline would leave only the last, possibly-partial chunk as the seeded
    // reference, discarding everything the rest of the baseline established. `update()` (called
    // for genuinely new streaming data after `fit()`) is what applies the windowed rollover
    // behavior going forward.
    for (const point of baseline) {
      for (const tree of this.trees) {
        this.walkAndIncrementReferenceDirectly(tree, point);
      }
    }

    const baselineRawScores = baseline.map((point) => this.computeRawScore(point));
    this.baselineRawScoreNormalizer = Math.max(mean(baselineRawScores), MINIMUM_RAW_SCORE_NORMALIZER);
  }

  /**
   * Streams a new batch of points into the model: walks each point down every tree incrementing
   * `latestWindowMass` along its path, and rolls the window over (reference := latest, latest :=
   * 0) once `streamingWindowSize` points have accumulated since the last rollover. This is the
   * incremental-update capability the batch detector doesn't have — no re-`fit()` needed.
   */
  update(points: BaselineFeatureVector[]): void {
    if (this.trees.length === 0) {
      throw new Error("StreamingIsolationForestAnomalyDetector.update() called before fit()");
    }
    for (const point of points) {
      this.accumulateMass(point);
    }
  }

  /** Scores each point against the current reference mass profile (from the most recently
   * completed window, or the seeded fit window if no rollover has happened yet). */
  score(points: BaselineFeatureVector[]): AnomalyScore[] {
    if (this.trees.length === 0) {
      throw new Error("StreamingIsolationForestAnomalyDetector.score() called before fit()");
    }
    return points.map((point) => {
      const rawScore = this.computeRawScore(point);
      // Empty/sparse regions score a low raw mass-profile value (little historical mass, shallow
      // stopping depth); dense/normal regions score a high raw value. Normalize against what a
      // typical baseline point scores, then invert, so this detector's score has the same
      // "higher = more anomalous", roughly-centered-on-0.5-for-typical-points convention as the
      // batch detector's isolation-path score.
      const score = 1 / (1 + rawScore / this.baselineRawScoreNormalizer);
      return {
        bucketMinute: point.bucketMinute,
        score,
        isAnomalous: score >= this.options.anomalyScoreThreshold,
        contributingFeatures: this.rankContributingFeatures(point),
      };
    });
  }

  private computeRawScore(point: BaselineFeatureVector): number {
    return mean(this.trees.map((tree) => this.massProfileScore(tree, point)));
  }

  private accumulateMass(point: BaselineFeatureVector): void {
    for (const tree of this.trees) {
      this.walkAndIncrement(tree, point);
    }
    this.pointsSeenInCurrentWindow += 1;
    if (this.pointsSeenInCurrentWindow >= this.options.streamingWindowSize) {
      this.rolloverWindow();
    }
  }

  private walkAndIncrement(node: HalfSpaceTreeNode, point: BaselineFeatureVector): void {
    node.latestWindowMass += 1;
    if (!node.splitFeature || node.splitValue === undefined || !node.left || !node.right) {
      return;
    }
    const branch = point[node.splitFeature] < node.splitValue ? node.left : node.right;
    this.walkAndIncrement(branch, point);
  }

  /** Used only by `fit()` to seed `referenceMass` straight from the full baseline — see the
   * comment in `fit()` for why this bypasses the windowed `latestWindowMass`/rollover path that
   * `update()` uses for genuinely incremental streaming data. */
  private walkAndIncrementReferenceDirectly(node: HalfSpaceTreeNode, point: BaselineFeatureVector): void {
    node.referenceMass += 1;
    if (!node.splitFeature || node.splitValue === undefined || !node.left || !node.right) {
      return;
    }
    const branch = point[node.splitFeature] < node.splitValue ? node.left : node.right;
    this.walkAndIncrementReferenceDirectly(branch, point);
  }

  private rolloverWindow(): void {
    for (const tree of this.trees) {
      this.rolloverNode(tree);
    }
    this.pointsSeenInCurrentWindow = 0;
  }

  private rolloverNode(node: HalfSpaceTreeNode): void {
    node.referenceMass = node.latestWindowMass;
    node.latestWindowMass = 0;
    if (node.left) this.rolloverNode(node.left);
    if (node.right) this.rolloverNode(node.right);
  }

  /**
   * Descends the tree while reference mass stays positive, accumulating
   * `(mass / rootMass) * 2^depth` at each node visited, and stops as soon as it would enter a
   * zero-mass node — deeper zero-mass nodes would only ever contribute 0 anyway, and stopping
   * early at the first empty node is what lets the *depth* at which a region goes from "some
   * history" to "no history" drive the score, exactly like the paper's mass-profile scoring.
   *
   * Normalizing by the tree's own root mass (rather than using raw counts, as the original
   * paper's formula does) makes the score a *density fraction* instead of an absolute count —
   * this is what keeps scores comparable before and after `rolloverWindow()` swaps in a
   * differently-sized window (the `fit()`-seeded reference profile spans the whole baseline,
   * while every later window from `update()` spans exactly `streamingWindowSize` points; without
   * normalizing, a point's score would jump purely because the reference mass total changed size,
   * not because the point's relative position in the distribution changed).
   */
  private massProfileScore(node: HalfSpaceTreeNode, point: BaselineFeatureVector): number {
    const rootMass = node.referenceMass;
    if (rootMass === 0) return 0;
    let current: HalfSpaceTreeNode | undefined = node;
    let total = 0;
    while (current) {
      total += (current.referenceMass / rootMass) * Math.pow(2, current.depth);
      if (current.referenceMass === 0) break;
      if (!current.splitFeature || current.splitValue === undefined || !current.left || !current.right) {
        break;
      }
      current = point[current.splitFeature] < current.splitValue ? current.left : current.right;
    }
    return total;
  }

  private buildTree(featureRanges: Map<FeatureKey, FeatureRange>, depth: number, random: () => number): HalfSpaceTreeNode {
    if (depth >= this.options.maxTreeDepth) {
      return { depth, referenceMass: 0, latestWindowMass: 0 };
    }
    const splittableFeatures = FEATURE_KEYS.filter((key) => {
      const range = featureRanges.get(key);
      return range !== undefined && range.max > range.min;
    });
    if (splittableFeatures.length === 0) {
      return { depth, referenceMass: 0, latestWindowMass: 0 };
    }

    const splitFeature = splittableFeatures[Math.floor(random() * splittableFeatures.length)];
    const range = featureRanges.get(splitFeature)!;
    const splitValue = range.min + random() * (range.max - range.min);

    // Structural splits are fixed once at fit() time — unlike the batch isolation forest, which
    // re-partitions actual data recursively, half-space trees only ever split the *workspace*
    // (the expanded feature-range box), never the data itself, which is exactly what lets mass
    // counts update in O(depth) per point instead of rebuilding the tree.
    const leftRanges = new Map(featureRanges);
    leftRanges.set(splitFeature, { min: range.min, max: splitValue });
    const rightRanges = new Map(featureRanges);
    rightRanges.set(splitFeature, { min: splitValue, max: range.max });

    return {
      depth,
      splitFeature,
      splitValue,
      left: this.buildTree(leftRanges, depth + 1, random),
      right: this.buildTree(rightRanges, depth + 1, random),
      referenceMass: 0,
      latestWindowMass: 0,
    };
  }

  private computeExpandedFeatureRanges(baseline: BaselineFeatureVector[]): Map<FeatureKey, FeatureRange> {
    const ranges = new Map<FeatureKey, FeatureRange>();
    for (const key of FEATURE_KEYS) {
      const values = baseline.map((p) => p[key]);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const expansion = (max - min) * this.options.workspaceExpansionFactor;
      ranges.set(key, { min: min - expansion, max: max + expansion });
    }
    return ranges;
  }

  /** Same cheap "which feature deviates furthest from what the trees expected" heuristic as
   * `StatisticalBaselineDeviationDetector.rankContributingFeatures`, adapted to use each tree's
   * root split value (the workspace midpoint-ish reference) instead of an isolation-path split,
   * and normalized by each feature's fitted range so p50/p99 latency (large numeric range) don't
   * always outrank error rate (0-1 range) regardless of which one is actually more unusual. */
  private rankContributingFeatures(point: BaselineFeatureVector): FeatureKey[] {
    const rootSplits = this.trees
      .map((tree) => (tree.splitFeature ? { feature: tree.splitFeature, value: tree.splitValue! } : null))
      .filter((v): v is { feature: FeatureKey; value: number } => v !== null);

    const deviationByFeature = new Map<FeatureKey, number>();
    for (const key of FEATURE_KEYS) {
      const splitsForFeature = rootSplits.filter((s) => s.feature === key).map((s) => s.value);
      if (splitsForFeature.length === 0) continue;
      const avgSplit = mean(splitsForFeature);
      const range = this.featureRanges.get(key);
      const rangeWidth = range ? range.max - range.min : 0;
      const normalizedDeviation = rangeWidth > 0 ? Math.abs(point[key] - avgSplit) / rangeWidth : Math.abs(point[key] - avgSplit);
      deviationByFeature.set(key, normalizedDeviation);
    }

    return Array.from(deviationByFeature.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([feature]) => feature);
  }
}

function mean(values: number[]): number {
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}
