# 0008. Streaming, seasonality-aware anomaly detection and root-cause correlation

- Status: Accepted
- Date: 2026-08-11
- Owners: telemetry-lake maintainers, telemetry-query-api maintainers

## Context

`StatisticalBaselineDeviationDetector` (ADR 0003's read path, `telemetry-query-api/src/analytics/`)
answers "does this service's current behavior look different from its own history" via a
batch-fit isolation forest: `fit()` a historical baseline window, then `score()` a fixed
evaluation window. Two gaps became apparent as more of the platform started depending on it for
more than one-off quality-gate checks:

1. **Seasonality causes false positives.** The detector scores raw `{p50, p99, errorRate}`
   feature vectors with no notion of time-of-week. A load-testing platform's *own* traffic is
   not stationary — the mix of scheduled pipeline runs on a Monday morning looks nothing like a
   Sunday night, independent of any real regression. Scoring raw values means every ordinary
   weekly/daily traffic swing risks tripping the same `anomalyScoreThreshold` a genuine
   regression would, which erodes trust in the detector's alerts over time (on-call engineers
   start ignoring "anomalous" pages that turn out to be "it's Monday").
2. **Batch-only scoring doesn't fit a live dashboard.** `fit()` requires a full historical
   window materialized up front and produces a static forest; there's no way to fold in new data
   without re-running `fit()` over a growing multi-day window on every update. That's the right
   shape for "evaluate the last 15 minutes before failing a quality gate" but the wrong shape for
   a dashboard panel that wants a continuously-updated anomaly score per incoming minute bucket.
3. **Detecting an anomaly was always the end of the investigation.** Once a deviation was
   flagged, answering "why" meant an engineer manually cross-referencing dashboards against
   `dag-workflow-scheduler` deploy history, `chaos-ebpf-sidecar` fault schedules, and any
   config-change log — by hand, under incident pressure, every time. The platform already
   ingests chaos-fault-injection events into the same telemetry lake (`raw_telemetry_events`,
   see `redpanda-ingestion-pipeline`'s `ChaosFaultInjectionEvent`); nothing was using that
   proximity in time to a flagged anomaly as an actual signal.

## Decision

Three additions to `telemetry-query-api`, all read-path only (no ingestion/schema changes beyond
retention, see the note on migration 005 below):

- **`SeasonalBaselineNormalizer`** (`src/analytics/seasonal-baseline-normalizer.ts`) decomposes a
  metric's time series into `trend + seasonal + residual` — an EWMA trend (causal, so it extends
  into a live evaluation window without needing future data) plus a centered per-`(day-of-week,
  hour-of-day)` seasonal offset table fit from history — *before* any anomaly scoring. Both
  detectors now score residuals, not raw values, when seasonality adjustment is wired in by the
  caller.
- **`StreamingIsolationForestAnomalyDetector`** (`src/analytics/streaming-isolation-forest.ts`)
  implements Half-Space Trees (Tan, Ting & Liu, IJCAI 2011) as a complementary, incrementally
  updatable detector: tree structure is fixed once at `fit()`, and `update()` folds in new data by
  adjusting per-node mass counts in O(depth) per point — cheap enough to call on every incoming
  bucket, and the model adapts to a sustained regime shift (e.g. a real, persistent regression)
  without a full re-`fit()`. `StatisticalBaselineDeviationDetector` is unchanged and remains the
  right choice for "evaluate a fixed window against a fixed historical baseline" quality-gate
  checks.
- **`AnomalyDetectorPort`** (`src/analytics/anomaly-detector.port.ts`) is the shared `fit()`/
  `score()` contract both detectors implement, so callers depend on "an anomaly detector," not a
  specific algorithm, and scores from either are directly comparable.
- **`IncidentRootCauseCorrelator`** (`src/correlation/incident-root-cause-correlator.ts`) takes a
  detected anomaly (service + timestamp) and queries three new `ClickHouseQueryRepository`
  methods — `getRecentChaosInjectionEventsNear`, `getRecentDeployEventsNear`,
  `getConfigDriftEventsNear` — for candidate causes within a configurable window, ranking them by
  temporal proximity (exponential decay from the anomaly timestamp) multiplied by a per-kind
  historical-correlation-strength prior (chaos-fault overlap is weighted highest, since the
  platform injects those faults specifically to observe blast radius; deploys next; config drift
  lowest, since it's real but weaker signal on its own). Exposed via
  `RootCauseCorrelationController` at `GET /telemetry/root-cause-correlation`.

All three stay dependency-free/from-scratch, matching this service's existing precedent
(`StatisticalBaselineDeviationDetector`'s isolation forest, `AbCanaryDifferentialBenchmarkComparator`'s
paired comparison) — the feature space is small and low-dimensional enough that a general ML
framework would be overkill, and a transparent, auditable scoring function is worth more than a
black-box model when the output feeds an on-call engineer's incident response.

Separately, `clickhouse-schema-migrations/005_hot_tier_ttl_and_cold_tier_storage_policy.sql`
introduces hot/cold storage tiering for `raw_telemetry_events` (7-day local-disk hot tier, then
S3-backed cold tier until the existing 30-day deletion) via `TTL ... TO VOLUME` + a
`storage_policy`. This is bundled into the same change set because both `IncidentRootCauseCorrelator`
and the streaming detector's `update()` path increase how often `raw_telemetry_events` gets
queried for recent (event-window, not full-history) data — reinforcing that recency is the access
pattern worth optimizing hot storage for, while the cost of keeping the full 30-day window on hot
disk was already a mismatch (see migration 005's own header comment and the schema-migrations
README's "Retention tiering" section for the full cost/latency tradeoff table).

## Consequences

**Positive:**

- Ordinary weekly/daily traffic shape (the "Monday vs. Sunday" case) no longer needs to trip an
  anomaly threshold once a caller routes a metric through `SeasonalBaselineNormalizer` first —
  the residual a genuine regression produces stays large regardless of what day/hour it lands on,
  while ordinary seasonal swings collapse toward zero.
- A live dashboard can now get a continuously-updated anomaly score without re-fitting a growing
  historical window on every tick, and the streaming detector's mass profile genuinely adapts to
  a sustained regression becoming "the new normal" rather than staying pinned to a stale baseline
  forever.
- Root-cause correlation turns "an anomaly fired" into a ranked, explainable list of likely
  causes in one API call, cutting the manual cross-referencing step out of the common case (an
  active chaos experiment or a recent deploy near the anomaly timestamp).
- `AnomalyDetectorPort` means a future third detection strategy (or a swap to a real ML package)
  is additive — implement the interface, no changes required at call sites already coded against
  the port.

**Negative / trade-offs:**

- Two anomaly detectors now exist side by side with different operational characteristics
  (batch/re-fit vs. streaming/update) and different score-normalization internals; a caller
  choosing the wrong one for their use case (e.g. streaming-scoring a one-off quality-gate check
  instead of just calling the simpler batch detector) works but is needlessly complex.
- `IncidentRootCauseCorrelator`'s confidence scoring is a transparent heuristic (temporal decay ×
  a hand-tuned per-kind prior), not a model trained on actual historical incident outcomes — the
  prior weights (`historicalCorrelationStrengthByKind`) are reasonable starting assumptions, not
  measured correlation strength, and should be revisited once enough real incidents have gone
  through this path to validate or correct them.
- Chaos/deploy/config-drift events are read from `raw_telemetry_events` as `signal_kind = 'log'`
  rows distinguished by an `event_type` attribute rather than dedicated tables — reuses the
  existing ingestion path with no schema changes, but means these repository methods depend on
  every producer (`chaos-ebpf-sidecar`, `dag-workflow-scheduler`, and whatever eventually detects
  config drift) consistently setting that attribute; a producer that emits a plain unstructured
  log instead is invisible to the correlator.
- The hot/cold storage tiering in migration 005 is production/staging-only (see that migration's
  header comment) — it depends on a storage policy declared in server config XML that local dev's
  `docker-compose.yml` does not define, so it is intentionally a no-op-fail in local development.
