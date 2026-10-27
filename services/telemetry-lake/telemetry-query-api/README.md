# telemetry-query-api

NestJS read API over the telemetry-lake ClickHouse instance. Serves the queries that back
dashboards, CI quality gates, and A/B canary evaluation: percentile latency, error-rate
rollups, cross-service trace correlation, statistical baseline deviation (batch and streaming),
A/B canary differential benchmarking, and incident root-cause correlation.

## Implements

- ARCHITECTURE.md: *"the read-optimized query path (`telemetry-query-api`) that dashboards and
  quality gates read from."*
- Feature manifest: **"Read API: P50/P99, error-rate, trace correlation"**, **"statistical
  baseline deviation"** (batch + streaming/seasonality-aware, see ADR 0008), and **"A/B canary
  differential benchmarking"**.
- Beyond the original manifest: **root-cause correlation** — see ADR 0008
  (`docs/architecture-decision-records/0008-streaming-anomaly-detection-and-root-cause-correlation.md`).

## Structure

```
src/
├── config/
│   └── click-house-connection-config.ts        env-sourced ClickHouse connection settings
├── repository/
│   └── click-house-query-repository.ts         ClickHouseQueryRepository — sole DB access point
├── analytics/
│   ├── anomaly-detector.port.ts                 AnomalyDetectorPort — shared fit()/score() contract
│   ├── statistical-baseline-deviation-detector.ts   batch Isolation-Forest-style anomaly scoring
│   ├── streaming-isolation-forest.ts            incremental Half-Space-Trees anomaly scoring
│   ├── seasonal-baseline-normalizer.ts          trend+seasonal+residual decomposition (pre-scoring)
│   ├── ab-canary-differential-benchmark-comparator.ts P50/P99/error-rate deltas, control vs canary
│   └── __tests__/                               unit tests for the seasonal + streaming modules
├── correlation/
│   ├── incident-root-cause-correlator.ts        ranks chaos/deploy/config-drift events near an anomaly
│   └── root-cause-correlation.controller.ts     HTTP surface for the correlator
├── controllers/
│   └── telemetry-query-controller.ts            HTTP surface (thin — delegates to the above)
├── telemetry-query-module.ts                     NestJS module wiring/DI
└── main.ts                                       bootstrap
```

## ClickHouseQueryRepository

Every query this service runs goes through `src/repository/click-house-query-repository.ts` so
there is exactly one place that knows the shape of the `clickhouse-schema-migrations` tables:

- `getPercentileLatency(service, percentile, window, dagNodeId?)` — reads
  `latency_percentile_rollup` (migration 003), merging quantile states with `quantilesMerge()`.
- `getErrorRateRollup(service, window, dagNodeId?)` — reads `error_rate_rollup` (migration 004).
- `getCrossServiceTraceCorrelation(traceId)` — reads raw `raw_telemetry_events` (migration 001)
  for every span sharing a trace id, across services, ordered by start time.
- `getLatencySeriesForBaseline(service, window)` — supporting query for the anomaly detectors;
  returns per-minute p50/p99 pairs rather than a single merged percentile.
- `getRecentChaosInjectionEventsNear(service, window)` / `getRecentDeployEventsNear(service,
  window)` / `getConfigDriftEventsNear(service, window)` — read `raw_telemetry_events` for
  `signal_kind = 'log'` rows whose `event_type` attribute marks them as a chaos-fault-lifecycle,
  deployment, or config-drift event, respectively. Feed `IncidentRootCauseCorrelator`.

## Anomaly detection: batch vs. streaming, and seasonality

Two detectors implement the shared `AnomalyDetectorPort` (`fit(baseline)` / `score(points)`) so
callers can pick the right shape for their use case; see ADR 0008 for the full rationale.

### StatisticalBaselineDeviationDetector (batch)

Implements Isolation Forest from scratch (no ML dependency — the feature space is just
`{p50LatencyMs, p99LatencyMs, errorRate}` per minute bucket, small enough that a dependency
would be overkill): builds `numberOfTrees` randomized isolation trees over a historical
baseline window via `fit()`, then `score()`s new points by how few random splits it takes to
isolate them. Short average isolation path -> high anomaly score. Deterministic given a fixed
`randomSeed`, so scoring is reproducible in tests and CI quality gates.

Typical usage: `fit()` against a 14-day baseline window, then `score()` the last 5-15 minutes
to decide whether current behavior looks like a real regression before failing a pipeline's
ASSERTION node.

### StreamingIsolationForestAnomalyDetector (incremental)

Implements Half-Space Trees (Tan, Ting & Liu, IJCAI 2011) — the streaming counterpart to
Isolation Forest — from scratch in `src/analytics/streaming-isolation-forest.ts`. Tree structure
is fixed once at `fit()`; `update(points)` folds new data into per-node mass counts in O(depth)
per point, cheap enough to call on every incoming bucket, and periodically rolls the "reference"
mass profile over to the most recently completed streaming window so the model adapts to a
sustained regime shift without ever needing a full re-`fit()`. Scores are normalized against the
fitted baseline's own typical score (not raw counts) so the `anomalyScoreThreshold` convention
stays meaningful across window-size changes. Use this for a continuously-updated live dashboard
panel; use the batch detector for a fixed-window quality-gate check.

### SeasonalBaselineNormalizer (pre-scoring decomposition)

Neither detector understands time-of-week on its own — scoring raw `{p50, p99, errorRate}`
directly means ordinary Monday-vs-Sunday traffic shape risks tripping the same threshold a real
regression would. `src/analytics/seasonal-baseline-normalizer.ts` decomposes a single metric's
time series into `trend (EWMA) + seasonal (centered per day-of-week/hour-of-day average) +
residual` before scoring; callers fit one normalizer per metric (p50, p99, errorRate) and feed
the **residuals** into either detector instead of raw values. See
`src/analytics/__tests__/seasonal-baseline-normalizer.test.ts` for the canonical "same raw value,
Monday business-hours vs. Sunday" example this exists to handle correctly.

## IncidentRootCauseCorrelator

Given a detected anomaly (`{ serviceName, timestamp }` — typically an `AnomalyScore.bucketMinute`
from either detector above), `src/correlation/incident-root-cause-correlator.ts` queries
`ClickHouseQueryRepository` for chaos-fault-injection, deploy, and config-drift events within a
configurable window around that timestamp, and ranks them as `RootCauseCandidate`s by
temporal-proximity (exponential decay from the anomaly) × a per-kind historical-correlation-
strength prior (chaos-fault overlap ranks highest, deploys next, config drift lowest). Exposed at
`GET /telemetry/root-cause-correlation?service=&anomalyTimestamp=` via
`RootCauseCorrelationController`. See ADR 0008 for the full design rationale and known
limitations (the correlation-strength priors are hand-tuned starting assumptions, not a trained
model).

## AbCanaryDifferentialBenchmarkComparator

Given a service name plus the DAG node ids representing a control deployment and a canary
deployment (both `ApiRequestNode`/`LoadTestNode` variants from `shared-libraries/dag-domain-model`
targeting the same logical service), fetches P50, P99, and error-rate for both over the same
window and computes `(canary - control) / control` for each. A pipeline's ASSERTION node can
set `regressionThresholdFraction` (e.g. `0.1` for "canary must not be >10% worse") and read
`overallVerdict` (`PASS` | `REGRESSED`) directly.

This is intentionally a different statistical approach from the baseline detector — comparing
two concurrently-sampled versions is a paired two-sample comparison, not historical anomaly
scoring — even though both classes read through the same repository.

## Running locally

```bash
pnpm install
pnpm --filter @telemetry-lake/telemetry-query-api dev
```

Expects `telemetry-metrics-clickhouse` (see repo-root `docker-compose.yml`) reachable at
`CLICKHOUSE_HTTP_URL` (defaults to `http://localhost:8123`).
