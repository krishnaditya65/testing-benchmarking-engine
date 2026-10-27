-- 003_p99_latency_materialized_view.sql
--
-- Pre-aggregated latency percentile rollups, refreshed incrementally as raw events land, using
-- AggregatingMergeTree + quantile state functions. This is what `ClickHouseQueryRepository
-- .getPercentileLatency()` in telemetry-query-api reads from — querying `quantile(0.99)` over
-- raw_telemetry_events directly at platform scale (billions of spans/day during heavy load
-- testing) would be far too slow for interactive dashboards and quality-gate checks.
--
-- Bucketed per (service_name, dag_node_id, minute) so both service-level and node-level P50/P99
-- queries, and the AbCanaryDifferentialBenchmarkComparator's per-version comparisons, can be
-- served from this table by additionally filtering/grouping on attributes at query time
-- (e.g. api_version, filtered from the `attributes` Map on the raw table when needed) without
-- re-scanning raw data for the common "trend over time" case.

CREATE TABLE IF NOT EXISTS telemetry_lake.latency_percentile_rollup
(
    service_name    LowCardinality(String),
    dag_node_id      String,
    bucket_minute    DateTime('UTC'),
    -- AggregateFunction state columns: cheap to merge across buckets/partitions at query time
    -- via quantileMerge()/quantilesMerge(), rather than storing already-finalized percentiles
    -- that couldn't be re-aggregated into a coarser time bucket later.
    duration_quantiles   AggregateFunction(quantiles(0.5, 0.9, 0.95, 0.99, 0.999), Float64),
    sample_count          AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket_minute)
ORDER BY (service_name, dag_node_id, bucket_minute)
TTL bucket_minute + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_lake.latency_percentile_rollup_mv
TO telemetry_lake.latency_percentile_rollup
AS
SELECT
    service_name,
    dag_node_id,
    toStartOfMinute(event_time)                       AS bucket_minute,
    quantilesState(0.5, 0.9, 0.95, 0.99, 0.999)(duration_ms) AS duration_quantiles,
    countState()                                        AS sample_count
FROM telemetry_lake.raw_telemetry_events
WHERE signal_kind = 'trace' AND duration_ms IS NOT NULL
GROUP BY service_name, dag_node_id, bucket_minute;

-- Example read pattern (what ClickHouseQueryRepository.getPercentileLatency executes):
--
-- SELECT
--     bucket_minute,
--     quantilesMerge(0.5, 0.9, 0.95, 0.99, 0.999)(duration_quantiles) AS p
-- FROM telemetry_lake.latency_percentile_rollup
-- WHERE service_name = {service:String}
--   AND bucket_minute >= {windowStart:DateTime}
--   AND bucket_minute < {windowEnd:DateTime}
-- GROUP BY bucket_minute
-- ORDER BY bucket_minute;
