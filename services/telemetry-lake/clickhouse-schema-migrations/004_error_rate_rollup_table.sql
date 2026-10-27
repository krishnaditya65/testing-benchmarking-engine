-- 004_error_rate_rollup_table.sql
--
-- Pre-aggregated error-rate rollups (error count vs. total count per minute bucket), mirroring
-- the latency rollup's structure (003) so `ClickHouseQueryRepository.getErrorRateRollup()` and
-- `AbCanaryDifferentialBenchmarkComparator`'s error-rate-delta computation can both read from a
-- small, cheap-to-scan table instead of counting status codes over raw spans on every request.
--
-- SummingMergeTree (rather than AggregatingMergeTree) is sufficient here because the metric is
-- a simple additive count, not a quantile requiring a mergeable intermediate state.

CREATE TABLE IF NOT EXISTS telemetry_lake.error_rate_rollup
(
    service_name     LowCardinality(String),
    dag_node_id       String,
    bucket_minute     DateTime('UTC'),
    total_count        UInt64,
    error_count         UInt64
)
ENGINE = SummingMergeTree((total_count, error_count))
PARTITION BY toDate(bucket_minute)
ORDER BY (service_name, dag_node_id, bucket_minute)
TTL bucket_minute + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_lake.error_rate_rollup_mv
TO telemetry_lake.error_rate_rollup
AS
SELECT
    service_name,
    dag_node_id,
    toStartOfMinute(event_time)                              AS bucket_minute,
    count()                                                    AS total_count,
    countIf(status_code = 'ERROR')                             AS error_count
FROM telemetry_lake.raw_telemetry_events
WHERE signal_kind = 'trace'
GROUP BY service_name, dag_node_id, bucket_minute;

-- Example read pattern (what ClickHouseQueryRepository.getErrorRateRollup executes):
--
-- SELECT
--     bucket_minute,
--     sum(error_count) / sum(total_count) AS error_rate
-- FROM telemetry_lake.error_rate_rollup
-- WHERE service_name = {service:String}
--   AND bucket_minute >= {windowStart:DateTime}
--   AND bucket_minute < {windowEnd:DateTime}
-- GROUP BY bucket_minute
-- ORDER BY bucket_minute;
