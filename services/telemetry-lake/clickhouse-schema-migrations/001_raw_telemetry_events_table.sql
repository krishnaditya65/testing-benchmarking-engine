-- 001_raw_telemetry_events_table.sql
--
-- Landing table for all telemetry (traces/metrics/logs) that has been consumed off the
-- `telemetry.raw.v1` Redpanda topic. This table is the durable, queryable copy of raw events;
-- the Kafka *engine* table that actually reads off Redpanda (002) writes into this table via a
-- materialized view, keeping the Kafka-engine table itself append-only/ephemeral as ClickHouse
-- best practice requires.
--
-- Cardinality note: dagNodeId/traceId/serviceName can each have very high cardinality
-- (thousands of concurrent DAG nodes x millions of spans per node during a load test), so
-- ORDER BY is chosen to make the platform's most common query shapes ("give me this service's
-- latency over the last window", "give me this trace") cheap, while partitioning stays coarse
-- (daily) so we don't explode the number of parts under high-cardinality columns.

CREATE DATABASE IF NOT EXISTS telemetry_lake;

CREATE TABLE IF NOT EXISTS telemetry_lake.raw_telemetry_events
(
    event_id            String,
    signal_kind         LowCardinality(String),   -- 'trace' | 'metric' | 'log'
    service_name        LowCardinality(String),
    pipeline_run_id      String,
    dag_node_id          String DEFAULT '',
    trace_id             String DEFAULT '',
    span_id              String DEFAULT '',
    status_code          LowCardinality(String) DEFAULT 'UNSET', -- 'OK' | 'ERROR' | 'UNSET'
    duration_ms          Nullable(Float64),
    event_time           DateTime64(9, 'UTC'),
    ingested_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    attributes            Map(String, String),
    raw_otlp_payload      String                  -- base64-encoded OTLP protobuf, for full-fidelity replay/debug
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
-- service_name first: nearly every dashboard/query filters by service before anything else.
-- pipeline_run_id + dag_node_id next: the second most common access pattern is "this run's
-- telemetry" (dashboards, quality gates). event_time last within the key so range scans on
-- time are still efficient once the higher-cardinality prefix has narrowed the granule set.
ORDER BY (service_name, pipeline_run_id, dag_node_id, event_time)
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

-- Skip index to accelerate "find this exact trace" lookups without needing trace_id in ORDER BY
-- (which would fragment the primary ordering the rest of the platform relies on).
ALTER TABLE telemetry_lake.raw_telemetry_events
    ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter GRANULARITY 4;

ALTER TABLE telemetry_lake.raw_telemetry_events
    ADD INDEX IF NOT EXISTS idx_status_code status_code TYPE set(3) GRANULARITY 4;
