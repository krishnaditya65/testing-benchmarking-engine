-- 002_kafka_engine_ingestion_table.sql
--
-- Kafka-engine table: ClickHouse's built-in Kafka consumer, reading directly off Redpanda's
-- `telemetry.raw.v1` topic (see ARCHITECTURE.md: "ClickHouse consumes via a Kafka-engine
-- materialized view"). This table is intentionally never queried directly by application
-- code — Kafka-engine tables in ClickHouse are a consumer-group handle, not storage. A
-- materialized view drains rows from it into the durable `raw_telemetry_events` table (001).
--
-- kafka_num_consumers matches the topic's partition count so each consumer thread owns whole
-- partitions (avoids ClickHouse-side rebalancing thrash under the ingestion volumes this
-- pipeline is sized for).

CREATE TABLE IF NOT EXISTS telemetry_lake.raw_telemetry_events_kafka_queue
(
    event_id            String,
    signal_kind         String,
    service_name        String,
    pipeline_run_id      String,
    dag_node_id          String,
    trace_id             String,
    span_id              String,
    status_code          String,
    duration_ms          Nullable(Float64),
    event_time_unix_nano  String,   -- string on the wire; cast to DateTime64 in the MV
    attributes            Map(String, String),
    raw_otlp_payload      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'telemetry-ingestion-redpanda:9092',
    kafka_topic_list = 'telemetry.raw.v1',
    kafka_group_name = 'clickhouse-telemetry-lake-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 6,
    kafka_thread_per_consumer = 1,
    kafka_skip_broken_messages = 1000,
    kafka_max_block_size = 65536,
    kafka_poll_timeout_ms = 1000;

-- Drains the Kafka queue into durable storage. Runs continuously as rows arrive; this is the
-- "raw -> materialized" first hop described in this directory's README.
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_lake.raw_telemetry_events_ingest_mv
TO telemetry_lake.raw_telemetry_events
AS
SELECT
    event_id,
    signal_kind,
    service_name,
    pipeline_run_id,
    dag_node_id,
    trace_id,
    span_id,
    status_code,
    duration_ms,
    fromUnixTimestamp64Nano(toInt64OrZero(event_time_unix_nano)) AS event_time,
    now64(3)                                                      AS ingested_at,
    attributes,
    raw_otlp_payload
FROM telemetry_lake.raw_telemetry_events_kafka_queue;
