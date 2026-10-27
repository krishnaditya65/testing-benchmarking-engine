# 0003. ClickHouse + Redpanda for the telemetry lake

- Status: Accepted
- Date: 2024-04-18
- Owners: telemetry-lake maintainers

## Context

`services/telemetry-lake/` needs to absorb telemetry from every execution-fleet
worker during a load test — at 50,000 concurrent virtual users
(`examples/pipelines/checkout-load-test.yaml`'s scale), that's tens of thousands of
spans, metric points, and log lines per second, bursty (ramp-up/ramp-down shape, not
steady-state), arriving from potentially hundreds of `protocol-load-worker-go` pods
simultaneously. On the read side, `telemetry-query-api` needs to serve P50/P99/
error-rate aggregates and the `active-virtual-users` custom metric that
`protocol-load-worker-go`'s HorizontalPodAutoscaler polls — both need low-latency
answers over recent data while a test is still running, not just after-the-fact
batch analysis.

Two separate problems are being solved here: (1) absorbing a bursty, high-volume
write stream without backpressure cascading into the OTel Collector or, worse, into
the workers generating load themselves, and (2) answering aggregate analytical
queries (percentiles, group-by-time-bucket rates) fast enough for both live
dashboards and quality-gate evaluation within a running pipeline. A general-purpose
OLTP database (Postgres) is a poor fit for (2) at this volume — percentile
aggregates over hundreds of millions of rows per load test degrade badly on
row-store engines. Writing directly to any analytical store from every worker is a
poor fit for (1) — analytical stores are optimized for large batched inserts, not
tens of thousands of small concurrent writers, and a slow sink must never apply
backpressure back to a load-generation worker (see `ARCHITECTURE.md`'s "Independent
failure domains").

## Decision

Split ingestion and storage into two purpose-built systems, connected by the OTel
Collector as the standardization point:

- **Redpanda** (`services/telemetry-lake/redpanda-ingestion-pipeline`) absorbs the
  write path. Workers emit OTLP to the OpenTelemetry Collector DaemonSet, which
  batches and forwards into Redpanda's `telemetry.raw.v1` topic. Redpanda's
  log-structured, append-only design absorbs bursty high-throughput writes cheaply
  and decouples producer rate from consumer (ClickHouse ingest) rate entirely — a
  ClickHouse hiccup queues in Redpanda instead of propagating backpressure to
  `protocol-load-worker-go`.
- **ClickHouse** (`services/telemetry-lake/clickhouse-schema-migrations`) is the
  read-optimized store. A Kafka-engine table consumes `telemetry.raw.v1` directly
  (no separate consumer application needed for the raw ingest path), materialized
  views pre-aggregate into P50/P99/error-rate rollup tables per time bucket, and
  `telemetry-query-api` reads only from those materialized views for anything
  latency-sensitive (dashboards, quality-gate evaluation, the `active-virtual-users`
  metric the execution-fleet HPA polls).

Both choices are deliberately boring/mainstream within their category — Redpanda for
Kafka-API compatibility (so any standard Kafka tooling/client library works
unmodified) without running a ZooKeeper-dependent Kafka cluster, ClickHouse for its
column-store MergeTree engine being a proven fit for exactly this "ingest a firehose,
query rollups fast" shape.

## Consequences

**Positive:**

- Ingestion throughput and query latency scale independently — Redpanda partitions
  scale write throughput, ClickHouse shards/replicas scale query concurrency,
  neither has to be over-provisioned to compensate for the other.
- A ClickHouse maintenance window or query-side incident cannot back-pressure into
  running load tests; Redpanda simply retains the topic per its configured
  retention window until ClickHouse catches up.
- Materialized views mean `telemetry-query-api` never runs a raw aggregate query
  over the full `telemetry.raw.v1` table at request time — it reads pre-computed
  rollups, keeping P99-latency-of-P99-latency-queries low.

**Negative / trade-offs:**

- Two additional stateful systems to operate (beyond Postgres for control-plane
  state) — schema/topic evolution in `clickhouse-schema-migrations` and
  `redpanda-ingestion-pipeline` must be coordinated so a materialized view is never
  left reading a topic schema it doesn't expect.
- Materialized views trade query-time flexibility for query-time speed — a genuinely
  novel ad hoc aggregate not covered by an existing view requires either a schema
  migration or an accepted slow-path query directly against raw data, not
  something `telemetry-query-api` can serve instantly today.
- Requires OTel Collector to be the single standardization chokepoint for all
  telemetry shape (traces/metrics/logs) across every language in the execution
  fleet — a bug in collector config affects every worker type at once.
