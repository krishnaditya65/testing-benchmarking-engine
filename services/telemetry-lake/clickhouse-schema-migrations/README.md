# clickhouse-schema-migrations

Numbered DDL migrations for the ClickHouse instance backing `telemetry-query-api`. Applied
in order; each file is idempotent (`CREATE ... IF NOT EXISTS`) so re-running the full sequence
against a fresh database is safe.

## Implements

- ARCHITECTURE.md: *"the write-optimized ingestion path (Redpanda → OTel Collector →
  ClickHouse) ... which ClickHouse consumes via a Kafka-engine materialized view."*
- Feature manifest: DDL for raw + materialized aggregate tables backing P50/P99 latency and
  error-rate queries.

## Local development

`docker-compose.yml` mounts this entire directory into `telemetry-metrics-clickhouse`'s
`/docker-entrypoint-initdb.d/`, so ClickHouse applies every `.sql` file here, in filename
order, on first container start. It also mounts `server-config/storage-policy.local-dev.xml`
into `/etc/clickhouse-server/config.d/`, and a second bind-mounted volume
(`.docker-volumes/clickhouse-cold-tier`) for that policy's local-disk cold tier — see
"Retention tiering (005)" below for why this is no longer single-disk.

## Server config

`server-config/` holds the ClickHouse server config XML fragments that migration 005's
`storage_policy` reference depends on (SQL cannot create storage policies, only reference an
already-declared one):

| File | Used by | Cold disk |
|---|---|---|
| `storage-policy.xml` | `infrastructure/kubernetes/base/telemetry-clickhouse-statefulset.yaml` (staging + production, via ConfigMap) | S3 (`cold_tier_s3`) |
| `storage-policy.local-dev.xml` | `docker-compose.yml`'s `telemetry-metrics-clickhouse` | Local bind-mounted directory (`cold_tier_local`) |

Both declare the same `hot_to_cold_tiered` policy name and `hot`/`cold` volume names that
migration 005's `ALTER TABLE` statements reference — `storage-policy.xml` is authoritative for
that naming (ClickHouse validates config XML at startup, this SQL file only references it), so
if the policy is ever renamed, change the XML first and update the SQL to match.

## Migrations

| File | Purpose |
|---|---|
| `001_raw_telemetry_events_table.sql` | Durable `MergeTree` landing table for all consumed telemetry, partitioned daily, ordered by `(service_name, pipeline_run_id, dag_node_id, event_time)`. |
| `002_kafka_engine_ingestion_table.sql` | `Kafka`-engine table consuming `telemetry.raw.v1` directly, plus the materialized view draining it into (001). |
| `003_p99_latency_materialized_view.sql` | `AggregatingMergeTree` rollup of `quantiles(0.5, 0.9, 0.95, 0.99, 0.999)` latency per `(service_name, dag_node_id, minute)`. |
| `004_error_rate_rollup_table.sql` | `SummingMergeTree` rollup of total/error counts per `(service_name, dag_node_id, minute)`. |
| `005_hot_tier_ttl_and_cold_tier_storage_policy.sql` | Two-stage `TTL ... TO VOLUME ... DELETE` on `raw_telemetry_events`: hot local disk for 7 days, then a cold volume (S3 in staging/production, local disk in dev) until the existing 30-day deletion. See "Retention tiering" below. |

## Raw → materialized aggregation strategy

Three tiers, each answering a different question at a different cost:

1. **Raw (`raw_telemetry_events`, 001)** — full-fidelity, one row per span/metric/log. Answers
   "show me this exact trace" or any ad-hoc query, but is too large to scan for dashboard-grade
   interactive queries once the platform is under sustained load (billions of rows/day at
   design-target traffic).
2. **Kafka queue (`raw_telemetry_events_kafka_queue`, 002)** — not really a tier, just the
   consumer-group handle ClickHouse uses to pull off Redpanda; never queried by application
   code, only drained by its materialized view into (1).
3. **Rollups (`latency_percentile_rollup` 003, `error_rate_rollup` 004)** — pre-aggregated per
   minute per `(service_name, dag_node_id)`. `AggregatingMergeTree`/`SummingMergeTree` let
   ClickHouse merge partial states across parts cheaply, and let query-time `quantilesMerge()`
   combine minute buckets into whatever window the caller asked for (last 5 minutes, last 24
   hours, etc.) without ever re-touching raw rows. This is what makes
   `getPercentileLatency`/`getErrorRateRollup` in `telemetry-query-api` fast enough for
   interactive dashboards and CI quality-gate checks.

New rollups (e.g. a future per-DAG-node throughput table) should follow the same pattern:
numbered migration, `AggregatingMergeTree`/`SummingMergeTree` engine, materialized view sourced
from `raw_telemetry_events`, partitioned by day, ordered with the most-filtered column first.

## Cardinality and retention notes

- `trace_id`/`span_id` are deliberately **not** part of any `ORDER BY` — they're accessed via a
  bloom-filter skip index on the raw table instead, since including them in the primary key
  would fragment the ordering every other query pattern relies on.
- Raw events TTL at 30 days; rollups TTL at 90 days, matching how much history quality gates
  and the statistical baseline detector need without keeping raw high-cardinality data around
  indefinitely.

## Retention tiering (005)

`raw_telemetry_events` (001) is the platform's largest table by a wide margin (one row per span/
metric/log, versus one row per service/dag-node/minute for the rollups), and its access pattern
is heavily skewed toward recency: `telemetry-query-api`'s trace-correlation lookups and
`IncidentRootCauseCorrelator`'s event-window queries (`src/correlation/`) almost always target an
active pipeline run or a fresh incident, not week-old history. Keeping the full 30-day retention
window on the same expensive, latency-optimized disk the whole time over-pays for storage on data
that's rarely read.

`005_hot_tier_ttl_and_cold_tier_storage_policy.sql` splits that 30-day window into two disks under
one `storage_policy`, using ClickHouse's native `TTL ... TO VOLUME` mechanism rather than a
separate archival table or an application-level migration job:

| Tier | Age | Disk | Why |
|---|---|---|---|
| Hot | 0-7 days | local SSD/NVMe (`default`) | Lowest latency, no network hop — this is what keeps interactive dashboards, quality gates, and fresh-incident correlation fast. |
| Cold | 7-30 days | S3-backed (`cold_tier_s3`) | ~5-10x cheaper than block storage; acceptable because reads against this age range are infrequent, ad-hoc historical/postmortem queries, not the interactive hot path. |
| Deleted | 30+ days | — | Unchanged from (001)'s original DELETE TTL; this migration only changes *where* a row lives for days 7-30, not total retention. |

The `hot_to_cold_tiered` storage policy must be declared in ClickHouse server config XML before
this migration's `ALTER TABLE ... MODIFY SETTING storage_policy` can succeed (SQL cannot create
storage policies, only reference them) — see "Server config" above for the two XML fragments
that now provide this end-to-end, in every environment:

| Environment | Config source | Cold tier backing store |
|---|---|---|
| Local dev | `server-config/storage-policy.local-dev.xml`, mounted by `docker-compose.yml` | Second bind-mounted local directory (`.docker-volumes/clickhouse-cold-tier`) |
| Staging | `server-config/storage-policy.xml`, mounted via `infrastructure/kubernetes/base/telemetry-clickhouse-statefulset.yaml`'s ConfigMap | S3 (base's default, no extra PVC) |
| Production | Same base ConfigMap, plus `infrastructure/kubernetes/overlays/production/clickhouse-cold-tier-volume-patch.yaml` adds an on-cluster cold-tier PVC as an alternative for clusters/cloud accounts without S3 access | S3 by default; on-cluster PVC where the patch's alternative is used |

Local dev is **no longer single-disk**: it now runs the same `hot_to_cold_tiered` policy and
`TTL ... TO VOLUME` mechanism as staging/production, just backed by a second local disk instead
of S3 — cheaper and simpler to run locally, but exercising the identical tiering mechanism, so
this migration succeeds uniformly across every environment instead of being a documented
production/staging-only step that no-op-fails locally.
