# otel-collector-config

DaemonSet configuration for the OpenTelemetry Collector that standardizes traces, metrics, and
logs emitted by every `execution-fleet` worker and `orchestration-control-plane` service before
they land in Redpanda.

## Implements

- Feature manifest: **"Telemetry Ingestion: OpenTelemetry (OTel) Collector deployed as a
  DaemonSet"** — `collector-config.yaml` is the exact config mounted into that DaemonSet
  (`infrastructure/kubernetes` ships the DaemonSet manifest; `infrastructure/helm-charts`
  packages this file as a ConfigMap per environment overlay).
- ARCHITECTURE.md communication contract: *"Workers → telemetry lake: OTLP (gRPC) to the
  OpenTelemetry Collector DaemonSet; the collector batches and forwards to Redpanda's
  `telemetry.raw.v1` topic."*

## Why a DaemonSet, not a centralized deployment

Running one collector per node means every worker/process sends OTLP to a collector on its own
node (`localhost` or node-local Service) instead of crossing the cluster network for the very
first hop. This matters at execution-fleet scale — thousands of `protocol-load-worker-go` /
`protocol-load-worker-rust` pods generating telemetry concurrently with the load they're
generating — because it keeps the hot path off the same network path being load-tested.

## Pipeline stages

```
                 ┌──────────────┐        ┌────────────────┐        ┌──────────────────┐        ┌─────────────────────┐
OTLP gRPC :4317  │              │        │                │        │                   │        │                     │
OTLP HTTP :4318  │──receivers──▶│ memory │──▶ tail_sampling│──▶batch│──▶ kafka exporters │──▶ Redpanda topic          │
                 │  (otlp)      │ limiter│  (traces only)  │        │  (traces/metrics/logs) │  telemetry.raw.v1     │
                 └──────────────┘        └────────────────┘        └──────────────────┘        └─────────────────────┘
```

1. **Receivers (`otlp`)** — accept OTLP over gRPC (workers, backend services) and OTLP over
   HTTP (browser RUM from `workflow-canvas-frontend`, or any sidecar without a gRPC exporter).
2. **`memory_limiter`** — first processor in every pipeline; protects the per-node collector
   pod from being OOM-killed during a load-test traffic spike, before any other processing
   happens.
3. **`tail_sampling`** (traces pipeline only) — waits for a trace's spans to fully arrive, then
   applies, in order:
   - **`keep-all-errors`** — 100% sampling for any trace containing an `ERROR` status span.
   - **`keep-high-latency`** — 100% sampling for traces whose duration exceeds 750ms, so tail
     latency investigations always have a real trace to open, not just an aggregate number.
   - **`baseline-probabilistic-sample`** — everything else (healthy, fast) is kept at a flat
     5% rate. Enough volume for accurate percentile/error-rate rollups in ClickHouse without
     paying storage/compute for every healthy span.
4. **`batch`** — batches spans/metrics/logs to amortize Kafka producer overhead.
5. **Kafka exporters** — one exporter per signal type (`kafka/traces`, `kafka/metrics`,
   `kafka/logs`), all publishing to Redpanda's `telemetry.raw.v1` topic (see
   `redpanda-ingestion-pipeline/src/topic-definitions.ts` for the topic's message shape and
   partition-key strategy). Splitting exporters per signal — even though they share a topic —
   keeps each pipeline's batching/retry tuning independent, so a metrics spike cannot stall
   trace export.

## Local development

`docker-compose.yml` at the repo root mounts this file directly into the `otel-collector-daemon`
container and points it at `telemetry-ingestion-redpanda:9092`. No changes needed to run it
locally — `docker compose up -d otel-collector-daemon`.

## Changing sampling behavior

Tail-sampling thresholds (`750ms` latency, `5%` baseline rate) are read by
`StatisticalBaselineDeviationDetector` in `telemetry-query-api` when reasoning about whether an
observed rate change is a real regression or an artifact of the sampling rate itself — if you
change `sampling_percentage` here, update that service's baseline assumptions too.
