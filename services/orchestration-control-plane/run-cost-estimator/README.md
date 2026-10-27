# run-cost-estimator

Estimates the projected dollar cost and rough CO2 footprint of a pipeline run **before it
starts**, so `workflow-canvas-frontend` can show a budgeting signal before the user clicks "Run".
A new service in the Orchestration Control Plane layer, per `ARCHITECTURE.md`'s "language
matches the job" and "independent scaling" principles — this is a small, stateless, pure-function
service with no reason to share a deploy unit with `pipeline-api-gateway`.

## Feature-manifest mapping

### "Pre-run cost/carbon estimation"

- `src/estimation/pipeline-cost-estimator.ts` — `PipelineCostEstimator.estimate` walks a
  `PipelineDefinition`'s `LoadTestNode`s (the only node kind that consumes sustained
  execution-fleet compute), computes each one's worker-hours
  (`workerReplicaCount x durationSeconds / 3600`), and prices those hours against
  `cloud-instance-pricing-catalog.ts`'s static per-instance-type/per-provider table. Every other
  node kind is treated as effectively free relative to sustained load generation — a reasonable
  approximation for a pre-run budgeting signal, not a hard accounting guarantee.
- `src/estimation/cloud-instance-pricing-catalog.ts` — a real-looking static on-demand hourly
  pricing table for two instance-type families (`GENERAL_PURPOSE_MEDIUM`,
  `COMPUTE_OPTIMIZED_LARGE`) across AWS and GCP, matching the worker sizing split ADR 0002
  describes (general-purpose for `protocol-load-worker-go`, compute-optimized for
  `protocol-load-worker-rust`). Explicitly documented as illustrative list prices, not a live
  pricing-API feed — see that file's NOTE.
- `src/estimation/carbon-footprint-estimator.ts` — `estimateCarbonFootprint` derives a rough
  CO2-equivalent estimate from instance-hours x an average watts-per-vCPU figure x a datacenter
  PUE multiplier x a published global-average grid carbon-intensity figure
  (`AVERAGE_GRID_CARBON_INTENSITY_KG_PER_KWH`). Every constant used is documented as an
  approximation, not a certified carbon-accounting methodology — see that file's NOTE.
- `src/index.ts` — `estimateCost(pipelineDefinition, options)` is the single entry point tying
  the above together: dollar cost plus per-line-item carbon, summed into a
  `RunCostAndCarbonEstimate`.
- `src/http/cost-estimate-http-server.ts` — exposes `estimateCost` over `POST /cost-estimate`
  using Node's built-in `http` module (no framework — see that file's header for why), so
  `pipeline-api-gateway`'s `src/cost-estimation/cost-estimator-client.port.ts` /
  `cost-estimator-http-client.adapter.ts` has something real to call, matching the same
  hexagonal port/adapter pattern that gateway already uses for
  `telemetry-query-api-client.port.ts`.

## Who calls this

`pipeline-api-gateway`'s `pipeline-run.service.ts` calls this service (best-effort — an outage
here never blocks a run from starting) before triggering a run against `dag-workflow-scheduler`,
and persists the result on `PipelineRunEntity.estimatedCostUsd` /
`estimatedCarbonKgCo2e`. See that gateway's `README.md` for the caller-side wiring.

## Local development

```bash
npm install
npm run dev   # POST /cost-estimate on :8095 by default (PORT env var to override)
```

```bash
curl -X POST localhost:8095/cost-estimate \
  -H 'Content-Type: application/json' \
  -d '{"pipelineDefinition": { "pipelineId": "p1", "nodes": [...], "edges": [] }, "cloudProvider": "AWS"}'
```

## Layout

```
src/estimation/cloud-instance-pricing-catalog.ts   Static AWS/GCP instance-type pricing table
src/estimation/pipeline-cost-estimator.ts          $ cost from LoadTestNode worker-hours x pricing
src/estimation/carbon-footprint-estimator.ts       Rough CO2e estimate from instance-hours
src/index.ts                                       estimateCost(pipelineDefinition) entry point
src/http/cost-estimate-http-server.ts              POST /cost-estimate HTTP entrypoint
```
