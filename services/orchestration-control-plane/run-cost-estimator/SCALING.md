# Scaling notes

`run-cost-estimator` is fully stateless: every request runs a pure computation
(`estimateCost`) over its own request body against two in-process static tables
(`CLOUD_INSTANCE_PRICING_CATALOG` and the carbon-estimation constants) — there is no database, no
cache, and no cross-request state of any kind. This means it scales exactly like a stateless
function-as-a-service workload: run any number of replicas behind a Kubernetes Service, drive a
Horizontal Pod Autoscaler off CPU or request latency, and add/remove replicas freely with zero
coordination concerns. A crashed or rescheduled pod loses nothing — the very next request to any
other replica produces an identical estimate for the identical input.

**Failure isolation for callers**: per `pipeline-api-gateway`'s `SCALING.md`, a
`run-cost-estimator` outage is designed to degrade gracefully for callers — `pipeline-run.service.ts`
catches and logs any failure from `CostEstimatorClientPort.estimateRunCost` rather than blocking
a run trigger on it. This service should therefore be tuned for low request latency (it is
already CPU-only, no I/O) over aggressive availability guarantees; a brief unavailability window
during a deploy only means some runs briefly launch without a cost estimate attached, not that
runs fail to start.

**Pricing/carbon-constant staleness**: `cloud-instance-pricing-catalog.ts` and
`carbon-footprint-estimator.ts` are both hand-maintained static tables (see their own NOTE
comments) — this is a data-freshness concern, not a scaling concern, but is worth flagging here
too: a production version of this service would refresh `CLOUD_INSTANCE_PRICING_CATALOG` from
each cloud provider's pricing API on a schedule (e.g. a daily cron job writing an updated catalog
file, redeployed like any other config change) rather than requiring a code change/redeploy every
time list prices shift.
