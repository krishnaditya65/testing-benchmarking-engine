# Scaling notes

`log-to-test-generator-service` is a batch/offline workload, not a request-serving one — it runs
periodically (e.g. nightly, or triggered off a "crash rate spike" alert) rather than behind a
Service/Ingress, so "scaling" here means "scaling one run", not "adding replicas behind a load
balancer".

**Where the cost actually is:** `FrequentTrafficPathMiner.mineFrequentPaths` is
`O(totalEntries * sequenceLength)` — one pass per session building N-gram windows — which is
linear and cheap even at production log volumes. `CrashSequenceExtractor` is a single linear
pass per session. Neither is the bottleneck. The real cost is **fetching** the log window:
`ApmSessionLogSourcePort.fetchSessionLogEntries` pulls every raw request span in the window
before any mining happens, so a naive "mine the last 30 days across all services" call can pull
far more rows than the miner needs. Mitigations belong at the port/adapter boundary, not in the
mining logic:

- Always pass a `serviceName` filter when the target service is known — `ClickHouseApmSessionLogAdapter`'s
  query already supports it.
- Prefer multiple short-window runs (e.g. per-day) over one long-window run; `MinedTrafficPath`
  results across windows can be merged by summing `occurrenceCount`/unioning `exampleSessionIds`
  without re-scanning.
- `CrashSequenceExtractor` only needs enough lookback history per session to satisfy
  `lookbackLength` — a real ClickHouse adapter should push a `LIMIT`-per-session or a
  pre-filtered "sessions containing a 5xx" subquery rather than fetching full session history for
  every session in the window, most of which never crash.

**Failure isolation:** this service never mutates production state — it only reads telemetry and
produces candidate `PipelineDefinition` objects that a human/CI step reviews before persisting via
`pipeline-api-gateway`. A bad mining run in the worst case produces a low-value or malformed
candidate pipeline, never a corrupted one that silently reaches the execution fleet, since
`pipeline-api-gateway`'s `DagStructuralValidator` still gates every save.
