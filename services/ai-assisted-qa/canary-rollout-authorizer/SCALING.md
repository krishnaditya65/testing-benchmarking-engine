# Scaling notes

`canary-rollout-authorizer` runs as a scheduled evaluation loop (e.g. every 1-5 minutes per
active canary rollout, triggered by a CronJob or a lightweight scheduler — not a
request-serving HTTP service with meaningful QPS of its own), so the scaling question is "how
many concurrently-active canary rollouts can one evaluation cycle handle", not "how many
replicas behind a load balancer".

**Statelessness is the load-bearing design choice.** `CanaryRolloutDecisionEngine.evaluate` is a
pure function — no internal state, no shared mutable counters. `consecutiveCleanWindowCount` is
threaded through as an explicit parameter/return value rather than kept in memory, so:

- Evaluating N canary rollouts concurrently is embarrassingly parallel — one `evaluate()` call
  per rollout, no locking, no coordination needed between them.
- A crashed/restarted evaluation process loses nothing beyond the in-flight cycle: whatever
  persists `consecutiveCleanWindowCount` between runs (a small key-value record per rollout,
  keyed by `serviceName` + `canaryVersionDagNodeId`) is external to this service, the same
  pattern `dag-workflow-scheduler` uses for workflow state (Temporal) instead of in-process
  memory (see ADR 0001).

**Where a real deployment needs care:**

- `CanaryDifferentialReportHttpClientAdapter` and `ArgoRolloutsWebhookAdapter` both make
  synchronous outbound HTTP calls per evaluation; a slow/unavailable `telemetry-query-api` or
  Argo Rollouts webhook receiver should time out (both adapters already carry a
  `requestTimeoutMs`, default 10s) rather than block an evaluation cycle indefinitely.
- `RolloutControlActionResult.succeeded === false` (a decision was computed correctly but the
  cluster-side action failed to apply) must be surfaced loudly — alerting, retry-with-backoff, or
  both — never silently dropped. A ROLLBACK decision that fails to actually reach Argo Rollouts is
  the single worst failure mode this service can produce: the audit trail says "we decided to
  roll back" while production traffic keeps hitting a regressed canary.
- Argo Rollouts/Flagger webhook rate limits (not this service's CPU/memory) are the realistic
  ceiling on how many rollouts can be evaluated per cycle at high concurrency; batching or
  staggering evaluation start times across rollouts avoids a thundering-herd of simultaneous
  webhook calls at the top of every cycle.
