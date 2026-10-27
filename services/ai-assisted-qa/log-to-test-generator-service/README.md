# log-to-test-generator-service

Implements the feature-manifest bullet **"Log-to-Test Generation"** (section 7, AI-Assisted QA &
Analytics): ingests production APM/session telemetry and auto-generates DAG workflows recreating
(a) the most frequent production traffic paths, and (b) recent crash sequences — so a load test
or regression pipeline reflects what production traffic actually does, instead of only what a
human remembered to hand-author.

See `docs/architecture-decision-records/0009-ai-assisted-qa-service-group.md` for why this lives
under `services/ai-assisted-qa/` rather than folded into `orchestration-control-plane`.

## Pipeline

```
ApmSessionLogSourcePort            (ingestion/apm-session-log-ingester.ts)
        │  ApmSessionLogEntry[] (sessionId, requestPath, method, status, timing)
        ▼
FrequentTrafficPathMiner  ─┐       (pathmining/frequent-traffic-path-miner.ts)
CrashSequenceExtractor    ─┤       (pathmining/crash-sequence-extractor.ts)
        │  MinedTrafficPath[] / CrashSequence[]
        ▼
TrafficPathToDagWorkflowGenerator  (generation/traffic-path-to-dag-workflow-generator.ts)
        │  PipelineDefinition (@shared-libraries/dag-domain-model)
        ▼
  pipeline-api-gateway's createPipelineDefinition mutation (caller's responsibility — this
  service produces candidates, it does not persist them itself)
```

## ApmSessionLogSourcePort / ClickHouseApmSessionLogAdapter

Modeled as reading from the telemetry lake's ClickHouse instance — the same physical store
`telemetry-lake/telemetry-query-api` reads (`raw_telemetry_events`, per
`clickhouse-schema-migrations` migration 001) — but through a port scoped to exactly the fields
path-mining needs (session id, path, method, status, timing), not that service's full
percentile/error-rate surface. `ClickHouseApmSessionLogAdapter`'s query is real SQL against that
table shape but takes an injected, narrowly-typed `ClickHouseApmLogClient` rather than a live
`@clickhouse/client` instance, since this service doesn't carry that dependency; wiring a real
client is a deployment-time concern (mirrors `telemetry-query-api/src/repository/click-house-query-repository.ts`'s
shape). `InMemoryApmSessionLogAdapter` satisfies the same port from a plain array and is what the
unit tests below use.

## FrequentTrafficPathMiner

Real sequential pattern mining: groups entries by `sessionId`, orders each session
chronologically (by `occurredAtEpochMs`, not log arrival order), and slides a length-N window
(default `sequenceLength: 3`) across every session's request sequence, counting how often each
exact ordered N-gram of `{httpMethod, requestPath}` occurs across *all* sessions. Returns the
top-K (default 5) most frequent N-grams as `MinedTrafficPath`s, each carrying both a raw
occurrence count and a `distinctSessionCount` — the latter matters because "one session hit the
same 3 requests 500 times" and "500 different sessions each did those 3 requests once" are very
different signals about how common a path really is in the user base, even though both count
as 500 occurrences.

## CrashSequenceExtractor

Finds every request with a 5xx `statusCode` (configurable via `crashStatusCodeThreshold`,
default 500) and captures the `lookbackLength` (default 4) requests immediately preceding it —
including the crashing request itself as the final step — per session, per crash occurrence (a
session that crashes twice yields two independent `CrashSequence`s). Sessions with fewer
preceding requests than the lookback window still produce a (shorter) sequence rather than being
dropped, so a crash on literally the first request of a session is still captured.

## TrafficPathToDagWorkflowGenerator

Converts either output into a `PipelineDefinition` (the canonical shape from
`@shared-libraries/dag-domain-model`, imported — not redeclared — per that package's "single
source of truth" contract):

- `generateFromTrafficPath`: one `ApiRequestNode` per mined step, chained by `ON_SUCCESS`
  `PipelineEdge`s.
- `generateFromCrashSequence`: the same `ApiRequestNode` chain, plus a trailing, blocking
  `AssertionNode` (`metricPath: "response.statusCode"`, `operator: "EQUALS"`,
  `expectedValue: <the observed crash status code>`) wired with an `ALWAYS` edge from the final
  request node — so *running* the generated pipeline is a genuine regression-reproduction
  check, not just a traffic replay with no pass/fail signal.

Generated `PipelineDefinition`s are candidates only: this service does not call
`pipeline-api-gateway` itself. A caller (CI job, scheduled reconciliation script) is expected to
review/POST the result to that gateway's `createPipelineDefinition` mutation, where
`DagStructuralValidator` remains the authoritative save-time gate.

## Local development

```bash
npm install
npm test    # vitest run — path miner, crash extractor, DAG generator, synthetic log fixtures
npm run build
```

`vitest.config.ts` aliases `@shared-libraries/dag-domain-model` directly to that package's
`src/index.ts` so tests run without depending on the root workspace reconciliation having landed
yet; `package.json` still declares the real `workspace:*` dependency for the eventual monorepo
build.
