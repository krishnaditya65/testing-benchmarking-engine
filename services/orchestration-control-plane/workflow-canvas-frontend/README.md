# workflow-canvas-frontend

Frontend of the Orchestration Control Plane. Humans design DAG test workflows
here; nothing in this service executes load — it only plans, schedules
(via authoring), and observes.

This service talks to the sibling `pipeline-api-gateway` (NestJS BFF/GraphQL
server) exclusively over the communication contract in ARCHITECTURE.md:
GraphQL over HTTP for CRUD/queries, WebSocket (`graphql-ws`) subscriptions
for live run status. Both endpoints are configured via
`NEXT_PUBLIC_PIPELINE_API_GATEWAY_URL` (default
`http://localhost:4000/graphql`; the ws:// subscription URL is derived from
it automatically — see `src/lib/graphql-client.ts`).

## Feature manifest mapping

- **"Visual DAG pipeline builder"** — `src/features/dag-canvas/DagPipelineCanvas.tsx`
  is a React Flow canvas with drag-and-drop node placement
  (`PipelineNodePalette.tsx`), connection wiring (`onConnect`), and
  save-to-gateway (`savePipelineDefinition`). Canvas state is decoupled into
  `dag-canvas-store.ts` (zustand) so it isn't tangled with React Flow's own
  hooks.

- **"Multi-protocol pipelining"** — modeled as distinct, purpose-built step
  types under `src/features/dag-canvas/node-types/`: `ApiRequestNode` (HTTP),
  `LoadTestNode` (load generation), `DataSeedNode` (data prep),
  `AssertionNode` (contract validation), `ChaosInjectionNode` (fault
  injection), and `EnvironmentTeardownNode` (cleanup). Each has its own typed
  `data` shape reflecting real fields for that step (e.g. `LoadTestNodeData`
  has `virtualUsers`/`durationSeconds`/`rampUpSeconds`;
  `ChaosInjectionNodeData` has `faultType`/`blastRadiusPercent`/`targetService`).
  They're registered together in `node-types/index.ts`'s
  `pipelineNodeTypeRegistry` for React Flow's `nodeTypes` prop.

- **"Dynamic variable extraction"** — `AssertionNode.tsx`'s
  `AssertionNodeData.jsonPathExpression` extracts a value out of an upstream
  step's response; `extractAsVariableName` optionally binds it for downstream
  steps to reference. `comparisonOperator`/`expectedValue` express the
  automated contract-validation check itself.

- **"Mathematical DAG execution with conditional branching"** — the actual
  branch evaluation happens in the scheduler (out of scope for this
  frontend), but it must be *authorable* here: `dag-edge-types.ts` defines
  `PipelineEdgeCondition` (`always` / `onParentAssertionPassed` /
  `onParentAssertionFailed`) as `PipelineEdgeData.condition`, attached to
  every edge created via `onConnect` in the canvas store. `AssertionNode`
  even exposes two separate source handles (`onAssertionPassed` /
  `onAssertionFailed`) so branch wiring is visually explicit.

- **Live run observability** — `src/features/live-run-monitor/useLiveDagRunStatus.ts`
  opens a `pipelineRunStatusUpdated(runId: ID!)` GraphQL subscription via
  `graphql-ws` against pipeline-api-gateway, with real subscribe/unsubscribe
  lifecycle management and reconnect-safe cleanup in `useEffect`.
  `TelemetryCorrelationDashboard.tsx` consumes the hook and renders per-node
  status (pending/running/passed/failed) correlated with `p99LatencyMs` and
  `errorRatePercent` telemetry, highlighting anomalous nodes.

## Local development

```bash
npm install
NEXT_PUBLIC_PIPELINE_API_GATEWAY_URL=http://localhost:4000/graphql npm run dev
```

Routes:
- `/` — pipeline list (fetched via `listPipelineSummaries`)
- `/pipelines/[pipelineId]` — the DAG canvas editor for one pipeline

## Scripts

- `npm run dev` — Next.js dev server
- `npm run build` — production build
- `npm run start` — run the production build
- `npm run lint` — eslint (eslint-config-next)
- `npm run typecheck` — `tsc --noEmit`
