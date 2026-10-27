# docs/api-specifications

Contract definitions for the platform's externally-consumed APIs. These are the
source of truth `pipeline-api-gateway`'s implementation and `testforge-cli`'s client
code are both expected to match — if they drift, this directory is wrong and should
be updated alongside the code, not the other way around.

| File | Protocol | Consumers |
|---|---|---|
| [`pipeline-api-gateway.graphql`](./pipeline-api-gateway.graphql) | GraphQL SDL (Query/Mutation/Subscription) | `workflow-canvas-frontend` (CRUD, live run status via subscriptions) |
| [`openapi/testforge-cli-submission-endpoint.yaml`](./openapi/testforge-cli-submission-endpoint.yaml) | OpenAPI 3.0 (REST) | `tools/testforge-cli` (`testforge run`'s submit + poll calls) |

## Why both a GraphQL schema and a REST excerpt for the same gateway

`pipeline-api-gateway` serves both. GraphQL is the primary interface —
`workflow-canvas-frontend` needs typed queries, mutations, and live subscriptions for
the DAG canvas, and GraphQL's single endpoint + schema introspection fits a rich
frontend client well. `tools/testforge-cli` deliberately uses a narrow REST surface
instead: CI runners (GitHub Actions, GitLab CI, Jenkins agents) benefit from a
dependency-light HTTP client (Go's `net/http`, no GraphQL client library, no
generated query documents to keep in sync) and a stable two-endpoint contract that
changes far less often than the full GraphQL schema does. The REST excerpt is
intentionally minimal — submit a run, poll a run — because that's the entirety of
what `testforge run` needs.

## Related contracts not duplicated here

- `shared-libraries/protocol-contracts/proto/dag_dispatch.proto` — the
  control-plane-to-worker dispatch contract (`dag.node.dispatch.v1` topic), not a
  public/external API and therefore not part of this directory.
- `shared-libraries/dag-domain-model` — the canonical TypeScript types for DAG
  nodes/edges/test definitions that both the GraphQL schema's `DAGNodeType` enum and
  the OpenAPI schema's node `type` enum are hand-kept consistent with.

## Change process

Update the spec file first, then the implementing service, then run the service's
own contract tests against the updated spec (see each service's `README.md`). A
schema change that isn't reflected here first is treated as undocumented, even if
merged.
