# Distributed Testing & Benchmarking Tool

An enterprise-grade, server-side distributed testing and benchmarking platform: visually
design DAG test workflows, run functional API tests across REST/gRPC/WebSocket/GraphQL/MQTT,
generate massive distributed load, inject chaos, and correlate every result against live APM
telemetry — all from one control plane.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full layer breakdown, service map, and naming
conventions. Read it before touching anything — it explains *why* the repo is split the way it
is, not just what's in it.

## Layers at a glance

| Layer | Lives in | Responsibility |
|---|---|---|
| Orchestration Control Plane | `services/orchestration-control-plane/` | DAG design, scheduling, RBAC, environment lifecycle, policy gates, cost estimation |
| Execution Fleet | `services/execution-fleet/` | Stateless workers: load generation, DAG/IPLD benchmarking, chaos, Wasm sandboxing, traffic shadowing |
| Telemetry Lake | `services/telemetry-lake/` | Ingest, store, and query traces/metrics/logs at scale; anomaly detection & root-cause correlation |
| AI-Assisted QA | `services/ai-assisted-qa/` | Contract healing, log-to-test generation, canary rollout authorization |
| Distributed Systems Validation | `services/distributed-systems-validation/` | Consistency benchmarking under partition, service-mesh & infra drift verification |
| Shared Libraries | `services/shared-libraries/` | Cross-service contracts and domain types |
| Infrastructure | `infrastructure/` | Kubernetes, Terraform, Crossplane, Helm |
| Tooling | `tools/testforge-cli/` | Headless CLI for CI/CD & GitOps pipelines |

See [ARCHITECTURE.md](./ARCHITECTURE.md)'s "Advanced features & extensions" table for the full
list of second-pass capabilities (dynamic DAG mutation, hypothesis-guarded chaos, streaming
anomaly detection, idempotent dispatch, etc.) and the ADR backing each one.

## Local development

```bash
# Install JS/TS workspace dependencies (control plane, frontend, shared libs, CLI)
pnpm install

# Bring up local dependencies: Postgres, Redis, Redpanda, ClickHouse, OTel Collector
docker compose up -d

# Run the control plane API gateway + frontend in dev mode
pnpm --filter pipeline-api-gateway dev
pnpm --filter workflow-canvas-frontend dev
```

Go and Rust services build independently — see each service's own `README.md` for its build
and run instructions (`go run ./...`, `cargo run`, etc.).

## Repository layout

```
distributed-testing-benchmarking-tool/
├── services/
│   ├── orchestration-control-plane/
│   ├── execution-fleet/
│   ├── telemetry-lake/
│   ├── ai-assisted-qa/
│   ├── distributed-systems-validation/
│   └── shared-libraries/
├── infrastructure/
├── tools/
└── docs/
```

## Feature manifest → code map

Every feature in the original product spec is traceable to a service. See each service's
`README.md` for the "Implements" section mapping code to feature-manifest bullet points, and
`docs/api-specifications/` for the GraphQL/OpenAPI/Protobuf contracts backing them.
