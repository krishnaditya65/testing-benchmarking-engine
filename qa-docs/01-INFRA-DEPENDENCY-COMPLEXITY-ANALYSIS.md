# distributed-testing-benchmarking-tool (TestForge) — Infra, Dependency & Complexity Analysis

Audience: LLM agents writing/running tests. Nothing here has been executed.

## 1. What it is
A platform that itself runs distributed load / chaos / validation pipelines (DAG of test stages) against target systems. Polyglot pnpm workspace: TypeScript 153 files / 13.8k LOC, Go 61 files / 11.9k LOC, Rust 32 files / 3.8k LOC, TSX 12 files, 5 ClickHouse SQL migrations, 3 protos. IaC: Terraform (`infrastructure/terraform/{modules/cloud-account-bootstrap,environments/{staging,production}}`), Crossplane compositions, Kustomize (`kubernetes/{base,overlays}`), Helm charts (pipeline-api-gateway, dag-workflow-scheduler, telemetry-query-api, protocol-load-worker-go). 13 ADRs in `docs/architecture-decision-records/`, API specs in `docs/api-specifications/{openapi,pipeline-api-gateway.graphql}`, `ARCHITECTURE.md`. 20 existing test files.

**Testing a testing tool has a special twist**: the product's measurements are its output. Accuracy of generated load, latency statistics, chaos blast radius and verdicts must be validated against ground truth (a calibrated target), not just "does it run".

## 2. Service groups (`services/`)
| Group | Service | Stack | Role |
|---|---|---|---|
| orchestration-control-plane | `pipeline-api-gateway` | NestJS, Apollo GraphQL + subscriptions (graphql-ws), TypeORM/pg, JWT, AWS Secrets Manager client, event-emitter; port 4000 | API: pipelines, runs, approvals, policy gates |
| | `dag-workflow-scheduler` | Go, Temporal SDK, kafka-go; HTTP `/pipeline-runs`; `internal/{scheduling,dispatch}` | executes pipeline DAG as Temporal workflow; priority-queue dispatch; dynamic DAG mutation; multi-tenant scheduling; approval resume (ADR 0001/0004/0012) |
| | `environment-lifecycle-manager` | TS, ioredis | ephemeral target environments: create / TTL / teardown |
| | `run-cost-estimator` | TS | pre-run cost estimate; policy gate (ADR 0005) |
| | `test-data-factory-service` | TS, faker, pg, mongodb | synthetic data seeding |
| | `workflow-canvas-frontend` | Next.js, reactflow, graphql-request/ws, zustand | DAG editor & live run view |
| execution-fleet | `protocol-load-worker-go` | Go, gorilla/websocket, grpc | HTTP/WS/gRPC load generation; connection-reuse topology (ADR 0006) |
| | `protocol-load-worker-rust` | Rust, tokio, quinn (QUIC), rustls | high-throughput / HTTP3-QUIC load |
| | `dag-ipld-benchmark-worker-rust` | Rust, sha2, ed25519-dalek, multihash | content-addressed DAG benchmark |
| | `wasm-payload-sandbox-runtime` | Rust, wasmtime + WASI, wasmparser | user-supplied payload generators in sandbox |
| | `traffic-shadow-replay-worker` | Go | record/replay production traffic (ADR 0006) |
| | `chaos-ebpf-sidecar` | Go, cilium/ebpf, client-go, OTel | fault injection (latency/drop/kill) with hypothesis guard + global kill switch (ADR 0007) |
| telemetry-lake | `redpanda-ingestion-pipeline` | TS, kafkajs | telemetry events → ClickHouse |
| | `clickhouse-schema-migrations` | SQL + `server-config` | storage tiering (ADR 0013) |
| | `otel-collector-config` | YAML | collector pipelines |
| | `telemetry-query-api` | NestJS, @clickhouse/client; port 4500; `telemetry`, `telemetry/root-cause-correlation` | results, anomaly detection, RCA (ADR 0008) |
| ai-assisted-qa | `log-to-test-generator-service` (pathmining, generation), `contract-healing-service` (diffing, healing), `canary-rollout-authorizer` (authorization) | TS (vitest) | ADR 0009 |
| distributed-systems-validation | `consistency-verification-worker` (Go), `service-mesh-policy-verifier` (TS, yaml) | | linearizability/consistency checking of targets; mesh policy verification (ADR 0010) |
| shared-libraries | `dag-domain-model` (TS), `protocol-contracts` (proto: `dag_dispatch`, `telemetry_event`, `chaos_fault_injection`), `observability-kit` (OTel) | | |
| tools | `testforge-cli` (Go, cobra, yaml.v3): `run`, `validate`; `internal/{pipelineconfig,cicdintegration}`; `examples/pipelines/checkout-load-test.yaml` | | CI entry point |

## 3. Infra (`docker-compose.yml`)
Postgres 16 (5432, test definitions) · Redis 7 (6379, coordinator state) · Temporal auto-setup 1.24 (7233) · Redpanda v24.2.4 (9092) · ClickHouse 24.8 (8123/9000) · OTel Collector contrib 0.108 (4317/4318) · Vault 1.17 (8200, secret injection). Root scripts: `build`, `lint`, `test`, `dev:control-plane`, `dev:frontend`.

## 4. Existing tests
gateway `__tests__` (3), env-lifecycle `__tests__` (3), telemetry-query analytics `__tests__` (2), scheduler `internal/{scheduling(2),dispatch(2)}`, mesh verifier `__tests__` (2), log-to-test `pathmining(2), generation(1)`, contract-healing `healing(1), diffing(1)`, canary authorizer (1). **No tests** for: both Rust load workers, wasm sandbox, IPLD worker, chaos sidecar, shadow replay, consistency worker, ingestion pipeline, CLI, IaC.

## 5. Complexity & risk hot-spots
| # | Area | Risk |
|---|---|---|
| 1 | Chaos sidecar (eBPF, k8s) | fault injected outside intended blast radius; **fault not removed** after run/crash (must be self-healing: TTL, kill switch); kernel compat; privilege |
| 2 | Load-generation accuracy | coordinated omission in latency stats; open vs closed model; actual RPS ≠ target RPS; client-side saturation misreported as target latency; clock source; histogram precision; connection reuse semantics |
| 3 | DAG scheduler on Temporal | determinism; dynamic DAG mutation mid-run; priority inversion/starvation across tenants; approval signal races; cancellation propagation to workers; idempotent dispatch over Kafka (at-least-once) |
| 4 | WASM sandbox | escape (FS/network/WASI caps), CPU/memory/fuel limits, infinite loops, malicious modules (parser bombs) |
| 5 | Shadow replay | replaying production traffic to wrong target; PII in recordings; non-idempotent requests replayed against prod |
| 6 | Policy gates & cost estimator | gate bypass; mis-estimate → runaway spend; OPA sidecar wiring (ADR 0013) |
| 7 | Environment lifecycle | leaked environments (cost), teardown of the wrong env, TTL races, Redis state loss |
| 8 | Telemetry lake | at-least-once → duplicates skewing percentiles; late data; ClickHouse TTL/tiering dropping data needed for reports; multi-tenant query isolation |
| 9 | Anomaly/RCA & AI-QA | nondeterminism, false verdicts; canary authorizer wrongly approving a bad rollout (safety-critical decision) |
| 10 | Consistency verifier | checker soundness (must flag known-bad histories, pass known-good), complexity blow-up on long histories |
| 11 | Secrets | Vault / AWS SM injection into workers; leakage into telemetry, logs, test artefacts |
| 12 | Kill switch | global stop must halt load + chaos fleet-wide within seconds even when control plane is degraded |
| 13 | IaC | prod/staging drift; destructive plan; Helm values/RBAC for privileged sidecar |
| 14 | CLI | YAML schema validation, exit codes for CI gating |

## 6. Invariants
1. **Safety**: no load or fault is ever applied to a target not explicitly in the run's allow-list; all faults are removed when the run ends, is cancelled, times out, or the injector crashes.
2. **Kill switch**: after activation, zero new requests/faults within T seconds, fleet-wide.
3. **Run state machine**: every run terminates (SUCCEEDED/FAILED/CANCELLED/TIMED_OUT); each DAG node executes once per attempt; children only after parents succeed; gates (approval/policy/cost) cannot be skipped.
4. **Measurement truth**: reported request count, error count, RPS and latency percentiles match ground truth from the calibrated target within stated tolerance (e.g. count exact, p99 ±5 %).
5. **Telemetry**: each event counted exactly once in results.
6. **Tenant isolation**: pipelines, runs, secrets, telemetry of tenant A invisible to B; scheduler fairness bounds per tenant.
7. **Sandbox**: WASM payload code cannot read host FS, open sockets, exceed fuel/memory limits.
8. **No leaked resources**: ephemeral environments, k8s jobs, connections are reclaimed after every run outcome.

## 7. Caveat
GraphQL operations, `/pipeline-runs` body, pipeline YAML schema, Kafka topics, ClickHouse tables, worker CLI flags were not read. Sources of truth: `docs/api-specifications/`, `tools/testforge-cli/examples/pipelines/checkout-load-test.yaml`, `services/shared-libraries/{dag-domain-model,protocol-contracts}`, each worker's `clap`/`flag` definitions.
