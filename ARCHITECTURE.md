# Architecture Reference

This document is the single source of truth for how the monorepo is laid out and *why*.
Every service, package, and folder name below is intentional — read this before adding new
code so naming stays consistent across the platform.

## Five Layers

1. **Orchestration Control Plane** (`services/orchestration-control-plane/`) — where humans
   design DAG test workflows, and where the platform's own state (test definitions, run
   history, RBAC, environment lifecycles, cost projections) lives. Nothing here executes
   load — it only plans, schedules, gates, and observes.
2. **Execution Fleet** (`services/execution-fleet/`) — stateless, horizontally scalable worker
   processes that actually generate traffic, traverse DAG/IPLD structures, inject chaos, run
   user-supplied Wasm logic, and shadow-replay production traffic. Workers never talk to each
   other directly; they only take orders from the control plane over the message bus and emit
   telemetry outward.
3. **Telemetry Lake** (`services/telemetry-lake/`) — the write-optimized ingestion path
   (Redpanda → OTel Collector → ClickHouse) and the read-optimized query path
   (`telemetry-query-api`) that dashboards, quality gates, anomaly detection, and root-cause
   correlation all read from.
4. **AI-Assisted QA** (`services/ai-assisted-qa/`) — analysis services that close feedback
   loops the other three layers can't close on their own: healing broken contract-derived test
   logic, mining production logs into new DAG workflows, and authorizing canary rollouts from
   differential benchmark data. These services *consume* the control plane's and telemetry
   lake's APIs; they don't own runtime state of their own beyond their own analysis artifacts.
5. **Distributed Systems Validation** (`services/distributed-systems-validation/`) —
   specialized verification workers that assert properties the generic execution fleet isn't
   shaped for: ACID/consistency guarantees under induced partition, and service-mesh/IAM/K8s
   policy drift ahead of a test run. `environment-lifecycle-manager` calls into this layer via
   its `POLICY_VERIFYING` lifecycle state before a load test is allowed to start.

Cross-cutting: `services/shared-libraries/` holds code with no owning service — the DAG domain
model (including the structural-diff type contract), Protobuf/gRPC contracts, and the
observability kit every other service imports rather than reimplementing.

## Why services are split this way

- **Independent scaling.** A load test might need 5,000 protocol workers but only 3
  scheduler replicas. Splitting by runtime characteristics (stateless/CPU-bound workers vs.
  stateful/low-QPS control plane) lets Kubernetes HPA scale each independently.
- **Independent failure domains.** A crashing chaos sidecar must never take down the
  scheduler that requested it. Workers and control plane communicate only through Redis
  pub/sub and Redpanda topics — no synchronous RPC that could cascade a failure backward.
- **Language matches the job, not convention.** Go/Rust for the workers because
  throughput and tail latency are the product; TypeScript for the control plane because
  developer velocity and type-shared contracts with the React canvas matter more there than
  raw speed.
- **Durability by design.** Long-running DAG test executions survive control-plane restarts
  because workflow state lives in Temporal, not in process memory. Load tests survive spot
  instance preemption because worker state is checkpointed to Redis before each load-phase
  transition (see `environment-lifecycle-manager` and `protocol-load-worker-go`).

## Communication contracts

- **Control plane → workers**: `dag.node.dispatch.v1` topic on Redpanda, payload defined by
  `shared-libraries/protocol-contracts/proto/dag_dispatch.proto`. Dispatch is idempotent —
  each message carries a `dispatch_idempotency_key` (pipelineRunId + nodeId + attemptGeneration)
  so a scheduler retry after a crash cannot double-run a node (see `dag-workflow-scheduler`'s
  `internal/dispatch/idempotent_dispatch_deduplicator.go`).
- **Workers → telemetry lake**: OTLP (gRPC) to the OpenTelemetry Collector DaemonSet; the
  collector batches and forwards to Redpanda's `telemetry.raw.v1` topic, which ClickHouse
  consumes via a Kafka-engine materialized view.
- **Telemetry lake → control plane (backpressure)**: when `redpanda-ingestion-pipeline`'s
  circuit breaker trips under ingestion overload, it publishes `telemetry.backpressure.v1`;
  `dag-workflow-scheduler` consumes it and throttles new dispatch until pressure clears — this
  closes the loop so the telemetry lake isn't the only thing protecting itself.
- **Chaos coordination**: `chaos.fault-injection-event.v1` carries induced-fault events (shape
  in `shared-libraries/protocol-contracts/proto/chaos_fault_injection.proto`), consumed by
  `distributed-systems-validation/consistency-verification-worker` to correlate reads against
  known partition windows. A separate `chaos.kill-switch.v1` topic feeds every
  `chaos-ebpf-sidecar` instance's `GlobalChaosCircuitBreaker` — publishing to it halts and
  rolls back *every* active fault injection platform-wide, independent of each experiment's
  own blast-radius cap.
- **Control plane ↔ frontend**: GraphQL over HTTP for CRUD + queries, WebSocket subscriptions
  for live run status (both served by `pipeline-api-gateway`).

## Directory map

```
distributed-testing-benchmarking-tool/
├── services/
│   ├── orchestration-control-plane/
│   │   ├── workflow-canvas-frontend/        Next.js + React Flow DAG builder & dashboards
│   │   ├── pipeline-api-gateway/            NestJS BFF/GraphQL: auth, RBAC, test CRUD,
│   │   │                                    policy gates, approval workflow, cost estimation
│   │   ├── dag-workflow-scheduler/          Go + Temporal: durable DAG run orchestration,
│   │   │                                    dynamic mid-run mutation, tenant-priority queues,
│   │   │                                    idempotent dispatch, DAG version diffing
│   │   ├── test-data-factory-service/       Synthetic/PII-masked relational data generator
│   │   ├── environment-lifecycle-manager/   Terraform/Crossplane provisioning + teardown,
│   │   │                                    POLICY_VERIFYING pre-flight gate
│   │   └── run-cost-estimator/              Pre-run $ cost + carbon-footprint projection
│   ├── execution-fleet/
│   │   ├── protocol-load-worker-go/         REST/gRPC/WebSocket/GraphQL concurrent client,
│   │   │                                    HTTP/2 multiplexed load shaping, traffic capture
│   │   ├── protocol-load-worker-rust/       tokio-based max-throughput TCP/QUIC saturation
│   │   ├── dag-ipld-benchmark-worker-rust/  CID resolution, crypto-overhead profiling
│   │   ├── chaos-ebpf-sidecar/              Privileged eBPF fault-injection agent,
│   │   │                                    hypothesis-guarded auto-abort, global kill switch
│   │   ├── wasm-payload-sandbox-runtime/    wasmtime host + scripting SDK for user pre/post
│   │   │                                    scripts, shareable snippet marketplace client
│   │   └── traffic-shadow-replay-worker/    Production traffic shadowing at N×, log-to-test
│   ├── telemetry-lake/
│   │   ├── otel-collector-config/           DaemonSet config standardizing traces/metrics/logs
│   │   ├── redpanda-ingestion-pipeline/     Topic/schema definitions, backpressure config +
│   │   │                                    upstream backpressure signal publishing
│   │   ├── clickhouse-schema-migrations/    DDL for raw + materialized tables, hot/cold tiering
│   │   └── telemetry-query-api/             Read API: P50/P99, error-rate, trace correlation,
│   │                                        streaming/seasonal anomaly detection, root-cause
│   │                                        correlation
│   ├── ai-assisted-qa/
│   │   ├── contract-healing-service/        Auto-heals JSONPath extraction after schema diffs
│   │   ├── log-to-test-generator-service/   Mines APM logs into candidate DAG pipelines
│   │   └── canary-rollout-authorizer/       Gates canary promotion from A/B benchmark deltas
│   ├── distributed-systems-validation/
│   │   ├── consistency-verification-worker/ ACID/consistency checks under induced partition
│   │   └── service-mesh-policy-verifier/    Istio/Linkerd + K8s/IAM drift verification
│   └── shared-libraries/
│       ├── dag-domain-model/                TS types: DAG nodes/edges/test definitions,
│       │                                    structural-diff type contract
│       ├── protocol-contracts/              .proto definitions shared by all services
│       └── observability-kit/               Shared OTel init, structured logging, tracing
├── infrastructure/
│   ├── kubernetes/                          Base manifests + Kustomize overlays per env
│   ├── terraform/                           Cloud account bootstrap (VPC, EKS/GKE, IAM)
│   ├── crossplane/                          Compositions for on-demand test namespaces
│   └── helm-charts/                         Packaged charts per service for GitOps deploy
├── tools/
│   └── testforge-cli/                       Headless CLI for GitOps-native pipeline runs
└── docs/
    ├── architecture-decision-records/       0001-0013, see ADR index below
    └── api-specifications/
```

## Advanced features & extensions (ADR-tracked)

Beyond the initial three-layer build, the following capabilities were added as a second pass.
Each has a corresponding ADR under `docs/architecture-decision-records/` — read the ADR for the
Context/Decision/Consequences reasoning, not just the bullet here:

| Capability | Owning service(s) | ADR |
|---|---|---|
| Dynamic mid-run DAG mutation, tenant-priority scheduling | `dag-workflow-scheduler` | 0004 |
| Policy-as-code gates (OPA/Rego), approval workflow, cost estimation | `pipeline-api-gateway`, `run-cost-estimator` | 0005 |
| HTTP/2 multiplexed load shaping, traffic capture, shadow replay | `protocol-load-worker-go`, `traffic-shadow-replay-worker` | 0006 |
| Hypothesis-guarded chaos auto-abort, global kill switch, scripting SDK | `chaos-ebpf-sidecar`, `wasm-payload-sandbox-runtime` | 0007 |
| Streaming/seasonal anomaly detection, root-cause correlation, retention tiering | `telemetry-query-api`, `clickhouse-schema-migrations` | 0008 |
| AI-assisted QA service group (contract healing, log-to-test, canary authorization) | `services/ai-assisted-qa/*` | 0009 |
| Distributed systems validation service group + `POLICY_VERIFYING` gate | `services/distributed-systems-validation/*`, `environment-lifecycle-manager` | 0010 |
| Idempotent dispatch, backpressure feedback loop, secret rotation, observability dogfooding | `dag-workflow-scheduler`, `redpanda-ingestion-pipeline`, `pipeline-api-gateway` | 0011 |
| Priority-queue-backed dispatch loop, event-driven approval-to-resume | `dag-workflow-scheduler`, `pipeline-api-gateway` | 0012 |
| OPA sidecar wired into the Helm chart, end-to-end ClickHouse hot/cold storage tiering (including local dev) | `pipeline-api-gateway` (Helm chart), `clickhouse-schema-migrations` | 0013 |

No further scope boundaries remain open from the second pass as of ADR 0012 and ADR 0013: the
scheduler's HTTP entrypoint is wired to dispatch through `WeightedFairShareQueue` (ADR 0012),
approving a held run auto-resumes it (ADR 0012), the OPA sidecar is wired into the Helm chart, and
ClickHouse's hot/cold storage tiering is implemented end to end including local dev (ADR 0013).

## Naming conventions

- Services are named `<domain>-<role>` (e.g. `dag-workflow-scheduler`, not `scheduler`) so the
  name alone tells a new engineer what it owns.
- Shared types live in `shared-libraries/dag-domain-model` and are imported — never copy-pasted
  — by both the frontend and the gateway, so DAG node/edge shapes cannot drift.
- Every service ships its own `README.md` mapping its code back to the feature-manifest items
  it implements, and a `SCALING.md` describing its scaling/failure characteristics.
