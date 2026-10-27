# TestForge — Complete Test Scenario Catalogue

ID `TF-<TIER>-NNN`. GW = pipeline-api-gateway :4000 (GraphQL), SCH = dag-workflow-scheduler, TQ = telemetry-query-api :4500, ENV = environment-lifecycle-manager, CAL = calibrated target (see scripts: a mock server with known latency/error behaviour and its own ground-truth counters). Invariants I1–I8: analysis §6.

## 1. Smoke
SMK-001 infra healthy: Postgres, Redis, Temporal, Redpanda, ClickHouse (migrations applied), OTel collector, Vault unsealed · SMK-002 GW GraphQL `{__typename}`; SCH health; TQ health · SMK-003 `testforge validate examples/pipelines/checkout-load-test.yaml` exit 0 · SMK-004 `testforge run` a 1-stage 10-s 50-rps HTTP load against CAL → run SUCCEEDED → results in TQ with non-zero request count · SMK-005 canvas frontend loads and shows the run · SMK-006 each worker binary `--help` exits 0 (Go ×4, Rust ×3).

## 2. Functional
| ID | Component | Scenario | Exp |
|---|---|---|---|
| FUN-001 | GW | pipeline CRUD, versioning; start/cancel run; list runs; subscription pushes node status changes in order | per schema in `docs/api-specifications` |
| FUN-002 | SCH | DAG: linear, fan-out/fan-in, conditional, retries per node, timeouts, `continue-on-failure` | I3 |
| FUN-003 | SCH | Dynamic DAG mutation mid-run (ADR 0004): add node after running node; invalid mutation rejected | consistent |
| FUN-004 | SCH | Priority queue dispatch (ADR 0012): high priority overtakes queued low; aging prevents starvation; per-tenant concurrency caps | ordering asserted |
| FUN-005 | SCH/GW | Approval gate: run pauses → approve → resumes (event-driven); reject → FAILED; timeout | no skip |
| FUN-006 | Cost estimator / policy | estimate ≈ formula; over-budget run blocked by policy gate (OPA); override requires role | I3 |
| FUN-007 | ENV | create env → READY → run uses it → teardown at end; TTL expiry reaps | I8 |
| FUN-008 | Test data factory | seed Postgres/Mongo with N records, deterministic by seed; cleanup | counts exact; same seed ⇒ same data |
| FUN-009 | Load worker Go | HTTP/1.1, HTTP/2, WebSocket, gRPC; constant rate, ramp, step; headers/body templating; connection reuse on/off | I4 |
| FUN-010 | Load worker Rust | HTTP/3 (QUIC), TLS options; high RPS | I4 |
| FUN-011 | WASM sandbox | user module generates payloads; deterministic with seed; limits enforced | I7 |
| FUN-012 | Shadow replay | record sample → replay at 1×/2× speed to target; ordering/timing fidelity; header rewrite; method filter (GET-only mode) | fidelity within tolerance |
| FUN-013 | IPLD worker | DAG build/verify: CID = multihash(sha2-256); ed25519 signatures verify; tamper detected | exact vectors |
| FUN-014 | Chaos sidecar | inject latency / packet drop / process kill on selected pod; hypothesis guard (steady-state check) aborts when violated; auto-revert at end | I1 |
| FUN-015 | Kill switch | global stop halts all workers and reverts all faults | I2 |
| FUN-016 | Ingestion pipeline | telemetry events → Redpanda → ClickHouse; schema per `telemetry_event.proto` | I5 |
| FUN-017 | TQ | summary (count, RPS, error %, p50/p90/p99/p999), time series, compare runs, anomaly detection, root-cause correlation (fault window ↔ latency shift) | correct vs reference computation |
| FUN-018 | Consistency verifier | known-linearizable history → PASS; known violations (stale read, lost write, dirty read) → FAIL with witness | sound |
| FUN-019 | Mesh policy verifier | YAML policies: mTLS strict, authz allow/deny; detects violations | golden |
| FUN-020 | Log-to-test generator | logs → mined paths → generated test cases; stable for same input | golden |
| FUN-021 | Contract healing | OpenAPI diff classification (breaking / non-breaking); proposed heal patch valid | golden |
| FUN-022 | Canary authorizer | metrics baseline vs canary → authorize / block per thresholds | decision table |
| FUN-023 | CLI | `run` exit codes: 0 pass, non-zero on SLO breach / failed run / invalid config; CI integration output formats (JUnit/annotations) | per `internal/cicdintegration` |
| FUN-024 | Observability kit / OTel | traces span GW→SCH→worker; metrics exported | linked trace ids |
| FUN-025 | Secrets | Vault secret referenced in pipeline is injected to worker, never appears in logs/telemetry/GraphQL responses | redaction verified |

## 3. Negative / boundary / edge
| ID | Scenario | Exp |
|---|---|---|
| NEG-001 | Pipeline YAML: invalid syntax, unknown stage type, cycle, self-dep, dangling dep, duplicate ids, empty, 10k nodes, YAML anchors bomb, wrong types, negative/zero/huge rps & duration, unknown protocol | `validate` exit ≠ 0 with line-level message; GW rejects same |
| NEG-002 | Target not in allow-list / internal IP / metadata endpoint / control-plane's own URL / `localhost` | rejected (I1; SSRF) |
| NEG-003 | AuthN/Z: no JWT, expired, other tenant's pipeline/run/env/telemetry ids; viewer starting runs / approving / flipping kill switch | 401/403 |
| NEG-004 | Run control: cancel twice; approve non-waiting run; approve by requester (4-eyes if required); start run of deleted pipeline; mutate finished run | proper errors |
| NEG-005 | Load params boundary: rps 1, 0.1 (fractional), 1e6; duration 0, 1 s, 24 h; concurrency 1 vs target saturation; payload 0 B / 10 MB | defined; accurate at low rates (1 rps = exactly N requests) |
| NEG-006 | Target behaviours: refuses connections, hangs, resets mid-response, slow-loris responses, 5xx, TLS handshake failure, bad cert, HTTP/2 GOAWAY, redirects loops, DNS NXDOMAIN, 0-RTT reject | errors classified by type; worker doesn't hang/leak; **latency of failed requests not polluting success percentiles** |
| NEG-007 | WASM: module with infinite loop, memory.grow to 4 GB, deep recursion, `fd_write` to host path, `sock_open`, env access, malformed/huge binary, start function trap, missing export | trapped with reason; host intact; fuel/epoch timeout fires |
| NEG-008 | Chaos: selector matching zero pods, matching all pods / kube-system / the control plane itself, duration 0 / very long, unsupported kernel, missing privileges | refused (blast-radius guard) / graceful no-op |
| NEG-009 | Replay: corrupted recording, recording containing POST/DELETE with replay target = production allow-listed? , auth tokens in recording | non-idempotent filtered by default; secrets scrubbed |
| NEG-010 | Telemetry: malformed events, unknown run id, timestamps out of order / future, duplicates, 1 M events/s burst, negative latency, NaN | DLQ; no pipeline stall |
| NEG-011 | TQ: unknown run, empty run (0 requests) percentiles, single sample, time range inverted, huge range, SQL injection in filters, cross-tenant run id | defined (null not NaN); 4xx |
| NEG-012 | Consistency verifier: empty history, single op, 1 M ops, ops with missing return, overlapping forever-pending ops | bounded time or explicit "unknown" |
| NEG-013 | ENV: create with invalid spec, quota exceeded, teardown unknown env, double teardown, TTL 0 | handled; no orphan |
| NEG-014 | Cost: estimate for zero-duration, astronomically large run (overflow), unknown instance type | finite / rejected |
| NEG-015 | CLI: missing file, unreadable, env var interpolation undefined, unreachable control plane, SIGINT mid-run (must cancel remote run) | exit codes; cancellation propagated |

## 4. Integration
INT-001 GW ↔ Postgres (TypeORM migrations, transactions) · INT-002 GW ↔ SCH (`/pipeline-runs`) ↔ Temporal (workflow start, signals for approval/cancel/mutate, queries) · INT-003 SCH ↔ Kafka `dag_dispatch` ↔ workers (dispatch, ack, heartbeat, result) — contract = `protocol-contracts/proto` · INT-004 workers → OTel collector / Redpanda → ingestion → ClickHouse → TQ · INT-005 chaos sidecar ↔ k8s API (kind cluster) ↔ `chaos_fault_injection.proto` · INT-006 ENV ↔ Redis ↔ k8s/cloud (mock) · INT-007 GW/worker ↔ Vault / AWS SM (localstack) · INT-008 policy gate ↔ OPA sidecar · INT-009 frontend ↔ GW GraphQL + WS subscriptions · INT-010 proto/GraphQL/OpenAPI breaking-change checks (`buf breaking`, schema diff) · INT-011 Helm `lint`/`template` + `kubeconform`, Kustomize build per overlay, `terraform validate/plan` (no apply), Crossplane composition render, `conftest`/OPA policies on manifests (privileged sidecar scoped) · INT-012 Temporal replay tests for scheduler workflows.

## 5. E2E / system
E2E-001 **Golden pipeline**: validate → cost gate → provision env → seed data → ramp load (Go worker) ∥ QUIC load (Rust worker) → inject latency fault mid-run with hypothesis guard → collect telemetry → RCA attributes latency shift to the fault window → consistency check of target → teardown → CLI exits per SLO · E2E-002 **Approval path** with human gate + reject path · E2E-003 **Kill-switch drill** in the middle of E2E-001 (I2, I1, I8) · E2E-004 **Multi-tenant**: 20 tenants × concurrent runs with priorities; fairness & isolation · E2E-005 **Shadow replay** of recorded traffic → compare response diff report · E2E-006 **Canary decision**: run against baseline & canary builds of CAL (canary has injected regression) → authorizer blocks · E2E-007 **CI integration**: pipeline in a CI job (GitHub Actions workflow under `tools/testforge-cli/.github`) gating a merge.

## 6. Measurement-accuracy validation (unique to this product; I4)
| ID | Scenario | Tolerance |
|---|---|---|
| ACC-001 | Request count: worker-reported == CAL-counted == TQ-reported, for 1 / 100 / 5k / 50k rps × 60 s | exact (worker vs CAL), TQ exact after dedupe |
| ACC-002 | Rate accuracy & pacing: achieved RPS per second vs target; inter-arrival distribution (constant vs Poisson as configured) | mean ±1 %, per-second ±5 % |
| ACC-003 | Latency truth: CAL injects fixed 50 ms ⇒ reported p50/p99 ∈ [50, 50+ε]; bimodal (95 % 10 ms, 5 % 500 ms) ⇒ p99 ≈ 500 ms | ±5 % or ±2 ms |
| ACC-004 | **Coordinated omission**: CAL stalls 2 s every 10 s under open-model load ⇒ reported p99.9 must reflect queueing (≈ seconds), not the service time | must not under-report |
| ACC-005 | Client saturation detection: target instant, worker CPU-bound ⇒ run flags "load generator saturated" rather than reporting target latency | flag present |
| ACC-006 | Error classification: CAL returns 10 % 503, 1 % resets, 1 % timeouts ⇒ reported breakdown matches | ±0.2 pp |
| ACC-007 | Multi-worker aggregation: percentiles merged from histograms (not averaged) — compare to exact percentile over all raw samples | within histogram precision |
| ACC-008 | Clock/skew: workers with ±2 s clock offset ⇒ time-series alignment & RCA windows still correct (server-side timestamps or sync) | aligned ±1 s |
| ACC-009 | Chaos accuracy: configured +100 ms / 10 % drop ⇒ measured +100 ±10 ms / 10 ±1 % at CAL; applied only to selected pods | as stated |
| ACC-010 | Replay timing fidelity at 1×: inter-request gaps vs recording | ±5 % |
| ACC-011 | Go vs Rust worker parity on same scenario | results within tolerance of each other |

## 7. Regression
Golden: pipeline YAML fixtures → parsed DAG; scheduler decision traces (priority/fairness) on fixed inputs; cost-estimate table; TQ SQL results on fixed dataset; consistency-checker history corpus (pass/fail labels); contract-diff corpus; canary decision table; proto descriptors; GraphQL schema; ClickHouse schema; Helm rendered manifests snapshot; Temporal histories; ACC baselines.

## 8. Concurrency / race
RACE-001 start same run twice concurrently / duplicate `/pipeline-runs` request → one workflow (idempotency key) · RACE-002 Kafka redelivery of dispatch → node executed once (I3) · RACE-003 two workers claim same dispatch → one executes (lease/fencing) · RACE-004 approve racing cancel racing timeout → one terminal outcome · RACE-005 DAG mutation racing node completion → no orphan / double-run node · RACE-006 kill switch racing new dispatches → none after activation ack · RACE-007 ENV TTL reaper racing run start on same env → run fails clean or env kept; never torn down under a running test silently · RACE-008 concurrent teardown + create with same name · RACE-009 priority queue under 1k concurrent enqueues/dequeues (Go `-race`; property: dequeue order respects (priority, fifo) per tenant) · RACE-010 ingestion consumer rebalance mid-batch → I5 · RACE-011 Rust workers: `loom`/TSan on shared stats aggregators; no lost counts at high concurrency (ACC-001 is the oracle) · RACE-012 GraphQL subscription fan-out under churn: no cross-tenant event delivery.

## 9. Consistency
CON-001 run status in Postgres (GW) == Temporal workflow status == per-node results in telemetry · CON-002 dispatched nodes == completed + failed + cancelled (no lost dispatch) · CON-003 env registry (Redis) == actual resources (k8s namespaces / mock cloud) — orphan sweep = ∅ · CON-004 telemetry rows == Σ worker-reported counts · CON-005 fault registry == faults actually present on nodes (tc/eBPF maps empty after run) · CON-006 cost actual vs estimate drift tracked.

## 10. Load / stress / spike / soak / scalability (of the platform itself)
| ID | Profile | Assert |
|---|---|---|
| LOAD-001 | 500 concurrent runs × 20-node DAGs | scheduling latency p99 < 2 s; no starvation |
| LOAD-002 | Single run fan-out to 500 workers | dispatch + result collection complete; aggregation correct |
| LOAD-003 | Telemetry 500k events/s for 15 min | ingestion lag bounded; CH inserts batched; TQ p99 < 2 s on that run |
| LOAD-004 | Max generator throughput per worker (Go vs Rust) vs cores | rps/core baseline; ACC-005 flag at limit |
| LOAD-005 | GW GraphQL 300 rps + 5k subscriptions | p99 < 300 ms |
| STR-001 | Queue 100k pending runs | bounded memory; priority still honoured |
| STR-002 | Temporal/Kafka back-pressure when workers absent | runs wait/timeout cleanly |
| STR-003 | ClickHouse disk tier full / TTL move during queries | queries still correct |
| SPK-001 | 0→1k run submissions in 5 s | accepted or 429; none lost |
| SOAK-001 | 48 h continuous pipelines + chaos + env churn | zero leaked envs/faults/goroutines/FDs/Redis keys; lag flat |
| SCAL-001 | Workers 1→10→100 | generated load scales linearly; aggregation exact |
| SCAL-002 | Scheduler / GW replicas 1→3 | throughput scales; RACE-001..003 hold |

## 11. Chaos / resilience / fault tolerance (of the platform)
| ID | Fault | Exp |
|---|---|---|
| CHA-001 | Kill load worker mid-stage | node retried or failed per policy; partial stats labelled incomplete; run terminal |
| CHA-002 | **Kill chaos sidecar while a fault is active** | fault auto-expires (kernel-side TTL / watchdog / reconciliation on restart) — I1 |
| CHA-003 | Control plane (GW+SCH) dies during active load+fault | workers hit dead-man timeout and stop; faults reverted (I1, I2) |
| CHA-004 | Temporal down 2 min | runs pause/resume; no duplicate node execution |
| CHA-005 | Redpanda down / partitioned | dispatch retries; telemetry buffered at worker/collector with bounded memory; I5 after recovery |
| CHA-006 | ClickHouse down | ingestion holds offsets; results complete after recovery |
| CHA-007 | Redis down / flushed | ENV state rebuilt or runs fail safe; **no mass teardown, no orphan leak** |
| CHA-008 | Postgres down | GW 503; running workflows continue; status reconciles |
| CHA-009 | Vault sealed / unreachable | runs needing secrets fail before generating load; no fallback to plaintext |
| CHA-010 | OPA sidecar down | policy gate fails closed |
| CHA-011 | Network partition worker↔control plane (worker still reaches target) | dead-man stop within T (I2) |
| CHA-012 | k8s API unavailable during teardown | retried until clean; alert on leak |
| CHA-013 | Kill switch with half the fleet unreachable | reachable half stops immediately; unreachable half stops by dead-man timer |
| CHA-014 | Clock skew on workers | ACC-008 |
| CHA-015 | Rolling upgrade of scheduler with in-flight workflows | Temporal versioning: no non-determinism |

## 12. Durability
DUR-001 pipeline definitions & run history survive Postgres/GW restart · DUR-002 Temporal histories survive restart; in-flight runs resume · DUR-003 acked telemetry survives collector/ingestion crash · DUR-004 ClickHouse tiered storage retains run data for retention period; backups restorable · DUR-005 audit trail of approvals, kill-switch activations, fault injections is immutable.

## 13. Security
SSRF/allow-list (NEG-002) · sandbox escape suite (NEG-007) + `cargo fuzz` on wasm validation · tenant isolation matrix on GraphQL (ids, subscriptions) and TQ · secrets redaction (FUN-025) incl. telemetry payload capture · privileged sidecar RBAC least privilege; admission policy · replay PII scrubbing · CLI token storage · supply chain (`pnpm audit`, `cargo audit`, `govulncheck`) · IaC scan (`tfsec`/`checkov`, `kube-linter`) · abuse prevention: the platform is a DDoS tool if misused — rate/target ownership verification is a P0 control.

## 14. Exit criteria
I1/I2 proven in CHA-002/003/011/013 (zero residual faults/load); ACC-001..007 within tolerance; I3 holds across RACE + CHA; sandbox suite 100 % contained; zero leaked resources after SOAK-001; IaC checks clean.
