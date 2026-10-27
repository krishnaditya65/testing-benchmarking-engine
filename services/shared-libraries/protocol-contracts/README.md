# protocol-contracts

Protobuf message definitions for every cross-service, cross-language wire contract in the
platform. This is the **only** place these message shapes are defined — each consuming service
compiles its own language binding from these `.proto` files rather than hand-writing structs
that could drift from the wire format.

## Implements

- ARCHITECTURE.md communication contracts: `dag.node.dispatch.v1` (`dag_dispatch.proto`) and
  `telemetry.raw.v1` (`telemetry_event.proto`); `chaos_fault_injection.proto` documents the
  `chaos.fault-injection-event.v1` topic used by `chaos-ebpf-sidecar`.

## Files

| File | Topic | Producer(s) | Consumer(s) |
|---|---|---|---|
| `proto/dag_dispatch.proto` | `dag.node.dispatch.v1` | `dag-workflow-scheduler` | all execution-fleet workers |
| `proto/telemetry_event.proto` | `telemetry.raw.v1` | OTel Collector kafka exporters, workers | ClickHouse Kafka-engine table |
| `proto/chaos_fault_injection.proto` | `chaos.fault-injection-event.v1` | `chaos-ebpf-sidecar` | `dag-workflow-scheduler`, telemetry lake |

## Per-language compilation

Each consuming service compiles these definitions in its own build, into its own language,
rather than this package shipping a single generated artifact everyone imports:

- **TypeScript** (`pipeline-api-gateway`, `telemetry-query-api`, `redpanda-ingestion-pipeline`
  producers) — [`ts-proto`](https://github.com/stephenh/ts-proto), generating idiomatic TS
  interfaces + encode/decode functions.
- **Go** (`dag-workflow-scheduler`, `protocol-load-worker-go`) — `protoc-gen-go` +
  `protoc-gen-go-grpc`; note the `go_package` option already set in each `.proto` file.
- **Rust** (`protocol-load-worker-rust`, `dag-ipld-benchmark-worker-rust`, `chaos-ebpf-sidecar`)
  — [`prost`](https://github.com/tokio-rs/prost) via a `build.rs` invocation.

None of these generated artifacts are checked in; each service's build pipeline regenerates
them from this directory's `.proto` files as a build step.

## Compatibility review requirement

**Any change to a message in this directory is a cross-service contract change and requires a
compatibility review before merging** — a field removed or renumbered here can silently break a
service written in a different language that has already deployed against the previous shape,
with no compiler to catch it across the language boundary. Concretely:

- Only add new fields with new numbers; never reuse or renumber an existing field number.
- Only widen types (e.g. never change a field from `string` to `int64`); prefer adding a new
  field over changing an existing one's type.
- Removing a field means marking it `reserved`, not deleting the line outright, so the field
  number can never be accidentally reused.
- If you're changing `dag_dispatch.proto`'s `DagNodeKind` enum, also update the corresponding
  `PipelineNodeKind` union in `shared-libraries/dag-domain-model` in the same change — they must
  stay in lockstep.
- Get a second reviewer from a team owning a *different*-language consumer of the message
  you're changing (e.g. a Go or Rust owner reviewing a TypeScript-authored proto change).
