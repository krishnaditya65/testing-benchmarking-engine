# consistency-verification-worker

Distributed Systems & Infrastructure Validation service that tests distributed databases
(CockroachDB, Cassandra, ScyllaDB) during induced network partitions to verify their ACID and/or
Eventual Consistency guarantees actually hold, rather than trusting the vendor's claims.

## Feature manifest mapping

**"Data Consistency Benchmarking"** (feature-manifest section 5, "Distributed Systems &
Infrastructure Validation")

- `internal/consistency/quorum_write_driver.go` — `DatabaseConsistencyDriver`, the seam between
  this package's write/read-comparison logic and a specific database's real wire protocol, plus
  `QuorumWriteBenchmarkDriver` (drives a batch of writes concurrently at a configurable
  `ConsistencyLevel`). `CockroachDbDriver` and `CassandraDriver` are stub implementations whose
  doc comments show the exact `pgx`/`crdbpgx` and `gocql` calls real wiring makes — see the
  "Not yet wired to a real database" section below for why they are stubs.
- `internal/consistency/partition_induced_read_verifier.go` — `PartitionInducedReadVerifier`,
  which coordinates with a chaos-ebpf-sidecar-induced network partition via the same
  `chaos.fault-injection-event.v1` Redpanda topic contract defined in
  `shared-libraries/protocol-contracts/proto/chaos_fault_injection.proto` (mirrored here,
  field-for-field, as `ChaosFaultInjectionEvent`). Once a fault event reports
  `CHAOS_FAULT_LIFECYCLE_STATE_ACTIVE`, `Verify()` reads the same keys back across every replica
  on both sides of the partition and classifies `ConsistencyViolation`s: stale reads under a
  linearizability claim, lost writes, and divergent replicas — producing a
  `ConsistencyVerificationReport`.
- `internal/consistency/acid_property_assertion_suite.go` — `AcidPropertyAssertionSuite`, four
  independent check functions (`AssertAtomicity`, `AssertConsistency`, `AssertIsolation`,
  `AssertDurability`), each returning a pass/fail `AcidAssertionResult` with concrete evidence
  rather than a single aggregate boolean, so a failure report says exactly which ACID guarantee
  broke.

## Why this service exists, not another worker

`services/execution-fleet/` workers generate load and inject chaos but never *interpret*
correctness against a specific database's consistency claims — that is a distinct concern
(comparing replica-level read results against acknowledged writes, understanding CAP-theorem
tradeoffs per consistency level) from `chaos-ebpf-sidecar`'s job of *causing* the partition or
`protocol-load-worker-go`'s job of generating protocol traffic. Per `ARCHITECTURE.md`'s
"independent failure domains" rationale, this worker only *consumes* the
`chaos.fault-injection-event.v1` topic chaos-ebpf-sidecar publishes to — it never calls that
service directly, and a crash here never affects an in-progress chaos experiment.

## Not yet wired to a real database

`CockroachDbDriver` and `CassandraDriver` (in `quorum_write_driver.go`) are structurally
complete but every method currently returns an explicit "not wired" error — this is a
starter/offline repository with no module-proxy network access, so `github.com/jackc/pgx/v5`
and `github.com/gocql/gocql` are referenced only in NOTE doc comments showing the real client
calls, not imported. Wiring a real backend means: add the driver to `go.mod`, run `go mod tidy`
against a live proxy, and replace the stub method bodies per each NOTE comment — no call site in
`partition_induced_read_verifier.go` or `acid_property_assertion_suite.go` needs to change,
since both depend only on the `DatabaseConsistencyDriver` interface.

## Layout

```
cmd/consistency-verification-worker/main.go   flag/env config, driver selection, process lifecycle
internal/consistency/
  quorum_write_driver.go               DatabaseConsistencyDriver + CockroachDbDriver/CassandraDriver
  partition_induced_read_verifier.go   ChaosFaultInjectionEvent mirror + PartitionInducedReadVerifier
  acid_property_assertion_suite.go     AcidPropertyAssertionSuite
```

## Development

```
go build ./...
go vet ./...
```

No `go test` suite ships yet — the driver stubs return explicit errors, so behavioral tests
against them would only assert "not wired," not real consistency-checking logic. Once a real
driver is wired, table-driven tests belong alongside each file above (e.g.
`partition_induced_read_verifier_test.go` constructing a fake `DatabaseConsistencyDriver` to
exercise `detectViolationsForKey` deterministically).
