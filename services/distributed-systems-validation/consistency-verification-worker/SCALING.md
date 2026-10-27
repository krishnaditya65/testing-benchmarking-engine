# Scaling & failure characteristics

## Deployment model: batch/Job, not a long-lived request-serving Deployment

Unlike `protocol-load-worker-go`'s HPA-scaled Deployment, `consistency-verification-worker`
runs one verification *scenario* at a time against one target cluster: drive writes, wait for a
chaos-ebpf-sidecar-induced partition to go `ACTIVE`, read across replicas, produce one
`ConsistencyVerificationReport`. This maps naturally onto a Kubernetes `Job` (or a
Temporal-orchestrated activity invoked by `dag-workflow-scheduler`, matching
`ARCHITECTURE.md`'s "durable DAG run orchestration") dispatched once per pipeline run that
includes a `CHAOS_INJECTION` DAG node targeting a distributed database, rather than a pool of
replicas competing for a shared work queue.

Running multiple instances concurrently is safe as long as each instance targets a distinct
`(targetHosts, key-namespace)` pair — the probe keys `AcidPropertyAssertionSuite` writes
(`acid-probe:*`) and whatever key namespace a caller passes to `PartitionInducedReadVerifier`
are the only shared mutable state, so two instances hammering the *same* keys against the *same*
cluster concurrently would produce misleading isolation/atomicity evidence (this service's own
writes interleaving with each other, not just the database's behavior under test). Namespacing
probe keys per pipeline-run-id is the caller's responsibility (main.go's Redpanda consumer, once
wired, derives this from the `ChaosFaultInjectionEvent.pipeline_run_id` field).

## State held

This process holds no durable state of its own between runs — `ConsistencyVerificationReport`
and `AcidAssertionSuiteReport` are computed in-memory per invocation and (once OTLP export is
wired, see `main.go`'s NOTE) emitted outward immediately. If the process crashes mid-verification,
the correct recovery is simply re-running the scenario; no partial-result reconciliation is
needed since nothing partial was ever persisted as authoritative.

## Resource profile

- **Write-phase bursts.** `QuorumWriteBenchmarkDriver.DriveConcurrentWrites` fans out up to
  `maxInFlight` goroutines per batch — CPU/memory usage tracks the configured concurrency, not
  the size of the target cluster.
- **Mostly I/O-bound, not CPU-bound.** Almost all wall-clock time is spent waiting on the target
  database's network round-trips (write quorum acknowledgement, per-replica reads), so this
  service benefits far more from `maxInFlight` tuning than from additional CPU.
- **No privileged access required**, unlike `chaos-ebpf-sidecar` — this worker only needs
  network reachability to the target cluster's client ports and to Redpanda, so it can run as an
  ordinary unprivileged pod.

## What happens if the induced partition never resolves

`PartitionInducedReadVerifier.Verify` reads once, at the point of call, and does not itself
poll or block waiting for `CHAOS_FAULT_LIFECYCLE_STATE_RECOVERED` — that re-verification pass
("did the partition heal without losing/diverging any acknowledged write") is a second `Verify`
call a real Redpanda-consuming `main.go` makes once it observes the fault event's lifecycle
state transition, not something this package does internally. This keeps `Verify`'s contract
simple (one call = one point-in-time read across replicas) and keeps the "wait for RECOVERED"
polling/timeout policy — which is properly a chaos-experiment-duration concern, not a
consistency-checking concern — out of this package entirely.
