# Scaling & failure characteristics — dag-ipld-benchmark-worker-rust

## Horizontal scaling

This worker is embarrassingly parallel at two grain sizes:

- **Per DAG-test-run.** Each dispatch message on `dag.node.dispatch.v1` names one DAG test
  run (or one node execution within a run) to benchmark; independent runs share nothing, so
  any number of replicas can each pick up a different run concurrently.
- **Per DAG-subtree.** Within a single large DAG, disjoint subtrees can be sharded across
  replicas — `IpldBlockTraversalProfiler::profile_traversal_from` only needs the root `CidLike`
  of the subtree it's given, so the control plane can fan a single run out across many worker
  pods, each benchmarking an independent shard, and merge the resulting `TraversalReport`s
  downstream in the telemetry lake.

Kubernetes HPA should scale replica count on CPU utilization (see "Resource profile" below)
against the depth of the `dag.node.dispatch.v1` consumer group's backlog, the same pattern
used by `protocol-load-worker-go`/`-rust`.

## State held

None persistent. Each invocation:

1. reads the DAG (or DAG subtree) it needs to benchmark from the dispatch payload and/or a
   blockstore fetch (via `BlockFetcher`) — never from local disk or a database this service
   owns;
2. runs entirely in process memory (`InMemoryBlockFetcher`-shaped local state, cryptographic
   sample buffers, the bisection's in-memory `HashMap` index over the execution history it was
   given);
3. reports results outward as OTLP telemetry and exits (or returns to waiting for the next
   dispatch message).

There is no local cache, no checkpoint file, and no state that would need to survive a pod
restart — every replica is fungible and interchangeable at message-dispatch granularity.

## Crash-mid-benchmark recovery

If a worker pod dies mid-run, the control plane's `dag-workflow-scheduler` simply
redispatches the node execution (or DAG-subtree benchmark) to another replica. This is safe
because:

- **Idempotent.** Re-running `IpldBlockTraversalProfiler`, `CryptographicOverheadProfiler`,
  or `DagFlakinessRootCauseIsolator` against the same inputs produces an equivalent report —
  none of them mutate the DAG, the blockstore, or any shared state as a side effect of
  running.
- **Side-effect-free.** Profiling reads blocks and times operations; it never writes to the
  IPLD blockstore, never signs anything on the network's behalf, and never mutates the
  execution history it diagnoses. A partial or lost run leaves nothing to clean up.

This mirrors the durability model described in ARCHITECTURE.md: durability lives in Temporal
(owned by `dag-workflow-scheduler`), not in this worker's process memory.

## Resource profile

**CPU-bound.** The dominant costs are:

- sha2-256 hashing and ed25519 verification (`CryptographicOverheadProfiler`) — pure CPU;
- the mock ZK-proof verifier's simulated compression rounds — pure CPU (a real
  `ark-groth16`/`halo2` verifier would be CPU-bound too, typically more so);
- traversal-timing precision (`std::time::Instant` measurements around each hop) — sensitive
  to CPU scheduling noise, so this worker benefits from dedicated cores / limited noisy-neighbor
  contention more than from extra memory or network bandwidth.

Network use is limited to the initial block fetch(es) — a real `BlockFetcher` implementation
talking to a remote blockstore or the IPFS/IPLD network — plus outbound OTLP export; both are
small relative to the CPU cost of the hashing/signature/traversal work itself. Size worker pod
CPU requests/limits generously and memory requests modestly; this is not a memory- or
network-bound workload.
