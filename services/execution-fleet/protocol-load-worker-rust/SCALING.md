# Scaling & failure characteristics — protocol-load-worker-rust

## Horizontal scaling

This worker is stateless and scales by replica count, not by growing any single
process's resource limits (per `ARCHITECTURE.md`'s "Independent scaling" rationale —
a load test might need thousands of these replicas while the scheduler stays at a
handful). Each replica independently ramps its own `TcpConnectionPoolSaturator` up to
`PLW_TARGET_CONCURRENCY` connections against its assigned target; the control plane
achieves a given aggregate connection count by choosing replica count ×
per-replica concurrency, not by raising per-replica concurrency without bound.

### File-descriptor / ulimit considerations

Every open TCP or QUIC connection consumes one file descriptor (TCP) or one UDP
socket FD shared across all QUIC connections on that endpoint. At thousands of
concurrent connections per replica, the default Linux soft limit (`ulimit -n`,
commonly 1024) is exhausted almost immediately, so:

- The container image / Kubernetes pod spec must raise `LimitNOFILE`
  (`securityContext` cannot set this directly — it needs a pod-level `initContainer`
  ulimit bump or a container runtime default override) to comfortably exceed
  `PLW_TARGET_CONCURRENCY` plus headroom for the worker's own listener/telemetry
  sockets — a common floor is `target_concurrency * 1.2` rounded up.
- TCP saturation at very high fan-out against a single target can also exhaust
  **ephemeral source ports** (default range `net.ipv4.ip_local_port_range`, ~28,000
  ports) well before hitting the FD limit if all connections share one source IP.
  Scaling past that requires either binding multiple source addresses (see the NOTE
  in `tcp_connection_pool_saturator.rs` about `socket2`-level port range control) or
  running enough replicas (each with its own pod IP) that no single IP needs more
  than ~20–25k outbound connections.
- `ramp_up_batch_size` exists specifically to keep connect()-storm behavior bounded
  even once FD/port budget is available — ramping 10,000 connections in batches of
  100 avoids a single scheduler tick trying to issue 10,000 syscalls at once.

## State held

- **In-memory only**: the open-connection pool itself (tokio task handles + the
  underlying `TcpStream`s) and the `ClockDriftCompensator`'s running offset estimate.
- **Nothing persisted.** No database, no local disk state, no durable queue
  membership. This is deliberate: per `ARCHITECTURE.md`, execution-fleet workers are
  stateless so Kubernetes HPA can scale them independently of the control plane, and
  so a worker crash never has a recovery story more complex than "reschedule it."

## Crash-mid-test recovery

If a worker Pod dies mid-saturation (OOM, node eviction, spot preemption):

1. All of its open sockets are closed by the kernel as the process exits — nothing to
   clean up on the target side beyond normal TCP FIN/RST or QUIC connection-close
   handling.
2. The worker holds no checkpointed state to reconcile, because it held no durable
   state to begin with — unlike `dag-workflow-scheduler`'s Temporal-backed durability
   or the Redis checkpoint `ARCHITECTURE.md` describes for load-phase transitions,
   this worker's entire contribution to a load phase is "some sockets were open";
   losing that contribution loses nothing that needs replaying.
3. The **control plane** (not this worker) owns recovery: it detects the missing
   worker (liveness/heartbeat gap on `dag.node.dispatch.v1`'s ack path) and
   redispatches the affected load phase to a replacement replica, which starts a
   fresh ramp-up from zero. There is no partial-progress handoff, because partial
   progress here is just "N connections were open," which the replacement
   immediately re-establishes.

## Resource profile

- **Network-bound**, first and foremost — the whole point of this service is to
  saturate a target's socket-accept and packet-processing capacity, not this
  worker's own CPU.
- **Some CPU for TLS/QUIC crypto**: QUIC mandates TLS 1.3, so every QUIC connection
  costs a handshake's worth of asymmetric crypto plus ongoing AEAD encryption for
  every packet — real deployments should budget CPU accordingly and watch for
  crypto becoming the bottleneck before the network does at very high QUIC fan-out.
- **Low per-connection memory footprint is the point of Rust/tokio here**: each open
  connection is one lightweight async task (tokio's default task overhead is on the
  order of tens to low hundreds of bytes plus the connection's own buffers), not an
  OS thread — this is what lets a single replica hold many thousands of connections
  without the per-connection stack overhead a thread-per-connection model would pay,
  and without a garbage collector introducing latency-spike risk under high
  connection churn.
