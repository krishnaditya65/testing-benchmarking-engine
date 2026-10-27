# Scaling & failure characteristics

## Horizontal scaling

Fully stateless per invocation. Every dispatched DAG node (a pre/post-request
script run, a payload-generation request, a fuzz-batch request) is handled
by building a brand-new `wasmtime::Store<SandboxGuestState>` inside
`WasmSandboxHost::execute`, running it to completion, and discarding it —
there is no session, connection, or in-process cache that a subsequent
invocation depends on. That means:

- Any replica can handle any dispatched node; there is no sticky routing.
- Replica count scales linearly with the `dag.node.dispatch.v1` consumer
  group's partition count and desired throughput — this service is designed
  to run as thousands of replicas under a Kubernetes HPA driven by Redpanda
  consumer-lag, the same pattern as its execution-fleet siblings.
- Losing a replica loses nothing but in-flight work, which the control
  plane redispatches (see "Crash-mid-test recovery" below).

The one piece of long-lived state per process is the `wasmtime::Engine`
inside `WasmSandboxHost` (expensive to construct — it owns JIT compilation
configuration and the epoch-ticker background thread) and its epoch-ticker
thread. Both are process-lifetime singletons shared read-only across every
`execute()` call; neither holds anything specific to a particular guest
execution.

## State held

**None persistent.** Per execution:

- The fuel budget (`Store::set_fuel`) and epoch deadline
  (`Store::set_epoch_deadline`) are set fresh on the new `Store` and consumed
  down during that one call only.
- The `WasiCtxBuilder`-constructed WASI context (deny-by-default, stdout
  piped to an in-memory buffer) is built fresh per execution and dropped
  with the `Store` when `execute()` returns.
- Generated payloads (`SyntheticPayloadGenerator`) and fuzzed variants
  (`SmartPayloadFuzzer`) are computed in memory and returned to the caller;
  nothing is cached or written to disk.
- A script's `ScriptExecutionContext` (variables, http context, assertions, log lines — see
  `src/sdk/`) is scoped to one execution the same way, and any snippet fetched via
  `SnippetRegistryClient` before that execution (module bytes + `ScriptManifest`) is read-only,
  keyed by immutable `(name, version)`, and safe to fetch once and reuse/cache across many
  executions of the same pinned snippet version — a marketplace fetch is off this service's hot
  path (an HTTP round trip to the registry, not a per-request cost) the same way `Module`
  compilation caching is described above.

If a future revision needs cross-execution memoization (e.g. caching a
compiled `wasmtime::Module` by content hash to skip re-validation/re-compile
on repeated dispatches of the same script), that cache would be read-only
and keyed by immutable artifact hash — it would not make executions
stateful with respect to each other's *results*.

## Crash-mid-test recovery

Two distinct failure modes, handled differently on purpose:

1. **Guest misbehavior (trap, fuel exhaustion, denied WASI call).** These
   are caught inside `WasmSandboxHost::execute` and returned as an ordinary
   `Result::Err(SandboxExecutionError)` — not a process crash. A user's
   infinite loop or out-of-bounds access degrades to a normal, observable
   test-failure signal (fed back to the control plane and into telemetry
   via OTLP), and the worker process keeps serving the next dispatched
   node immediately.
2. **Genuine process crash** (OOM, worker pod eviction, node preemption).
   Because every sandbox execution is designed to be idempotent and
   side-effect-free from the target-under-test's perspective — a script
   evaluates a payload or generates one, it does not itself perform the
   load-test's HTTP/gRPC calls — the control plane can safely redispatch
   the same `dag.node.dispatch.v1` node to a different replica without risk
   of double-executing an observable side effect. There is no
   worker-local state to reconcile on restart; the next `WasmSandboxHost`
   instance starts with an identical, empty `Engine`/ticker pair.

## Resource profile

- **CPU-bound.** Wasm execution (interpreted/JIT-compiled guest
  instructions under fuel metering) and payload generation/fuzzing
  (`ChaCha20Rng` draws, JSON tree construction/mutation) are both pure CPU
  work with no I/O wait in the hot path — this service's replica count
  should be sized against CPU request/limit, not connection count.
- **Memory-bound per concurrent `Store`.** Each in-flight execution holds
  one guest linear memory plus its `WasiCtxBuilder`-constructed context and
  stdout buffer (capped at `MAX_CAPTURED_STDOUT_BYTES` — see
  `wasm_sandbox_host.rs`). wasmtime's `Store`/`ResourceLimiter` hooks (guest
  linear-memory growth limits, instance-count limits) are the intended lever
  for tuning how many concurrent executions one replica can safely hold in
  memory before OOM risk outweighs the throughput gained from another
  replica — this starter uses wasmtime's default limits and does not yet
  wire a custom `ResourceLimiter`; doing so is the natural next step before
  running with adversarial user scripts at production scale.
