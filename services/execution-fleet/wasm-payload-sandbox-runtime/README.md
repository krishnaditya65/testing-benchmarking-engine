# wasm-payload-sandbox-runtime

Execution-fleet worker: a wasmtime host embedding that runs user-defined
pre/post-request scripts and payload-generation logic inside a fuel- and
epoch-bounded, WASI-deny-by-default sandbox. Part of the `execution-fleet`
layer described in [ARCHITECTURE.md](../../../ARCHITECTURE.md) — stateless,
horizontally scalable, and driven only by orders from the orchestration
control plane over Redpanda's `dag.node.dispatch.v1` topic. It never calls
sibling workers directly, and emits telemetry outward via OTLP to the OTel
Collector like every other execution-fleet service.

This is the fleet's **designated home for executing untrusted user code**.
Every other worker in `execution-fleet` runs code the platform itself wrote
(load generators, DAG traversal, chaos injection); this one is the only
place a test author's own script logic actually executes, which is why its
design center is the sandbox boundary rather than raw throughput.

## Implements (feature-manifest bullets)

- **"In-memory Wasm synthetic payload generation"** — implemented by
  [`WasmSandboxHost`](src/sandbox/wasm_sandbox_host.rs) running a user's
  Wasm module to completion inside a wasmtime `Store`, combined with
  [`SyntheticPayloadGenerator`](src/generation/synthetic_payload_generator.rs)
  (`ChaCha20SyntheticPayloadGenerator`), which produces structured JSON
  payloads — strings, numbers, booleans, nested objects/arrays — from a
  `PayloadSchemaHint`, with a UUIDv4 uniqueness token embedded in every
  payload so millions of generated requests never collide. Nothing is
  written to disk at any point; generation and sandbox execution both live
  entirely in process memory for the lifetime of one dispatched node.
- **"Smart payload fuzzing"** — implemented by
  [`SmartPayloadFuzzer`](src/fuzzing/smart_payload_fuzzer.rs), which walks a
  baseline JSON payload and emits real boundary-value mutations (integer
  extremes, empty/very-long strings, type-confusion swaps, array length
  extremes, unicode/injection-character edge cases) as individually labeled
  variants via the `PayloadFuzzer<T>` trait.
- **"Scripting SDK + snippet marketplace"** — implemented by
  [`sdk`](src/sdk/) (the typed `env.sdk_*` host-function surface user
  pre/post-request scripts run against, plus `ScriptManifest` for
  publishing them) and [`marketplace`](src/marketplace/)
  (`SnippetRegistryClient`, the port/stub-adapter for sharing snippets
  across pipelines). See "Scripting SDK" and "Snippet marketplace" below.

## What does *not* live here

This service owns the Wasm sandbox and payload logic only. Adjacent
execution-fleet concerns live in their own services — see each one's own
`README.md`:

| Concern | Lives in |
|---|---|
| REST/gRPC/WebSocket/GraphQL protocol clients | `execution-fleet/protocol-load-worker-go` |
| Raw TCP/QUIC socket saturation | `execution-fleet/protocol-load-worker-rust` |
| DAG/IPLD block traversal, CID resolution, crypto-overhead profiling | `execution-fleet/dag-ipld-benchmark-worker-rust` |
| eBPF-based fault injection (chaos) | `execution-fleet/chaos-ebpf-sidecar` |
| Cross-region clock-drift compensation for latency measurements | `execution-fleet/protocol-load-worker-rust` (`timing` module) |

If you came here looking for one of those, it is intentionally not in this
crate — see `ARCHITECTURE.md`'s "Why services are split this way" section
for the reasoning (independent scaling, independent failure domains).

## Sandboxing / safety model

Untrusted user scripts are the whole point of this service, so every
execution is bounded on three independent axes:

1. **Fuel-bounded work.** `wasmtime::Config::consume_fuel(true)` plus a
   per-`Store` `set_fuel(...)` budget means an infinite loop or
   pathologically expensive script traps with `SandboxExecutionError::FuelExhausted`
   instead of consuming a worker thread forever.
2. **Epoch-bounded wall-clock time.** A background ticker thread increments
   the shared `Engine`'s epoch on a fixed interval; each `Store` sets an
   epoch deadline as a time-based backstop independent of how fuel happens
   to be metered for a given script.
3. **WASI deny-by-default.** The `WasiCtxBuilder` used to construct each
   execution's context grants **no** filesystem preopens and **no**
   sockets. The only wired capability is stdout, and it is piped into an
   in-memory buffer the host reads back after execution — never the real
   process stdout, never a file, never the network. Any guest import that
   isn't provided (a denied filesystem/network syscall) fails to link or
   traps, which `WasmSandboxHost::classify_wasmtime_error` reports as
   `SandboxExecutionError::WasiAccessDenied` rather than a generic failure.

Every execution gets a **brand-new `Store<T>`** — nothing about a sandbox
run persists past `WasmSandboxHost::execute` returning, so a compromised or
buggy guest in one execution cannot taint the next (see `SCALING.md`).

## Guest ABI

A `.wasm` module hosted by this sandbox is expected to target `wasm32-wasi`
and export `memory`, `sandbox_alloc(len: i32) -> i32`, and an entry point
with signature `(ptr: i32, len: i32) -> i64` (packed output pointer/length).
Full contract is documented on `WasmSandboxHost` in
`src/sandbox/wasm_sandbox_host.rs`.

NOTE: in production, the `.wasm` bytes and schema hints referenced by
`main.rs`'s CLI flags instead arrive from the control plane as part of the
dispatched `dag.node.dispatch.v1` message (an artifact-store reference, not
inline bytes). See the NOTE comments in `src/main.rs` and
`src/generation/synthetic_payload_generator.rs`.

## Scripting SDK

[`src/sdk/scripting_sdk_host_functions.rs`](src/sdk/scripting_sdk_host_functions.rs) defines
the typed `env.sdk_*` host-function surface a user's pre/post-request script is written
against — the actual API surface, analogous to Postman's `pm.*` API but Wasm-sandboxed: every
call still crosses `WasmSandboxHost`'s fuel-metered, WASI-deny-by-default boundary.

| Import | Signature | Purpose |
|---|---|---|
| `env.sdk_get_variable` | `(name_ptr, name_len) -> i64` | Read a variable set by an earlier script in the same test run. Packed `(ptr << 32) \| len`, or `SDK_VARIABLE_NOT_FOUND` (`-1`) if unset. |
| `env.sdk_set_variable` | `(name_ptr, name_len, value_ptr, value_len) -> ()` | Store a variable for a later script to read. |
| `env.sdk_http_context` | `() -> i64` | Fetch the JSON-serialized `HttpContext` (method/URL/headers/body, plus response fields for a post-request script) for the request this script is attached to. |
| `env.sdk_assert` | `(description_ptr, description_len, passed: i32) -> ()` | Record a pass/fail assertion — the scripting-SDK equivalent of a DAG `ASSERTION` node. |
| `env.sdk_log` | `(level: i32, message_ptr, message_len) -> ()` | Append a structured log line, surfaced via this worker's normal OTLP telemetry. |

`register_into_linker` wires all five into a `wasmtime::Linker<T>` given an accessor closure
that extracts a `ScriptExecutionContext` from the store's state — see that function's NOTE
comment for exactly what remains to thread this into `WasmSandboxHost::execute`'s
`SandboxGuestState` end to end. `ScriptManifest` ([`src/sdk/script_manifest.rs`](src/sdk/script_manifest.rs))
describes a publishable snippet — name, description, author, `SemanticVersion`, and which
`HostFunctionCapability`s it requires — so a snippet can declare (and a runtime or reviewer can
check) exactly which of the table above it needs before ever instantiating the module.

### Example: extract a JWT from a login response, inject it into the next request

The canonical scripting-SDK pattern — a post-request script on a login call extracts a token
and stores it as a variable; a pre-request script on a later call reads that variable and adds
it as an `Authorization` header. Shown here as commented pseudo-Wasm-source (what a script
author would compile against this SDK) rather than a full toolchain-specific example, since the
guest ABI is toolchain-agnostic by design (see `WasmSandboxHost`'s module doc comment):

```text
// post-request script attached to POST /login, entry point `run_post_request_script`.
// Pseudo-source for a wasm32-wasi guest compiled against this SDK's env.sdk_* imports.
fn run_post_request_script(ptr: i32, len: i32) -> i64 {
    let ctx = env::sdk_http_context();               // packed ptr/len -> JSON HttpContext
    let response_body = parse_json(read_guest(ctx)).get("response_body");
    let token = parse_json(response_body).get("access_token");

    env::sdk_set_variable("auth_token", token);       // visible to the next script in this run
    env::sdk_assert("login response includes access_token", token.is_some());
    env::sdk_log(LOG_INFO, "extracted access_token for downstream requests");

    pack_empty_result()
}

// pre-request script attached to a later authenticated request, entry point
// `run_pre_request_script`.
fn run_pre_request_script(ptr: i32, len: i32) -> i64 {
    let token = env::sdk_get_variable("auth_token");  // SDK_VARIABLE_NOT_FOUND (-1) if unset
    if token == SDK_VARIABLE_NOT_FOUND {
        env::sdk_assert("auth_token was set by a prior script", false);
        return pack_empty_result();
    }
    // A real script mutates its own request headers here via a request-shaping host import;
    // out of scope for this SDK pass — see env.sdk_http_context's doc comment for what's
    // currently read-only vs. a documented future extension.
    env::sdk_log(LOG_INFO, "injecting auth_token into Authorization header");
    pack_empty_result()
}
```

The equivalent as a Rust-host-side test fixture — what a unit test in this crate would assert
against `ScriptExecutionContext` directly, without a real compiled `.wasm` module — looks like:

```rust
use wasm_payload_sandbox_runtime::sdk::{HttpContext, ScriptExecutionContext};

let http_context = HttpContext {
    method: "POST".to_string(),
    url: "https://api.example.com/login".to_string(),
    response_status: Some(200),
    response_body: Some(r#"{"access_token":"eyJhbGciOi..."}"#.to_string()),
    ..Default::default()
};
let mut ctx = ScriptExecutionContext::new(http_context);

// What the post-request script's env.sdk_set_variable / env.sdk_assert calls do, expressed
// directly against the host-side context a real execution would mutate through the Linker:
ctx.variables.insert("auth_token".to_string(), "eyJhbGciOi...".to_string());
assert!(ctx.all_assertions_passed()); // no assertions recorded yet == vacuously true

// The next script's env.sdk_get_variable("auth_token") call is exactly this lookup:
assert_eq!(ctx.variables.get("auth_token").map(String::as_str), Some("eyJhbGciOi..."));
```

## Snippet marketplace

[`src/marketplace/snippet_registry_client.rs`](src/marketplace/snippet_registry_client.rs)
defines `SnippetRegistryClient` — `publish`/`fetch` against a central registry of `ScriptManifest`
+ compiled `.wasm` bytes (`PublishedSnippet`) — plus a stub HTTP-backed adapter,
`HttpSnippetRegistryClient`. **The registry service itself is out of scope for this pass** — see
that file's module doc comment; this is the client contract only, matching this repository's
established pattern of documenting a seam before its real backend exists (compare
`chaos-ebpf-sidecar`'s `KernelNetworkFaultApplier`/`PodKillTarget`).

## Local development

```bash
cargo run -- --wasm-module-path ./fixtures/example-pre-request-script.wasm
```

Omit `--wasm-module-path` to run only the payload generation + fuzzing
steps (no sandbox fixture required).
