//! The typed host-function surface exposed *into* the Wasm sandbox for a user's pre/post-
//! request script — the actual API surface a script is written against, analogous to Postman's
//! `pm.*` API but Wasm-sandboxed: every call crosses the same fuel-metered, WASI-deny-by-default
//! boundary [`crate::sandbox::WasmSandboxHost`] enforces for everything else a guest does.
//!
//! # Guest-visible surface
//!
//! A guest module imports these functions from the `"env"` module (the same import module
//! wasmtime-wasi's `preview1` shim uses for its own imports, so a script author's toolchain
//! needs no special import-namespace configuration):
//!
//! | Import | Signature | Behavior |
//! |---|---|---|
//! | `env.sdk_get_variable` | `(name_ptr: i32, name_len: i32) -> i64` | Reads a variable set earlier in this test run (by a prior script, or seeded by the DAG node config). Returns a packed `(ptr << 32) \| len` pointing at the value in guest memory, or [`SDK_VARIABLE_NOT_FOUND`] if unset. |
//! | `env.sdk_set_variable` | `(name_ptr, name_len, value_ptr, value_len) -> ()` | Stores a variable, visible to subsequent scripts in the same test run (e.g. a post-request script extracting a token for the next request's pre-request script — see README.md's worked example). |
//! | `env.sdk_http_context` | `() -> i64` | Returns a packed pointer/length to the JSON-serialized [`HttpContext`] (method, URL, headers, body, and — for a post-request script — response status/headers/body) for the request this script is attached to. |
//! | `env.sdk_assert` | `(description_ptr, description_len, passed: i32) -> ()` | Records a pass/fail assertion against the current test run, the scripting equivalent of a DAG `ASSERTION` node. `passed` is `1` for pass, `0` for fail; any other value is treated as fail. |
//! | `env.sdk_log` | `(level: i32, message_ptr: i32, message_len: i32) -> ()` | Appends a structured log line, later surfaced through this worker's normal OTLP telemetry rather than a real stdout/stderr (guests have neither — see [`crate::sandbox`]'s safety model). `level` is a [`SdkLogLevel`] discriminant. |
//!
//! # String-passing convention
//!
//! Every string parameter/return follows [`WasmSandboxHost`]'s existing calling convention
//! (documented on that type): the guest owns memory allocation via its exported
//! `sandbox_alloc(len: i32) -> i32`, and pointer/length pairs — packed as `(ptr << 32) | len`
//! for return values — are how host and guest exchange byte buffers without either side
//! assuming anything about the other's internal memory layout.
//!
//! [`WasmSandboxHost`]: crate::sandbox::WasmSandboxHost

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use wasmtime::{Caller, Linker, Memory};

/// Sentinel packed return value for `env.sdk_get_variable` when the requested variable has
/// never been set. Distinguishable from every real packed pointer/length because a guest's
/// linear memory can never legitimately claim to start at `u32::MAX`.
pub const SDK_VARIABLE_NOT_FOUND: i64 = -1;

/// Discriminant passed to `env.sdk_log`'s `level` parameter, mirroring the handful of severities
/// this platform's OTel Collector pipeline (see `ARCHITECTURE.md`'s telemetry contract)
/// distinguishes for log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkLogLevel {
    Info,
    Warn,
    Error,
}

impl SdkLogLevel {
    fn from_guest_i32(raw: i32) -> Self {
        match raw {
            1 => SdkLogLevel::Warn,
            2 => SdkLogLevel::Error,
            // 0, and anything else a misbehaving/future guest sends, defaults to Info rather
            // than trapping the whole execution over a malformed log level — a script's logging
            // call should never be why the script fails.
            _ => SdkLogLevel::Info,
        }
    }
}

/// One recorded `env.sdk_log` call.
#[derive(Debug, Clone)]
pub struct SdkLogLine {
    pub level: SdkLogLevel,
    pub message: String,
}

/// One recorded `env.sdk_assert` call — the scripting-SDK equivalent of a DAG `ASSERTION` node's
/// outcome, but authored inline in a pre/post-request script rather than as a separate DAG node.
#[derive(Debug, Clone)]
pub struct SdkAssertionOutcome {
    pub description: String,
    pub passed: bool,
}

/// The HTTP request (and, for a post-request script, response) a script is attached to.
/// Serialized to JSON and handed to the guest via `env.sdk_http_context` — JSON rather than a
/// bespoke binary layout so the same guest ABI works whether the script was compiled from Rust,
/// AssemblyScript, or TinyGo (see [`crate::sandbox::wasm_sandbox_host`]'s module doc comment on
/// why this crate's guest ABI avoids requiring host-specific tooling).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpContext {
    pub method: String,
    pub url: String,
    pub request_headers: HashMap<String, String>,
    pub request_body: Option<String>,
    /// `None` for a pre-request script, since the response does not exist yet.
    pub response_status: Option<u16>,
    pub response_headers: HashMap<String, String>,
    pub response_body: Option<String>,
}

/// Per-execution state the host functions in this module read and mutate. One
/// `ScriptExecutionContext` is created per script invocation (or, for the "extract from
/// response, inject into next request" pattern documented in README.md, threaded across the
/// pre-request and post-request script pair for one HTTP call via the `variables` map), mirroring
/// [`crate::sandbox::WasmSandboxHost::execute`]'s "brand-new `Store` per execution" isolation —
/// nothing here is shared across unrelated scripts.
#[derive(Debug, Clone, Default)]
pub struct ScriptExecutionContext {
    /// Variables set via `env.sdk_set_variable` and read via `env.sdk_get_variable`. Seeded,
    /// before a script runs, with whatever variables a prior script in the same test run
    /// produced — threading that continuity through is the caller's responsibility (typically
    /// the DAG node executor that owns the request/script pair), not this module's.
    pub variables: HashMap<String, String>,
    pub http_context: HttpContext,
    pub assertions: Vec<SdkAssertionOutcome>,
    pub log_lines: Vec<SdkLogLine>,
}

impl ScriptExecutionContext {
    pub fn new(http_context: HttpContext) -> Self {
        Self {
            variables: HashMap::new(),
            http_context,
            assertions: Vec::new(),
            log_lines: Vec::new(),
        }
    }

    /// Convenience constructor seeded with variables carried over from a prior script in the
    /// same test run (the "extract JWT from response, inject into next request" pattern — see
    /// README.md's worked example).
    pub fn with_carried_variables(http_context: HttpContext, variables: HashMap<String, String>) -> Self {
        Self {
            variables,
            http_context,
            assertions: Vec::new(),
            log_lines: Vec::new(),
        }
    }

    /// Reports whether every assertion this script recorded passed. A script with no assertions
    /// at all is considered passing — matching a DAG `ASSERTION` node's absence meaning
    /// "nothing to check here", not "implicitly failed".
    pub fn all_assertions_passed(&self) -> bool {
        self.assertions.iter().all(|a| a.passed)
    }
}

/// Reads `len` bytes at `ptr` out of the guest's exported linear memory.
fn read_guest_bytes<T>(caller: &mut Caller<'_, T>, ptr: i32, len: i32) -> anyhow::Result<Vec<u8>> {
    let memory = guest_memory(caller)?;
    let mut buf = vec![0u8; len.max(0) as usize];
    memory
        .read(&mut *caller, ptr as usize, &mut buf)
        .map_err(|e| anyhow::anyhow!("scripting sdk: failed to read {len} byte(s) from guest memory at offset {ptr}: {e}"))?;
    Ok(buf)
}

/// Reads `len` UTF-8 bytes at `ptr` out of guest memory and validates them as a `String`.
fn read_guest_string<T>(caller: &mut Caller<'_, T>, ptr: i32, len: i32) -> anyhow::Result<String> {
    let bytes = read_guest_bytes(caller, ptr, len)?;
    String::from_utf8(bytes).map_err(|e| anyhow::anyhow!("scripting sdk: guest string at offset {ptr} is not valid UTF-8: {e}"))
}

/// Allocates `bytes.len()` bytes inside the guest (via its exported `sandbox_alloc`), copies
/// `bytes` into that region, and returns the packed `(ptr << 32) | len` pair the guest ABI uses
/// to describe a host-produced buffer — the same packing convention
/// [`crate::sandbox::WasmSandboxHost::execute`] uses for the entry point's own return value.
fn write_guest_bytes<T>(caller: &mut Caller<'_, T>, bytes: &[u8]) -> anyhow::Result<i64> {
    let alloc_export = caller
        .get_export("sandbox_alloc")
        .and_then(|e| e.into_func())
        .ok_or_else(|| anyhow::anyhow!("scripting sdk: guest module does not export 'sandbox_alloc'"))?;
    let alloc = alloc_export
        .typed::<i32, i32>(&caller)
        .map_err(|e| anyhow::anyhow!("scripting sdk: guest 'sandbox_alloc' has an incompatible signature: {e}"))?;
    let ptr = alloc.call(&mut *caller, bytes.len() as i32)?;

    let memory = guest_memory(caller)?;
    memory
        .write(&mut *caller, ptr as usize, bytes)
        .map_err(|e| anyhow::anyhow!("scripting sdk: failed to write {} byte(s) into guest memory at offset {ptr}: {e}", bytes.len()))?;

    Ok(((ptr as i64) << 32) | (bytes.len() as i64 & 0xFFFF_FFFF))
}

fn guest_memory<T>(caller: &mut Caller<'_, T>) -> anyhow::Result<Memory> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| anyhow::anyhow!("scripting sdk: guest module does not export linear memory named 'memory'"))
}

/// Wires every `env.sdk_*` host function described at the module level into `linker`, so any
/// guest instantiated through it gains the scripting SDK surface. `context_accessor` extracts
/// the [`ScriptExecutionContext`] this execution should read/mutate from the store's `T` — the
/// same "narrow accessor closure" pattern [`crate::sandbox::wasm_sandbox_host`] uses for its own
/// `preview1::add_to_linker_sync` call, so wiring this alongside WASI in one `Store<T>` needs no
/// new pattern, just a `T` whose state includes a `ScriptExecutionContext` field.
///
/// NOTE: `WasmSandboxHost::execute`'s current `SandboxGuestState` holds only a `WasiP1Ctx` and
/// has no `ScriptExecutionContext` field yet — threading a script's variables/http-context/
/// assertions/log output through `WasmSandboxHost` end to end (extending `SandboxGuestState`,
/// calling `register_into_linker` alongside `preview1::add_to_linker_sync`, and surfacing
/// `ScriptExecutionContext`'s final contents in `SandboxExecutionOutcome`) is the real
/// integration step left for when a pre/post-request-script DAG node type actually drives this
/// crate — this function is complete, tested-against-the-real-wasmtime-API host-function
/// wiring, ready for that integration, matching this repository's established pattern of
/// documenting-a-seam rather than force-fitting an unrelated struct today (see e.g.
/// `synthetic_payload_generator.rs`'s NOTE about a future `host_generate_payload` import).
pub fn register_into_linker<T, F>(linker: &mut Linker<T>, context_accessor: F) -> anyhow::Result<()>
where
    T: 'static,
    F: Fn(&mut T) -> &mut ScriptExecutionContext + Send + Sync + Copy + 'static,
{
    linker.func_wrap(
        "env",
        "sdk_get_variable",
        move |mut caller: Caller<'_, T>, name_ptr: i32, name_len: i32| -> anyhow::Result<i64> {
            let name = read_guest_string(&mut caller, name_ptr, name_len)?;
            let value = context_accessor(caller.data_mut()).variables.get(&name).cloned();
            match value {
                Some(v) => write_guest_bytes(&mut caller, v.as_bytes()),
                None => Ok(SDK_VARIABLE_NOT_FOUND),
            }
        },
    )?;

    linker.func_wrap(
        "env",
        "sdk_set_variable",
        move |mut caller: Caller<'_, T>, name_ptr: i32, name_len: i32, value_ptr: i32, value_len: i32| -> anyhow::Result<()> {
            let name = read_guest_string(&mut caller, name_ptr, name_len)?;
            let value = read_guest_string(&mut caller, value_ptr, value_len)?;
            context_accessor(caller.data_mut()).variables.insert(name, value);
            Ok(())
        },
    )?;

    linker.func_wrap(
        "env",
        "sdk_http_context",
        move |mut caller: Caller<'_, T>| -> anyhow::Result<i64> {
            let json = serde_json::to_vec(&context_accessor(caller.data_mut()).http_context)
                .map_err(|e| anyhow::anyhow!("scripting sdk: failed to serialize HttpContext: {e}"))?;
            write_guest_bytes(&mut caller, &json)
        },
    )?;

    linker.func_wrap(
        "env",
        "sdk_assert",
        move |mut caller: Caller<'_, T>, description_ptr: i32, description_len: i32, passed: i32| -> anyhow::Result<()> {
            let description = read_guest_string(&mut caller, description_ptr, description_len)?;
            context_accessor(caller.data_mut()).assertions.push(SdkAssertionOutcome {
                description,
                passed: passed == 1,
            });
            Ok(())
        },
    )?;

    linker.func_wrap(
        "env",
        "sdk_log",
        move |mut caller: Caller<'_, T>, level: i32, message_ptr: i32, message_len: i32| -> anyhow::Result<()> {
            let message = read_guest_string(&mut caller, message_ptr, message_len)?;
            context_accessor(caller.data_mut()).log_lines.push(SdkLogLine {
                level: SdkLogLevel::from_guest_i32(level),
                message,
            });
            Ok(())
        },
    )?;

    Ok(())
}
