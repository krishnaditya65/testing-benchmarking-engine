//! Result types returned by [`super::wasm_sandbox_host::WasmSandboxHost::execute`].

use std::time::Duration;

/// Successful (or at least "the module ran to completion without trapping")
/// result of one sandboxed Wasm execution.
#[derive(Debug, Clone)]
pub struct SandboxExecutionOutcome {
    /// Raw bytes returned by the guest's entry point (via its return-buffer
    /// convention — see `WasmSandboxHost::execute` doc comment).
    pub output_bytes: Vec<u8>,
    /// UTF-8-lossy capture of anything the guest wrote to stdout during
    /// execution. Guests have no filesystem/network, so this is their only
    /// observable side channel besides the return value.
    pub captured_stdout: String,
    /// Fuel consumed by this execution, as reported by `Store::fuel_consumed`.
    /// Useful for cost-attribution and for tuning the per-node fuel budget
    /// the control plane hands out via `dag.node.dispatch.v1`.
    pub fuel_consumed: u64,
    /// Wall-clock duration of the `Linker`-instantiate + entry-point-call
    /// span, independent of fuel (fuel measures *work*, this measures
    /// *time*, and the two can diverge under host contention).
    pub wall_clock_duration: Duration,
}

/// Every way a sandboxed execution can fail to produce a
/// [`SandboxExecutionOutcome`]. Kept distinct (rather than collapsed into
/// `anyhow::Error`) because the control plane needs to tell "the user's
/// script is buggy" (`Trapped`), "the user's script is too expensive"
/// (`FuelExhausted`), and "the user's script tried to escape the sandbox"
/// (`WasiAccessDenied`) apart for retry/backoff/quota decisions.
#[derive(Debug, thiserror::Error)]
pub enum SandboxExecutionError {
    /// The supplied bytes did not parse/validate as a Wasm module.
    #[error("module bytes failed validation: {0}")]
    InvalidModule(String),

    /// The module has no export named `entry_point` with the expected
    /// `(i32, i32) -> i32` shape (pointer/len in, pointer-to-result out).
    #[error("entry point '{entry_point}' not found or has an incompatible signature")]
    EntryPointNotFound { entry_point: String },

    /// The guest ran out of fuel before returning — either an infinite loop
    /// or a script that is simply doing more work than its budget allows.
    #[error("execution exhausted its fuel budget of {fuel_budget} units before completing")]
    FuelExhausted { fuel_budget: u64 },

    /// The guest trapped (e.g. unreachable, out-of-bounds memory access,
    /// integer overflow in a checked op, explicit `abort`).
    #[error("guest module trapped during execution: {0}")]
    Trapped(String),

    /// The guest attempted to call a WASI import this sandbox does not
    /// provide (filesystem, network, clock-set, etc.) — deny-by-default.
    #[error("guest attempted a denied WASI operation: {0}")]
    WasiAccessDenied(String),

    /// Host-side setup failure unrelated to the guest's behavior (engine
    /// construction, linker wiring, memory allocation).
    #[error("sandbox host error: {0}")]
    HostSetupFailed(String),
}
