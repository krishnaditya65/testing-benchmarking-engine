//! The wasmtime sandbox: turns untrusted `.wasm` bytes plus an input buffer
//! into a classified execution outcome, never trusting the guest with
//! filesystem, network, or unbounded CPU/time.

mod sandbox_execution_outcome;
mod wasm_sandbox_host;

pub use sandbox_execution_outcome::{SandboxExecutionError, SandboxExecutionOutcome};
pub use wasm_sandbox_host::WasmSandboxHost;
