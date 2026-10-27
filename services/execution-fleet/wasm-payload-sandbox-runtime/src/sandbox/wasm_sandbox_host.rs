//! [`WasmSandboxHost`]: the wasmtime embedding that runs a user's untrusted
//! pre/post-request script (or payload-generation script) to completion, or
//! fails it safely.
//!
//! # Guest ABI contract
//!
//! A guest module targeting `wasm32-wasi` is expected to export:
//! - `memory` — the module's linear memory (standard for `wasm32-wasi`).
//! - `sandbox_alloc(len: i32) -> i32` — allocate `len` bytes inside guest
//!   memory and return the offset. Used by the host to copy `input` in
//!   before calling the entry point.
//! - the entry point itself, with signature `(ptr: i32, len: i32) -> i64`,
//!   where the guest reads its input from `[ptr, ptr+len)` and returns a
//!   packed `(output_ptr << 32) | output_len` describing where its output
//!   bytes live in guest memory.
//!
//! This is a deliberately minimal calling convention (no host-side
//! WIT/component tooling required) so a script author can compile a small
//! Rust/AssemblyScript/TinyGo module against it directly.
//!
//! # Safety model
//!
//! - **Fuel-bounded**: every execution gets a finite fuel budget
//!   (`Config::consume_fuel(true)` + `Store::set_fuel`); an infinite loop
//!   burns fuel and traps instead of hanging a worker thread forever.
//! - **Epoch-bounded**: a background ticker thread increments the shared
//!   `Engine`'s epoch on a fixed interval, giving a wall-clock backstop
//!   independent of how expensive individual Wasm opcodes are metered.
//! - **WASI deny-by-default**: the `WasiCtxBuilder` we construct grants no
//!   preopened directories and no sockets. Only stdout is wired, and it is
//!   wired to an in-memory pipe the host reads back — never to the real
//!   process stdout, and never to the filesystem or network.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
use wasmtime_wasi::pipe::MemoryOutputPipe;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

use super::sandbox_execution_outcome::{SandboxExecutionError, SandboxExecutionOutcome};

/// How often the epoch ticker increments the engine's epoch. Combined with
/// `epoch_deadline_ticks` on a per-store basis, this bounds wall-clock
/// execution time as a backstop to fuel metering (fuel bounds *work*, epoch
/// bounds *time*, and a script that's mostly waiting on host-call overhead
/// could otherwise burn little fuel while still running unboundedly long).
const EPOCH_TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Maximum bytes of stdout a guest may buffer before further writes are
/// silently dropped by the pipe. Prevents a chatty/malicious guest from
/// exhausting host memory via stdout alone.
const MAX_CAPTURED_STDOUT_BYTES: usize = 64 * 1024;

/// Per-execution host state handed to `Store<T>`. Holds nothing but the WASI
/// context — the sandbox is intentionally stateless across executions, so
/// nothing here survives past a single `execute()` call.
struct SandboxGuestState {
    wasi: WasiP1Ctx,
}

/// Embeds wasmtime to run user-supplied `.wasm` modules (pre/post-request
/// scripts, in-sandbox payload-generation logic) under a fuel- and
/// epoch-bounded, WASI-deny-by-default sandbox.
///
/// One `WasmSandboxHost` is created per worker process and reused across
/// many executions — the `Engine` is expensive to build (it JIT-compiles
/// configuration and owns the epoch ticker thread) but `Store`s, which hold
/// per-execution guest state, are cheap and short-lived by design.
pub struct WasmSandboxHost {
    engine: Engine,
    /// Fuel units granted to each execution. One unit is roughly one Wasm
    /// instruction's worth of metered work; wasmtime's fuel accounting is
    /// coarse by design (cheap to check, not a cycle-accurate cost model).
    default_fuel_budget: u64,
    /// Wall-clock backstop: number of epoch ticks (see `EPOCH_TICK_INTERVAL`)
    /// an execution may run before wasmtime interrupts it regardless of
    /// remaining fuel.
    epoch_deadline_ticks: u64,
    epoch_ticker: EpochTicker,
}

/// Owns the background thread that periodically calls `Engine::increment_epoch`.
/// Stopped and joined on drop so the sandbox host leaves no dangling threads
/// behind when a worker shuts down.
struct EpochTicker {
    stop_flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl EpochTicker {
    fn spawn(engine: Engine, tick_interval: Duration) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_thread = Arc::clone(&stop_flag);
        let handle = std::thread::Builder::new()
            .name("wasm-sandbox-epoch-ticker".to_string())
            .spawn(move || {
                while !stop_flag_for_thread.load(Ordering::Relaxed) {
                    std::thread::sleep(tick_interval);
                    engine.increment_epoch();
                }
            })
            .expect("failed to spawn epoch ticker thread");
        Self {
            stop_flag,
            handle: Some(handle),
        }
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl WasmSandboxHost {
    /// Builds the sandbox host. `default_fuel_budget` and
    /// `epoch_deadline_ticks` are the platform-wide defaults; the control
    /// plane may in the future ship per-node overrides in the
    /// `dag.node.dispatch.v1` payload, at which point `execute_with_fuel`
    /// below should grow a caller-supplied budget parameter.
    pub fn new(
        default_fuel_budget: u64,
        epoch_deadline_ticks: u64,
    ) -> Result<Self, SandboxExecutionError> {
        let mut config = Config::new();
        config
            .consume_fuel(true)
            .epoch_interruption(true)
            // Guests never share memory with the host process; wasmtime's
            // default (non-shared, bounds-checked) linear memory is exactly
            // what an untrusted-script sandbox wants.
            .wasm_threads(false)
            .wasm_simd(true);

        let engine = Engine::new(&config)
            .map_err(|e| SandboxExecutionError::HostSetupFailed(e.to_string()))?;

        let epoch_ticker = EpochTicker::spawn(engine.clone(), EPOCH_TICK_INTERVAL);

        Ok(Self {
            engine,
            default_fuel_budget,
            epoch_deadline_ticks,
            epoch_ticker,
        })
    }

    /// Runs `module_bytes` in an isolated `Store`, invoking `entry_point`
    /// with `input`, and returns either the execution's output or a
    /// classified failure.
    ///
    /// Every call gets a brand-new `Store<SandboxGuestState>` — the sandbox
    /// holds no state across invocations, so a compromised or buggy guest in
    /// one execution cannot taint the next.
    pub fn execute(
        &self,
        module_bytes: &[u8],
        entry_point: &str,
        input: &[u8],
    ) -> Result<SandboxExecutionOutcome, SandboxExecutionError> {
        let started_at = Instant::now();

        // Cheap structural validation before we ask wasmtime to compile
        // (invalid section ordering, malformed LEB128 etc. get caught here
        // with a clearer error than wasmtime's compile-time one).
        wasmparser::validate(module_bytes)
            .map_err(|e| SandboxExecutionError::InvalidModule(e.to_string()))?;

        let module = Module::new(&self.engine, module_bytes)
            .map_err(|e| SandboxExecutionError::InvalidModule(e.to_string()))?;

        let stdout_pipe = MemoryOutputPipe::new(MAX_CAPTURED_STDOUT_BYTES);

        // Deny-by-default WASI context: no preopened directories, no
        // sockets, no env vars, no inherited args. Only stdout is wired,
        // and it is wired to an in-memory pipe the host reads back after
        // execution rather than the real process's stdout.
        let wasi = WasiCtxBuilder::new()
            .stdout(stdout_pipe.clone())
            .build_p1();

        let mut store = Store::new(&self.engine, SandboxGuestState { wasi });
        store
            .set_fuel(self.default_fuel_budget)
            .map_err(|e| SandboxExecutionError::HostSetupFailed(e.to_string()))?;
        store.set_epoch_deadline(self.epoch_deadline_ticks);

        let mut linker: Linker<SandboxGuestState> = Linker::new(&self.engine);
        preview1::add_to_linker_sync(&mut linker, |state: &mut SandboxGuestState| &mut state.wasi)
            .map_err(|e| SandboxExecutionError::HostSetupFailed(e.to_string()))?;

        let instance = match linker.instantiate(&mut store, &module) {
            Ok(instance) => instance,
            Err(err) => return Err(self.classify_wasmtime_error(err, &store)),
        };

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| SandboxExecutionError::HostSetupFailed(
                "guest module does not export linear memory named 'memory'".to_string(),
            ))?;

        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "sandbox_alloc")
            .map_err(|_| SandboxExecutionError::EntryPointNotFound {
                entry_point: "sandbox_alloc".to_string(),
            })?;

        let entry = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, entry_point)
            .map_err(|_| SandboxExecutionError::EntryPointNotFound {
                entry_point: entry_point.to_string(),
            })?;

        // Copy `input` into guest memory via the guest's own allocator so
        // we never assume anything about its internal memory layout.
        let input_ptr = match alloc.call(&mut store, input.len() as i32) {
            Ok(ptr) => ptr,
            Err(err) => return Err(self.classify_wasmtime_error(err, &store)),
        };
        memory
            .write(&mut store, input_ptr as usize, input)
            .map_err(|e| SandboxExecutionError::HostSetupFailed(e.to_string()))?;

        let packed_result = match entry.call(&mut store, (input_ptr, input.len() as i32)) {
            Ok(packed) => packed,
            Err(err) => return Err(self.classify_wasmtime_error(err, &store)),
        };

        let output_ptr = (packed_result >> 32) as u32 as usize;
        let output_len = (packed_result & 0xFFFF_FFFF) as u32 as usize;
        let mut output_bytes = vec![0u8; output_len];
        memory
            .read(&store, output_ptr, &mut output_bytes)
            .map_err(|e| SandboxExecutionError::HostSetupFailed(e.to_string()))?;

        let fuel_consumed = self.default_fuel_budget - store.get_fuel().unwrap_or(0);
        let captured_stdout = String::from_utf8_lossy(&stdout_pipe.contents()).into_owned();

        Ok(SandboxExecutionOutcome {
            output_bytes,
            captured_stdout,
            fuel_consumed,
            wall_clock_duration: started_at.elapsed(),
        })
    }

    /// Turns an opaque `anyhow::Error` from wasmtime into one of our
    /// distinctly-classified `SandboxExecutionError` variants so callers
    /// (and the control plane, over telemetry) can tell "out of fuel" from
    /// "guest trapped" from "guest tried something WASI denied".
    fn classify_wasmtime_error(
        &self,
        err: anyhow::Error,
        store: &Store<SandboxGuestState>,
    ) -> SandboxExecutionError {
        if let Some(trap) = err.downcast_ref::<Trap>() {
            return match trap {
                Trap::OutOfFuel => SandboxExecutionError::FuelExhausted {
                    fuel_budget: self.default_fuel_budget,
                },
                Trap::Interrupt => SandboxExecutionError::FuelExhausted {
                    // Epoch-triggered interrupt: treat the same as fuel
                    // exhaustion from the caller's point of view — the
                    // execution ran too long and was cut off.
                    fuel_budget: self.default_fuel_budget,
                },
                other => SandboxExecutionError::Trapped(other.to_string()),
            };
        }

        let message = err.to_string();
        if message.contains("wasi") || message.contains("not permitted") || message.contains("denied") {
            return SandboxExecutionError::WasiAccessDenied(message);
        }

        // `store` is accepted so future revisions can attach fuel-remaining
        // context to the trap message without changing this fn's signature.
        let _ = store;
        SandboxExecutionError::Trapped(message)
    }
}
