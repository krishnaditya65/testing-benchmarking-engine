//! Entry point for `wasm-payload-sandbox-runtime`.
//!
//! Wires [`WasmSandboxHost`], [`ChaCha20SyntheticPayloadGenerator`], and
//! [`SmartPayloadFuzzer`] together end to end: generate a unique synthetic
//! payload, fuzz it into boundary-value/type-confusion variants, and — if a
//! `.wasm` fixture was supplied — run one variant through a user script
//! inside the sandbox and report the classified outcome.
//!
//! NOTE: in production, this binary does not take `--wasm-module-path` on
//! the command line at all. It instead consumes `dag.node.dispatch.v1`
//! messages from Redpanda, each of which carries a reference to the
//! user-supplied `.wasm` artifact (resolved against an artifact store /
//! object storage bucket keyed by content hash) plus the entry point name
//! and any schema hints for payload generation. The CLI flags here exist so
//! this crate is runnable and demonstrable in isolation during local
//! development, without a live control plane or Redpanda cluster.

use std::path::PathBuf;

use clap::Parser;
use tracing::{info, warn};

use wasm_payload_sandbox_runtime::fuzzing::{PayloadFuzzer, SmartPayloadFuzzer};
use wasm_payload_sandbox_runtime::generation::{
    ChaCha20SyntheticPayloadGenerator, PayloadSchemaHint, SyntheticPayloadGenerator,
};
use wasm_payload_sandbox_runtime::sandbox::{SandboxExecutionError, WasmSandboxHost};

/// Local-development entry point for the wasm sandbox / payload
/// generation / fuzzing pipeline. See module-level NOTE for how this
/// differs from the production Redpanda-driven code path.
#[derive(Debug, Parser)]
#[command(name = "wasm-payload-sandbox-runtime")]
struct CliArgs {
    /// Path to a `.wasm` fixture implementing the guest ABI documented on
    /// `WasmSandboxHost` (exports `memory`, `sandbox_alloc`, and the entry
    /// point below). If omitted, the sandbox-execution step is skipped and
    /// only payload generation + fuzzing are demonstrated.
    #[arg(long, env = "SANDBOX_WASM_MODULE_PATH")]
    wasm_module_path: Option<PathBuf>,

    /// Exported guest function to invoke as the script entry point.
    #[arg(long, default_value = "run_pre_request_script")]
    entry_point: String,

    /// Fuel units granted to each sandboxed execution.
    #[arg(long, default_value_t = 5_000_000)]
    fuel_budget: u64,

    /// Wall-clock backstop, expressed as epoch ticks (10ms each — see
    /// `EPOCH_TICK_INTERVAL` in `wasm_sandbox_host.rs`). 500 ticks ≈ 5s.
    #[arg(long, default_value_t = 500)]
    epoch_deadline_ticks: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = CliArgs::parse();

    // Step 1: generate a unique synthetic payload from a demo schema hint.
    //
    // NOTE: in production this hint is decoded from the `dag.node.dispatch.v1`
    // message body (see `shared-libraries/protocol-contracts/proto/dag_dispatch.proto`),
    // not hand-built here.
    let payload_generator = ChaCha20SyntheticPayloadGenerator;
    let schema_hint = PayloadSchemaHint::Object(vec![
        ("request_id".to_string(), PayloadSchemaHint::UniqueIdentifier),
        (
            "user_email".to_string(),
            PayloadSchemaHint::RandomString {
                min_len: 8,
                max_len: 24,
            },
        ),
        (
            "amount_cents".to_string(),
            PayloadSchemaHint::RandomInteger { min: 0, max: 1_000_000 },
        ),
        (
            "is_retry".to_string(),
            PayloadSchemaHint::RandomBoolean,
        ),
        (
            "tags".to_string(),
            PayloadSchemaHint::Array {
                element_hint: Box::new(PayloadSchemaHint::RandomString {
                    min_len: 3,
                    max_len: 10,
                }),
                length: 3,
            },
        ),
    ]);

    let rng_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let generated = payload_generator.generate(&schema_hint, rng_seed);
    info!(
        uniqueness_token = %generated.uniqueness_token,
        payload = %generated.json,
        "generated synthetic payload"
    );

    // Step 2: fuzz the generated payload into boundary-value / type-confusion variants.
    let fuzzer = SmartPayloadFuzzer;
    let variants = fuzzer.fuzz(&generated.json);
    info!(variant_count = variants.len(), "generated fuzzed payload variants");
    for variant in variants.iter().take(5) {
        info!(label = %variant.label, "sample fuzzed variant");
    }

    // Step 3: run a variant through the sandbox, if a .wasm fixture was supplied.
    match &args.wasm_module_path {
        Some(path) => {
            let sandbox_host = WasmSandboxHost::new(args.fuel_budget, args.epoch_deadline_ticks)?;
            let module_bytes = std::fs::read(path)?;
            let input_variant = variants
                .first()
                .map(|v| v.payload.clone())
                .unwrap_or(generated.json.clone());
            let input_bytes = serde_json::to_vec(&input_variant)?;

            match sandbox_host.execute(&module_bytes, &args.entry_point, &input_bytes) {
                Ok(outcome) => {
                    info!(
                        fuel_consumed = outcome.fuel_consumed,
                        wall_clock_ms = outcome.wall_clock_duration.as_millis(),
                        output_len = outcome.output_bytes.len(),
                        captured_stdout = %outcome.captured_stdout,
                        "sandbox execution completed"
                    );
                }
                Err(SandboxExecutionError::FuelExhausted { fuel_budget }) => {
                    warn!(fuel_budget, "sandbox execution exhausted its fuel budget");
                }
                Err(SandboxExecutionError::Trapped(message)) => {
                    warn!(%message, "sandbox execution trapped");
                }
                Err(SandboxExecutionError::WasiAccessDenied(message)) => {
                    warn!(%message, "sandbox execution attempted a denied WASI operation");
                }
                Err(other) => {
                    warn!(error = %other, "sandbox execution failed");
                }
            }
        }
        None => {
            // NOTE: no `.wasm` fixture was supplied via `--wasm-module-path` /
            // `SANDBOX_WASM_MODULE_PATH`. In a real deployment the module
            // bytes come from the artifact reference on the dispatched DAG
            // node, resolved against the control plane's artifact store —
            // there is no "generate a trivial module at runtime" path in
            // production, so this starter does not fabricate one either. To
            // exercise the sandbox locally, compile a module against the
            // ABI documented on `WasmSandboxHost` (e.g. a small
            // `wasm32-wasi` Rust crate exporting `memory`, `sandbox_alloc`,
            // and `run_pre_request_script`) and pass its path here.
            info!(
                "no --wasm-module-path supplied; skipping sandbox execution demo \
                 (payload generation and fuzzing above still ran end to end)"
            );
        }
    }

    Ok(())
}
