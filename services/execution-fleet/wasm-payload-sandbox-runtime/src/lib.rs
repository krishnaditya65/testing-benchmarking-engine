//! `wasm-payload-sandbox-runtime` - wasmtime host embedding for user
//! pre/post-request scripts and in-sandbox synthetic payload generation.
//!
//! This crate is one member of the `execution-fleet` layer described in
//! `ARCHITECTURE.md`: a stateless, horizontally scalable worker that takes
//! orders from the orchestration control plane over Redpanda's
//! `dag.node.dispatch.v1` topic and emits OTLP telemetry outward to the
//! OTel Collector. It never calls other workers directly, and it is the
//! fleet's designated home for executing *untrusted* user-authored logic —
//! everything in [`sandbox`] exists to make that safe.
//!
//! Module map:
//! - [`sandbox`] - the wasmtime embedding (`WasmSandboxHost`) that runs a
//!   user's `.wasm` module under fuel/epoch limits with WASI access denied
//!   by default.
//! - [`generation`] - `SyntheticPayloadGenerator`, producing structured,
//!   collision-free JSON payloads at load-test scale.
//! - [`fuzzing`] - `SmartPayloadFuzzer`, producing boundary-value and
//!   type-confusion mutations of a baseline payload.
//! - [`sdk`] - the typed `env.sdk_*` host-function surface (`get_variable`,
//!   `set_variable`, `http_context`, `assert`, `log`) user pre/post-request
//!   scripts are written against, plus `ScriptManifest` for publishing them.
//! - [`marketplace`] - `SnippetRegistryClient`, the port/adapter for
//!   publishing and fetching shared scripting-SDK snippets.

pub mod fuzzing;
pub mod generation;
pub mod marketplace;
pub mod sandbox;
pub mod sdk;
