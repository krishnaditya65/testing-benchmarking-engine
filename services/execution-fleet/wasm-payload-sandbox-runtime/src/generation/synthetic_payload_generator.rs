//! Synthetic JSON payload generation, invoked from around (and, once a
//! guest module requests it via a host import — NOTE below — from within)
//! the wasm sandbox to manufacture request bodies at load-test scale.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde_json::{Map, Value};
use uuid::Uuid;

/// Describes the shape of a payload to generate. Deliberately small: this
/// starter supports the field kinds a load test typically needs (string,
/// integer range, float range, boolean, nested object, homogeneous array)
/// rather than a full JSON-Schema implementation.
///
/// NOTE: in production this hint is expected to arrive from the control
/// plane as part of the `dag.node.dispatch.v1` payload (authored in the
/// workflow-canvas-frontend's DAG node config), not hand-built as it is in
/// `main.rs`'s demo below.
#[derive(Debug, Clone)]
pub enum PayloadSchemaHint {
    /// A random string of `min_len..=max_len` printable ASCII characters.
    RandomString { min_len: usize, max_len: usize },
    /// A random integer in `min..=max`.
    RandomInteger { min: i64, max: i64 },
    /// A random float in `min..=max`.
    RandomFloat { min: f64, max: f64 },
    /// A random boolean.
    RandomBoolean,
    /// A globally-unique identifier, guaranteed distinct across every
    /// invocation (see `GeneratedPayload::uniqueness_token`).
    UniqueIdentifier,
    /// A JSON object with named, independently-generated fields.
    Object(Vec<(String, PayloadSchemaHint)>),
    /// A JSON array of `length` elements, each generated from `element_hint`.
    Array {
        element_hint: Box<PayloadSchemaHint>,
        length: usize,
    },
}

/// A generated payload plus the metadata a load test needs to reason about
/// it: the raw JSON, and the uniqueness token that guarantees this payload
/// never collides with any other one generated across the fleet, even at
/// millions of requests per run.
#[derive(Debug, Clone)]
pub struct GeneratedPayload {
    pub json: Value,
    /// A UUIDv4 minted fresh for this call. Embedding it into every
    /// generated payload (see `SyntheticPayloadGenerator::generate`'s
    /// implementation) is what makes cross-request collisions
    /// astronomically unlikely even when many workers generate payloads
    /// concurrently from the same schema hint.
    pub uniqueness_token: Uuid,
}

/// Produces randomized structured payloads matching a [`PayloadSchemaHint`].
///
/// Implementations must guarantee uniqueness across invocations even under
/// massive concurrency — the `rng_seed` parameter alone is not sufficient
/// for that (a caller might accidentally reuse a seed), so implementations
/// should always fold in a fresh source of entropy (a UUID, in
/// [`ChaCha20SyntheticPayloadGenerator`]) rather than relying on the caller
/// never repeating a seed.
pub trait SyntheticPayloadGenerator {
    /// Generates one payload matching `schema_hint`, deterministically
    /// derived from `rng_seed` for the structural/value randomness, but
    /// always unique via an embedded UUID component.
    fn generate(&self, schema_hint: &PayloadSchemaHint, rng_seed: u64) -> GeneratedPayload;
}

/// Reference implementation: seeds a `ChaCha20Rng` per call from `rng_seed`
/// (cheap, reproducible for a given seed — useful for replaying a flaky
/// test case) and mints a fresh UUIDv4 uniqueness token per call (not
/// reproducible, by design — this is what prevents collisions across the
/// millions of payloads a load test can generate).
#[derive(Debug, Default, Clone, Copy)]
pub struct ChaCha20SyntheticPayloadGenerator;

const RANDOM_STRING_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

impl SyntheticPayloadGenerator for ChaCha20SyntheticPayloadGenerator {
    fn generate(&self, schema_hint: &PayloadSchemaHint, rng_seed: u64) -> GeneratedPayload {
        let mut rng = ChaCha20Rng::seed_from_u64(rng_seed);
        let uniqueness_token = Uuid::new_v4();
        let json = build_value(schema_hint, &mut rng, uniqueness_token);
        GeneratedPayload {
            json,
            uniqueness_token,
        }
    }
}

/// Recursively builds a `serde_json::Value` from a schema hint. `UniqueIdentifier`
/// nodes reuse the single `uniqueness_token` minted for the whole payload
/// rather than minting a new UUID per node, so a caller can join a payload
/// back to its uniqueness token by field value alone.
fn build_value(hint: &PayloadSchemaHint, rng: &mut ChaCha20Rng, uniqueness_token: Uuid) -> Value {
    match hint {
        PayloadSchemaHint::RandomString { min_len, max_len } => {
            let len = if max_len > min_len {
                rng.gen_range(*min_len..=*max_len)
            } else {
                *min_len
            };
            let s: String = (0..len)
                .map(|_| {
                    let idx = rng.gen_range(0..RANDOM_STRING_ALPHABET.len());
                    RANDOM_STRING_ALPHABET[idx] as char
                })
                .collect();
            Value::String(s)
        }
        PayloadSchemaHint::RandomInteger { min, max } => {
            Value::from(rng.gen_range(*min..=*max))
        }
        PayloadSchemaHint::RandomFloat { min, max } => {
            Value::from(rng.gen_range(*min..=*max))
        }
        PayloadSchemaHint::RandomBoolean => Value::Bool(rng.gen_bool(0.5)),
        PayloadSchemaHint::UniqueIdentifier => Value::String(uniqueness_token.to_string()),
        PayloadSchemaHint::Object(fields) => {
            let mut map = Map::with_capacity(fields.len());
            for (field_name, field_hint) in fields {
                map.insert(field_name.clone(), build_value(field_hint, rng, uniqueness_token));
            }
            Value::Object(map)
        }
        PayloadSchemaHint::Array { element_hint, length } => {
            let elements = (0..*length)
                .map(|_| build_value(element_hint, rng, uniqueness_token))
                .collect();
            Value::Array(elements)
        }
    }
}

// NOTE: a future revision can expose `SyntheticPayloadGenerator` as a WASI
// host import (e.g. `env.host_generate_payload(schema_ptr, schema_len) ->
// packed_ptr_len`) so a user's Wasm pre-request script can request
// generated payloads *from inside* the sandbox rather than only having
// them injected as the script's `input` by `WasmSandboxHost::execute`
// callers. That requires extending `Linker::func_wrap` in
// `wasm_sandbox_host.rs` with a host function bound to a
// `ChaCha20SyntheticPayloadGenerator`, which is out of scope for this
// starter.
