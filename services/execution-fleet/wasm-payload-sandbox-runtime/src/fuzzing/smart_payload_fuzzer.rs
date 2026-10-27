//! Boundary-value and type-confusion fuzzing of request payloads: given a
//! baseline payload that a test author knows is *valid*, produce a batch of
//! deliberately edge-case variants to probe how the target handles
//! malformed/extreme input.

use serde_json::Value;

/// Generates a batch of mutated variants of a baseline payload of type `T`.
///
/// Parameterized over `T` so the mutation *strategy* (boundary values, type
/// confusion, length extremes) can be reused across payload encodings, not
/// just JSON.
///
/// NOTE: a future `ProtobufFieldMutation`-driven implementation of this
/// trait (`impl PayloadFuzzer<prost_types::DynamicMessage>` or similar) is
/// the intended way to fuzz binary gRPC payloads, reusing the dynamic
/// Protobuf descriptors already loaded by
/// `execution-fleet/protocol-load-worker-go`'s `internal/protocolclient`
/// package for its gRPC reflection support. That requires decoding
/// FileDescriptorSet bytes on the Rust side (e.g. via `prost-reflect`) and
/// is out of scope for this starter — JSON is the only payload encoding
/// implemented here.
pub trait PayloadFuzzer<T> {
    /// Produces mutated variants of `baseline`. Each variant is paired with
    /// a short, human-readable label describing what was mutated, so test
    /// reports can say *why* a given variant was generated.
    fn fuzz(&self, baseline: &T) -> Vec<FuzzedVariant<T>>;
}

/// One mutated payload plus a description of the mutation that produced it.
#[derive(Debug, Clone)]
pub struct FuzzedVariant<T> {
    pub label: String,
    pub payload: T,
}

/// Real boundary-value/type-confusion fuzzer for `serde_json::Value`
/// payloads. Walks every leaf of the baseline document and, for each leaf,
/// emits one mutated copy of the whole document with just that leaf
/// replaced — so each variant isolates a single mutation, which is what
/// makes a subsequent test failure attributable to a specific field.
#[derive(Debug, Default, Clone, Copy)]
pub struct SmartPayloadFuzzer;

/// Integer boundary values every numeric leaf is mutated to, one variant
/// per value.
const INTEGER_BOUNDARY_VALUES: [i64; 6] = [
    i32::MAX as i64,
    i32::MIN as i64,
    i64::MAX,
    i64::MIN,
    0,
    -1,
];

/// String-length boundary cases: empty, single character, and a string long
/// enough to trip naive fixed-size buffers server-side.
const VERY_LONG_STRING_LEN: usize = 65_536;

/// Unicode/injection-character edge cases known to break naive string
/// handling: null byte, RTL override, zero-width space, emoji (multi-byte
/// UTF-8 + grapheme-cluster edge case), and classic injection payloads.
const INJECTION_STRING_VARIANTS: [&str; 6] = [
    "\u{0000}",
    "\u{202E}reversed",
    "\u{200B}zero-width",
    "🧨💥",
    "'; DROP TABLE users; --",
    "<script>alert(1)</script>",
];

impl PayloadFuzzer<Value> for SmartPayloadFuzzer {
    fn fuzz(&self, baseline: &Value) -> Vec<FuzzedVariant<Value>> {
        let mut variants = Vec::new();
        mutate_paths(baseline, &mut Vec::new(), &mut variants);
        variants.push(FuzzedVariant {
            label: "type-confusion: whole payload replaced with null".to_string(),
            payload: Value::Null,
        });
        variants.push(FuzzedVariant {
            label: "type-confusion: whole payload replaced with empty array".to_string(),
            payload: Value::Array(Vec::new()),
        });
        variants
    }
}

/// Walks `value` depth-first, and at every leaf (or array/object needing a
/// length-extreme mutation), clones the *root* document with that one
/// location replaced, pushing the result onto `out`. `path` tracks the
/// breadcrumb from root to the current node, purely for the variant label.
fn mutate_paths(root: &Value, path: &mut Vec<PathSegment>, out: &mut Vec<FuzzedVariant<Value>>) {
    let current = navigate(root, path);
    match current {
        Value::Number(n) if n.is_i64() || n.is_u64() => {
            for boundary in INTEGER_BOUNDARY_VALUES {
                push_variant(root, path, Value::from(boundary), "integer boundary", out);
            }
            push_variant(root, path, Value::String("not-a-number".to_string()), "type confusion (number -> string)", out);
            push_variant(root, path, Value::Null, "type confusion (number -> null)", out);
        }
        Value::Number(_) => {
            // Non-integer (floating point) numeric leaf: still worth type-confusion coverage,
            // just without the integer-specific INTEGER_BOUNDARY_VALUES set used above.
            push_variant(root, path, Value::from(0), "float boundary (zero)", out);
            push_variant(root, path, Value::String("not-a-number".to_string()), "type confusion (number -> string)", out);
            push_variant(root, path, Value::Null, "type confusion (number -> null)", out);
        }
        Value::String(_) => {
            push_variant(root, path, Value::String(String::new()), "empty string", out);
            push_variant(root, path, Value::String("x".repeat(VERY_LONG_STRING_LEN)), "very long string", out);
            for injected in INJECTION_STRING_VARIANTS {
                push_variant(root, path, Value::String(injected.to_string()), "unicode/injection edge case", out);
            }
            push_variant(root, path, Value::from(0), "type confusion (string -> number)", out);
            push_variant(root, path, Value::Null, "type confusion (string -> null)", out);
        }
        Value::Bool(current_bool) => {
            push_variant(root, path, Value::Bool(!current_bool), "boolean flip", out);
            push_variant(root, path, Value::String("true".to_string()), "type confusion (bool -> string)", out);
        }
        Value::Array(items) => {
            push_variant(root, path, Value::Array(Vec::new()), "array length extreme (empty)", out);
            if let Some(first) = items.first().cloned() {
                let oversized = std::iter::repeat(first).take(1024).collect();
                push_variant(root, path, Value::Array(oversized), "array length extreme (1024 elements)", out);
            }
            for index in 0..items.len() {
                path.push(PathSegment::Index(index));
                mutate_paths(root, path, out);
                path.pop();
            }
        }
        Value::Object(fields) => {
            let keys: Vec<String> = fields.keys().cloned().collect();
            for key in keys {
                path.push(PathSegment::Key(key));
                mutate_paths(root, path, out);
                path.pop();
            }
        }
        Value::Null => {
            push_variant(root, path, Value::String("was-null".to_string()), "type confusion (null -> string)", out);
        }
    }
}

/// Breadcrumb segment used to locate a mutation target within the root
/// document without holding a live mutable borrow of it.
#[derive(Debug, Clone)]
enum PathSegment {
    Key(String),
    Index(usize),
}

fn navigate<'a>(root: &'a Value, path: &[PathSegment]) -> &'a Value {
    let mut current = root;
    for segment in path {
        current = match (segment, current) {
            (PathSegment::Key(key), Value::Object(map)) => map.get(key).expect("path built from live keys"),
            (PathSegment::Index(idx), Value::Array(items)) => &items[*idx],
            _ => unreachable!("path segment does not match document shape"),
        };
    }
    current
}

/// Clones `root`, replaces the value at `path` with `replacement`, and
/// pushes the resulting document onto `out` as a labeled variant.
fn push_variant(
    root: &Value,
    path: &[PathSegment],
    replacement: Value,
    mutation_kind: &str,
    out: &mut Vec<FuzzedVariant<Value>>,
) {
    let mut mutated = root.clone();
    set_at_path(&mut mutated, path, replacement);
    let path_label = if path.is_empty() {
        "$".to_string()
    } else {
        path.iter()
            .map(|segment| match segment {
                PathSegment::Key(key) => format!(".{key}"),
                PathSegment::Index(index) => format!("[{index}]"),
            })
            .collect::<String>()
    };
    out.push(FuzzedVariant {
        label: format!("{path_label}: {mutation_kind}"),
        payload: mutated,
    });
}

fn set_at_path(root: &mut Value, path: &[PathSegment], replacement: Value) {
    let mut current = root;
    for segment in path {
        current = match (segment, current) {
            (PathSegment::Key(key), Value::Object(map)) => {
                map.get_mut(key).expect("path built from live keys")
            }
            (PathSegment::Index(idx), Value::Array(items)) => &mut items[*idx],
            _ => unreachable!("path segment does not match document shape"),
        };
    }
    *current = replacement;
}
