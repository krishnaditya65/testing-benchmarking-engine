//! Smart payload fuzzing: boundary-value and type-confusion mutation of
//! request payloads, used to probe target robustness beyond happy-path
//! synthetic payloads.

mod smart_payload_fuzzer;

pub use smart_payload_fuzzer::{FuzzedVariant, PayloadFuzzer, SmartPayloadFuzzer};
