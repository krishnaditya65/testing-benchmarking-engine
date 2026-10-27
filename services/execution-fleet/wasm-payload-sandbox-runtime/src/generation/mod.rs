//! Synthetic payload generation used to produce unique, structured request
//! bodies at load-test scale.

mod synthetic_payload_generator;

pub use synthetic_payload_generator::{
    ChaCha20SyntheticPayloadGenerator, GeneratedPayload, PayloadSchemaHint,
    SyntheticPayloadGenerator,
};
