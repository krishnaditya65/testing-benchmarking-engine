//! Cryptographic overhead profiling: isolates and measures the per-operation
//! cost of the hashing, signature verification, and ZK-proof verification
//! primitives that sit on the hot path of real IPLD/DAG execution, including
//! how that cost degrades under concurrent load.

pub mod cryptographic_overhead_profiler;

pub use cryptographic_overhead_profiler::{
    ConcurrentLoadReport, CryptographicOverheadProfiler, NoOpZkProofVerifier, OperationTiming,
    ZkProofVerifier,
};
