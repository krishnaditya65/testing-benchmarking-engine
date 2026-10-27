//! `dag-ipld-benchmark-worker-rust`: an execution-fleet worker that
//! benchmarks IPLD/DAG block traversal and the cryptographic primitives
//! that sit on its hot path, and offers a bisection-based diagnostic for
//! isolating the root cause of flaky DAG runs.
//!
//! See the service `README.md` for how each module maps to the platform's
//! feature manifest, and `SCALING.md` for its horizontal-scaling and
//! failure-recovery characteristics.

/// IPLD block traversal profiling (CID resolution / link-hop timing).
pub mod traversal;

/// Cryptographic overhead profiling (multihash, signature, ZK-proof cost).
pub mod crypto;

/// DAG flakiness root-cause isolation (bisection over execution history).
pub mod diagnostics;
