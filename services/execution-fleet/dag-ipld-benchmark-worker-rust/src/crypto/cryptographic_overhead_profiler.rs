//! Measures, in isolation, the wall-clock cost of the three cryptographic
//! primitives a DAG/IPLD execution path typically pays for on every hop:
//! multihash computation, signature verification, and ZK-proof verification.
//! Each is also measured under concurrent load to surface thread-contention
//! and throughput degradation, since production dispatch runs many DAG
//! nodes' crypto work in parallel across worker threads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::traversal::ipld_block_traversal_profiler::LatencyStats;

/// Timing for a single cryptographic operation kind, aggregated across all
/// samples collected for it (single-threaded baseline).
#[derive(Debug, Clone, Serialize)]
pub struct OperationTiming {
    pub operation_name: &'static str,
    pub stats: LatencyStats,
}

/// Result of running an operation concurrently across `worker_thread_count`
/// threads, so per-operation contention/throughput degradation is visible
/// relative to the single-threaded baseline in `OperationTiming`.
#[derive(Debug, Clone, Serialize)]
pub struct ConcurrentLoadReport {
    pub operation_name: &'static str,
    pub worker_thread_count: usize,
    pub total_operations: u64,
    pub wall_clock_micros: u64,
    /// Operations completed per second across all threads combined.
    pub aggregate_throughput_ops_per_sec: f64,
    /// aggregate_throughput_ops_per_sec / worker_thread_count; a value well
    /// below the single-threaded throughput indicates lock/allocator/CPU
    /// contention rather than clean linear scaling.
    pub throughput_per_thread_ops_per_sec: f64,
}

/// Abstracts ZK-proof verification so the profiler can time it without
/// depending on a specific proving system.
///
/// NOTE: A production implementation would plug in a real proving-system
/// crate here — e.g. `ark-groth16` (`ark_groth16::verify_proof`) or
/// `halo2_proofs` verifier circuits — keyed by the proof system the DAG
/// execution layer actually anchors state transitions with. `NoOpZkProofVerifier`
/// below only models the *shape* of that cost (a fixed-ish CPU-bound
/// verification call) so the timing harness and concurrent-load harness are
/// exercised realistically without a heavyweight cryptographic dependency
/// in this starter repo.
pub trait ZkProofVerifier: Send + Sync {
    /// Verifies `proof_bytes` against `public_input`, returning whether it
    /// is valid. Implementations should perform real, non-trivial CPU work
    /// proportional to what a real verifier would do, so timings are
    /// meaningful even when mocked.
    fn verify(&self, proof_bytes: &[u8], public_input: &[u8]) -> bool;
}

/// A no-op-but-CPU-real `ZkProofVerifier`: it does not implement any actual
/// zero-knowledge proof system, but it performs a fixed number of hash
/// compressions to approximate the CPU cost shape of pairing-based proof
/// verification, so profiler output is a plausible stand-in rather than a
/// meaningless near-zero measurement.
///
/// NOTE: swap for a real verifier (see the `ZkProofVerifier` trait doc)
/// before using this profiler's ZK numbers for capacity planning.
pub struct NoOpZkProofVerifier {
    /// Number of sha2-256 compressions used to approximate verifier cost.
    simulated_compression_rounds: u32,
}

impl NoOpZkProofVerifier {
    pub fn new(simulated_compression_rounds: u32) -> Self {
        Self { simulated_compression_rounds }
    }
}

impl Default for NoOpZkProofVerifier {
    fn default() -> Self {
        // Chosen to approximate the rough order of magnitude of Groth16
        // pairing verification cost on commodity hardware; tune per proof
        // system when wiring in a real verifier.
        Self::new(400)
    }
}

impl ZkProofVerifier for NoOpZkProofVerifier {
    fn verify(&self, proof_bytes: &[u8], public_input: &[u8]) -> bool {
        let mut acc = [0u8; 32];
        for round in 0..self.simulated_compression_rounds {
            let mut hasher = Sha256::new();
            hasher.update(acc);
            hasher.update(proof_bytes);
            hasher.update(public_input);
            hasher.update(round.to_le_bytes());
            acc = hasher.finalize().into();
        }
        // A real verifier would return a pairing-equation check result;
        // this stand-in reports "valid" whenever the accumulator is
        // non-zero, which is always true here, so callers exercise the
        // success path deterministically.
        acc.iter().any(|&b| b != 0)
    }
}

/// Isolates and times the cryptographic primitives on a DAG node's hot
/// path: multihash computation, ed25519 signature verification, and
/// ZK-proof verification (via a pluggable `ZkProofVerifier`).
pub struct CryptographicOverheadProfiler<Z: ZkProofVerifier> {
    zk_proof_verifier: Z,
}

impl<Z: ZkProofVerifier> CryptographicOverheadProfiler<Z> {
    pub fn new(zk_proof_verifier: Z) -> Self {
        Self { zk_proof_verifier }
    }

    /// Times `sample_count` sha2-256 multihash computations over
    /// `payload_size_bytes`-sized random payloads, single-threaded.
    pub fn profile_multihash_computation(
        &self,
        sample_count: usize,
        payload_size_bytes: usize,
    ) -> OperationTiming {
        let payload = deterministic_payload(payload_size_bytes);
        let mut latencies_micros = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let started_at = Instant::now();
            let _digest = Sha256::digest(&payload);
            latencies_micros.push(started_at.elapsed().as_micros() as u64);
        }
        OperationTiming {
            operation_name: "multihash_sha2_256",
            stats: LatencyStats::from_micros(latencies_micros)
                .expect("sample_count must be > 0"),
        }
    }

    /// Times `sample_count` ed25519 signature verifications, single-threaded.
    /// Key generation and signing happen outside the timed region so only
    /// verification cost is captured.
    pub fn profile_signature_verification(&self, sample_count: usize) -> OperationTiming {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let message = deterministic_payload(256);
        let signature: Signature = signing_key.sign(&message);

        let mut latencies_micros = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let started_at = Instant::now();
            let is_valid = verifying_key.verify(&message, &signature).is_ok();
            latencies_micros.push(started_at.elapsed().as_micros() as u64);
            debug_assert!(is_valid, "profiler must exercise the success path");
        }
        OperationTiming {
            operation_name: "signature_verification_ed25519",
            stats: LatencyStats::from_micros(latencies_micros)
                .expect("sample_count must be > 0"),
        }
    }

    /// Times `sample_count` ZK-proof verifications via the configured
    /// `ZkProofVerifier`, single-threaded.
    pub fn profile_zk_proof_verification(&self, sample_count: usize) -> OperationTiming {
        let proof_bytes = deterministic_payload(192);
        let public_input = deterministic_payload(32);

        let mut latencies_micros = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let started_at = Instant::now();
            let _is_valid = self.zk_proof_verifier.verify(&proof_bytes, &public_input);
            latencies_micros.push(started_at.elapsed().as_micros() as u64);
        }
        OperationTiming {
            operation_name: "zk_proof_verification",
            stats: LatencyStats::from_micros(latencies_micros)
                .expect("sample_count must be > 0"),
        }
    }

    /// Runs `operations_per_thread` sha2-256 multihash computations on each
    /// of `worker_thread_count` OS threads concurrently and reports
    /// aggregate/per-thread throughput, revealing contention that a
    /// single-threaded measurement cannot.
    pub fn profile_multihash_under_concurrent_load(
        &self,
        worker_thread_count: usize,
        operations_per_thread: u64,
        payload_size_bytes: usize,
    ) -> ConcurrentLoadReport {
        let payload = Arc::new(deterministic_payload(payload_size_bytes));
        let completed_operations = Arc::new(AtomicU64::new(0));

        let wall_clock_start = Instant::now();
        thread::scope(|scope| {
            for _ in 0..worker_thread_count {
                let payload = Arc::clone(&payload);
                let completed_operations = Arc::clone(&completed_operations);
                scope.spawn(move || {
                    for _ in 0..operations_per_thread {
                        let _digest = Sha256::digest(payload.as_slice());
                        completed_operations.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        let wall_clock_elapsed = wall_clock_start.elapsed();

        build_concurrent_load_report(
            "multihash_sha2_256",
            worker_thread_count,
            completed_operations.load(Ordering::Relaxed),
            wall_clock_elapsed,
        )
    }

    /// Same as `profile_multihash_under_concurrent_load` but for signature
    /// verification: each thread verifies the same pre-computed signature
    /// repeatedly so only verification cost (not key generation/signing)
    /// contributes to the measured wall-clock time.
    pub fn profile_signature_verification_under_concurrent_load(
        &self,
        worker_thread_count: usize,
        operations_per_thread: u64,
    ) -> ConcurrentLoadReport {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = Arc::new(signing_key.verifying_key());
        let message = Arc::new(deterministic_payload(256));
        let signature = Arc::new(signing_key.sign(&message));
        let completed_operations = Arc::new(AtomicU64::new(0));

        let wall_clock_start = Instant::now();
        thread::scope(|scope| {
            for _ in 0..worker_thread_count {
                let verifying_key = Arc::clone(&verifying_key);
                let message = Arc::clone(&message);
                let signature = Arc::clone(&signature);
                let completed_operations = Arc::clone(&completed_operations);
                scope.spawn(move || {
                    for _ in 0..operations_per_thread {
                        let _ = verifying_key.verify(message.as_slice(), &signature).is_ok();
                        completed_operations.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        let wall_clock_elapsed = wall_clock_start.elapsed();

        build_concurrent_load_report(
            "signature_verification_ed25519",
            worker_thread_count,
            completed_operations.load(Ordering::Relaxed),
            wall_clock_elapsed,
        )
    }
}

fn build_concurrent_load_report(
    operation_name: &'static str,
    worker_thread_count: usize,
    total_operations: u64,
    wall_clock_elapsed: Duration,
) -> ConcurrentLoadReport {
    let wall_clock_micros = wall_clock_elapsed.as_micros() as u64;
    let wall_clock_seconds = wall_clock_elapsed.as_secs_f64().max(f64::EPSILON);
    let aggregate_throughput_ops_per_sec = total_operations as f64 / wall_clock_seconds;
    let throughput_per_thread_ops_per_sec =
        aggregate_throughput_ops_per_sec / worker_thread_count.max(1) as f64;

    ConcurrentLoadReport {
        operation_name,
        worker_thread_count,
        total_operations,
        wall_clock_micros,
        aggregate_throughput_ops_per_sec,
        throughput_per_thread_ops_per_sec,
    }
}

/// Builds a deterministic pseudo-random payload of `size_bytes`, avoiding an
/// extra RNG-timing dependency inside the timed regions above.
fn deterministic_payload(size_bytes: usize) -> Vec<u8> {
    (0..size_bytes).map(|i| (i % 256) as u8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multihash_profiling_produces_stats_for_every_sample() {
        let profiler = CryptographicOverheadProfiler::new(NoOpZkProofVerifier::default());
        let timing = profiler.profile_multihash_computation(50, 1024);
        assert_eq!(timing.stats.sample_count, 50);
    }

    #[test]
    fn signature_verification_profiling_produces_stats_for_every_sample() {
        let profiler = CryptographicOverheadProfiler::new(NoOpZkProofVerifier::default());
        let timing = profiler.profile_signature_verification(20);
        assert_eq!(timing.stats.sample_count, 20);
    }

    #[test]
    fn zk_proof_verifier_reports_valid_for_mock_proofs() {
        let verifier = NoOpZkProofVerifier::new(8);
        assert!(verifier.verify(b"proof", b"public-input"));
    }

    #[test]
    fn concurrent_multihash_load_completes_all_operations() {
        let profiler = CryptographicOverheadProfiler::new(NoOpZkProofVerifier::default());
        let report = profiler.profile_multihash_under_concurrent_load(4, 25, 512);
        assert_eq!(report.total_operations, 100);
        assert_eq!(report.worker_thread_count, 4);
    }
}
