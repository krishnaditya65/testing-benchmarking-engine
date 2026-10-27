//! Entrypoint for the `dag-ipld-benchmark-worker` process.
//!
//! In production this worker is dispatched by the orchestration control
//! plane's `dag-workflow-scheduler` over the Redpanda `dag.node.dispatch.v1`
//! topic (see ARCHITECTURE.md § Communication contracts), runs the
//! requested benchmark against the DAG/IPLD payload named in that dispatch
//! message, and emits results as OTLP telemetry to the OTel Collector
//! rather than printing them locally.
//!
//! NOTE: This starter binary does not yet wire up either side of that
//! contract — there is no live Redpanda broker or OTel Collector in this
//! repo. Instead it runs the same profiling/diagnostic logic once, against
//! a small in-memory sample DAG, and logs the results via `tracing` so the
//! logic itself is exercised end-to-end. Wiring points are marked below.

use std::time::Duration;

use clap::Parser;
use tracing::info;

use dag_ipld_benchmark_worker_rust::crypto::{
    CryptographicOverheadProfiler, NoOpZkProofVerifier,
};
use dag_ipld_benchmark_worker_rust::diagnostics::{
    DagFlakinessRootCauseIsolator, LatencySpikeThresholdPredicate, NodeExecutionRecord,
    NodeOutcome,
};
use dag_ipld_benchmark_worker_rust::traversal::{
    InMemoryBlockFetcher, IpldBlockTraversalProfiler,
};

/// Configuration for a single benchmark run, sourced from CLI flags or
/// their `DAG_IPLD_BENCH_*`-prefixed environment variable equivalents.
///
/// NOTE: In production, most of these fields would instead come from the
/// `dag_dispatch.proto`-defined payload delivered over
/// `dag.node.dispatch.v1`, not from process-local config — this CLI
/// surface exists so the worker is independently runnable for local
/// development and CI smoke tests.
#[derive(Debug, Parser)]
#[command(name = "dag-ipld-benchmark-worker", about = "IPLD/DAG traversal, crypto-overhead, and flakiness diagnostics benchmark worker")]
struct WorkerConfig {
    /// Number of samples to take for each single-threaded crypto profile.
    #[arg(long, env = "DAG_IPLD_BENCH_CRYPTO_SAMPLE_COUNT", default_value_t = 200)]
    crypto_sample_count: usize,

    /// Number of OS threads to use for the concurrent-load crypto profiles.
    #[arg(long, env = "DAG_IPLD_BENCH_CONCURRENCY", default_value_t = 4)]
    worker_thread_count: usize,

    /// Number of operations each concurrent-load thread performs.
    #[arg(long, env = "DAG_IPLD_BENCH_OPS_PER_THREAD", default_value_t = 5_000)]
    operations_per_thread: u64,

    /// Latency threshold (milliseconds) used by the flakiness isolator's
    /// anomaly predicate.
    #[arg(long, env = "DAG_IPLD_BENCH_LATENCY_THRESHOLD_MS", default_value_t = 100)]
    latency_spike_threshold_millis: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = WorkerConfig::parse();
    info!(?config, "dag-ipld-benchmark-worker starting");

    // NOTE: production wiring point — instead of running once and exiting,
    // this would be a long-lived loop consuming `dag.node.dispatch.v1`
    // dispatch messages (e.g. via rdkafka's Redpanda-compatible client),
    // running the profiler(s)/isolator named in each message's payload,
    // and pushing results out via an OTLP exporter
    // (opentelemetry-otlp) to the OTel Collector DaemonSet, per
    // ARCHITECTURE.md's "Workers → telemetry lake" contract. Workers never
    // call each other directly and never call back into the control plane
    // synchronously.

    run_ipld_block_traversal_profiling();
    run_cryptographic_overhead_profiling(&config);
    run_dag_flakiness_root_cause_isolation(&config);

    info!("dag-ipld-benchmark-worker run complete");
    Ok(())
}

/// Builds a small sample DAG of IPLD-like blocks and profiles a full
/// traversal of it.
fn run_ipld_block_traversal_profiling() {
    let mut fetcher = InMemoryBlockFetcher::new();
    let leaf_1 = fetcher.put_payload(b"benchmark-leaf-1", vec![], 0x55);
    let leaf_2 = fetcher.put_payload(b"benchmark-leaf-2", vec![], 0x55);
    let leaf_3 = fetcher.put_payload(b"benchmark-leaf-3", vec![], 0x55);
    let branch_a = fetcher.put_payload(b"benchmark-branch-a", vec![leaf_1, leaf_2], 0x71);
    let branch_b = fetcher.put_payload(b"benchmark-branch-b", vec![leaf_3], 0x71);
    let root = fetcher.put_payload(b"benchmark-root", vec![branch_a, branch_b], 0x71);

    let profiler = IpldBlockTraversalProfiler::new(fetcher);
    match profiler.profile_traversal_from(&root) {
        Ok(report) => info!(
            blocks_visited = report.blocks_visited,
            end_to_end_micros = report.end_to_end_micros,
            p50_micros = report.hop_latency_stats.p50_micros,
            p99_micros = report.hop_latency_stats.p99_micros,
            min_micros = report.hop_latency_stats.min_micros,
            max_micros = report.hop_latency_stats.max_micros,
            "ipld_block_traversal_profile_complete"
        ),
        Err(err) => tracing::error!(error = %err, "ipld_block_traversal_profile_failed"),
    }
}

/// Runs single-threaded and concurrent-load cryptographic overhead
/// profiles for multihash, signature verification, and ZK-proof
/// verification.
fn run_cryptographic_overhead_profiling(config: &WorkerConfig) {
    let profiler = CryptographicOverheadProfiler::new(NoOpZkProofVerifier::default());

    let multihash_timing = profiler.profile_multihash_computation(config.crypto_sample_count, 4096);
    info!(
        operation = multihash_timing.operation_name,
        p50_micros = multihash_timing.stats.p50_micros,
        p99_micros = multihash_timing.stats.p99_micros,
        "cryptographic_overhead_profile_complete"
    );

    let signature_timing = profiler.profile_signature_verification(config.crypto_sample_count);
    info!(
        operation = signature_timing.operation_name,
        p50_micros = signature_timing.stats.p50_micros,
        p99_micros = signature_timing.stats.p99_micros,
        "cryptographic_overhead_profile_complete"
    );

    let zk_timing = profiler.profile_zk_proof_verification(config.crypto_sample_count);
    info!(
        operation = zk_timing.operation_name,
        p50_micros = zk_timing.stats.p50_micros,
        p99_micros = zk_timing.stats.p99_micros,
        "cryptographic_overhead_profile_complete"
    );

    let multihash_load = profiler.profile_multihash_under_concurrent_load(
        config.worker_thread_count,
        config.operations_per_thread,
        4096,
    );
    info!(
        operation = multihash_load.operation_name,
        worker_thread_count = multihash_load.worker_thread_count,
        aggregate_throughput_ops_per_sec = multihash_load.aggregate_throughput_ops_per_sec,
        throughput_per_thread_ops_per_sec = multihash_load.throughput_per_thread_ops_per_sec,
        "cryptographic_overhead_concurrent_load_complete"
    );

    let signature_load = profiler.profile_signature_verification_under_concurrent_load(
        config.worker_thread_count,
        config.operations_per_thread,
    );
    info!(
        operation = signature_load.operation_name,
        worker_thread_count = signature_load.worker_thread_count,
        aggregate_throughput_ops_per_sec = signature_load.aggregate_throughput_ops_per_sec,
        throughput_per_thread_ops_per_sec = signature_load.throughput_per_thread_ops_per_sec,
        "cryptographic_overhead_concurrent_load_complete"
    );
}

/// Builds a sample DAG execution history with an injected latency spike
/// partway through the parent chain, then bisects it to isolate the origin.
fn run_dag_flakiness_root_cause_isolation(config: &WorkerConfig) {
    let history = vec![
        NodeExecutionRecord {
            node_id: "fetch-input".to_string(),
            parent_node_id: None,
            started_at_unix_millis: 0,
            latency: Duration::from_millis(8),
            outcome: NodeOutcome::Success,
        },
        NodeExecutionRecord {
            node_id: "validate-schema".to_string(),
            parent_node_id: Some("fetch-input".to_string()),
            started_at_unix_millis: 8,
            latency: Duration::from_millis(12),
            outcome: NodeOutcome::Success,
        },
        NodeExecutionRecord {
            node_id: "resolve-remote-cid".to_string(),
            parent_node_id: Some("validate-schema".to_string()),
            started_at_unix_millis: 20,
            latency: Duration::from_millis(450), // <- injected latency spike origin
            outcome: NodeOutcome::Success,
        },
        NodeExecutionRecord {
            node_id: "merge-subgraph".to_string(),
            parent_node_id: Some("resolve-remote-cid".to_string()),
            started_at_unix_millis: 470,
            latency: Duration::from_millis(460),
            outcome: NodeOutcome::Success,
        },
        NodeExecutionRecord {
            node_id: "assert-final-state".to_string(),
            parent_node_id: Some("merge-subgraph".to_string()),
            started_at_unix_millis: 930,
            latency: Duration::from_millis(470),
            outcome: NodeOutcome::Success,
        },
    ];

    let isolator = DagFlakinessRootCauseIsolator::from_execution_history(history);
    let predicate = LatencySpikeThresholdPredicate {
        threshold: Duration::from_millis(config.latency_spike_threshold_millis),
    };

    match isolator.isolate_earliest_anomalous_ancestor("assert-final-state", &predicate) {
        Ok(outcome) => info!(
            isolated_node_id = ?outcome.isolated_node_id,
            probes_evaluated = outcome.probes_evaluated,
            chain_length = outcome.chain_node_ids.len(),
            "dag_flakiness_root_cause_isolation_complete"
        ),
        Err(err) => tracing::error!(error = %err, "dag_flakiness_root_cause_isolation_failed"),
    }
}
