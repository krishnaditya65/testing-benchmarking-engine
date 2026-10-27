//! Entrypoint for `protocol-load-worker-rust`.
//!
//! Wires up the TCP connection-pool saturator, the userspace latency probe, and the
//! clock-drift compensator, then runs until the control plane's load phase ends or the
//! process receives SIGTERM/SIGINT.
//!
//! NOTE: this starter binary does not yet consume `dag.node.dispatch.v1` off Redpanda
//! (per ARCHITECTURE.md's communication contract) or emit OTLP spans/metrics to the
//! OTel Collector - both are integration points a production build must add via
//! `services/shared-libraries/observability-kit` and a Redpanda consumer client. What's
//! here is the config/lifecycle/shutdown skeleton those integrations plug into, plus
//! working saturation/timing/telemetry logic driven directly by CLI/env config for now.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use protocol_load_worker_rust::saturation::{SaturatorConfig, TcpConnectionPoolSaturator};
use protocol_load_worker_rust::telemetry::{EbpfLatencyProbe, UserspaceApproximateLatencyProbe};
use protocol_load_worker_rust::timing::ClockDriftCompensator;
use tokio::sync::watch;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// CLI/env configuration for one worker process. Every field is also settable via the
/// matching `PLW_*` environment variable (clap's `env` attribute) since this worker is
/// deployed as a container and configured by its Kubernetes manifest, not invoked
/// interactively.
#[derive(Debug, Parser)]
#[command(name = "protocol-load-worker-rust", about = "TCP/QUIC socket saturation worker")]
struct WorkerConfig {
    /// Target address this worker saturates with TCP connections.
    #[arg(long, env = "PLW_TARGET_ADDR", default_value = "127.0.0.1:9000")]
    target_addr: SocketAddr,

    /// Steady-state number of concurrently open TCP connections to hold against the
    /// target.
    #[arg(long, env = "PLW_TARGET_CONCURRENCY", default_value_t = 1_000)]
    target_concurrency: usize,

    /// Number of new connect attempts allowed in flight at once during ramp-up.
    #[arg(long, env = "PLW_RAMP_UP_BATCH_SIZE", default_value_t = 100)]
    ramp_up_batch_size: usize,

    /// How long each connection is held open before being recycled.
    #[arg(long, env = "PLW_HOLD_DURATION_SECS", default_value_t = 30)]
    hold_duration_secs: u64,

    /// Interval on which this worker logs a saturation-progress snapshot. A production
    /// build emits this as an OTLP metric export instead of/in addition to a log line.
    #[arg(long, env = "PLW_REPORT_INTERVAL_SECS", default_value_t = 10)]
    report_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .json()
        .init();

    let config = WorkerConfig::parse();
    info!(?config, "protocol-load-worker-rust starting");

    // Clock-drift compensation: without a live reference-time exchange wired to the
    // control plane yet, this starts unsynchronized (offset = 0) and is ready to have
    // samples applied once that exchange exists. Kept in the wiring here so downstream
    // telemetry code has a single place to pull `corrected_now()` from.
    let clock_compensator = ClockDriftCompensator::new();
    info!(
        offset_us = ?clock_compensator.current_offset(),
        "clock drift compensator initialized (unsynchronized until first sample)"
    );

    // Userspace latency probe: real kernel-level NIC-to-NIC capture requires a
    // privileged eBPF sidecar (see NOTE in telemetry::ebpf_latency_probe); this worker
    // runs unprivileged, so it attaches the in-process approximation.
    let mut latency_probe = UserspaceApproximateLatencyProbe::new();
    latency_probe
        .attach()
        .await
        .context("failed to attach userspace latency probe")?;

    let saturator_config = SaturatorConfig {
        target: config.target_addr,
        target_concurrency: config.target_concurrency,
        ramp_up_batch_size: config.ramp_up_batch_size,
        hold_duration: Duration::from_secs(config.hold_duration_secs),
        ..SaturatorConfig::default()
    };
    let saturator = TcpConnectionPoolSaturator::new(saturator_config);
    let counters = saturator.counters();

    // Shutdown fans out via a watch channel: SIGTERM/SIGINT flips it to `true`, and
    // both the saturator's ramp-up loop and every in-flight connection task observe
    // that flip and stop opening new work / start draining.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let report_interval = Duration::from_secs(config.report_interval_secs.max(1));
    let report_shutdown_rx = shutdown_rx.clone();
    let reporter = tokio::spawn(report_saturation_progress(
        counters,
        report_interval,
        report_shutdown_rx,
    ));

    let saturator_run = saturator.run(shutdown_rx);

    tokio::select! {
        _ = saturator_run => {
            info!("saturator run loop exited");
        }
        _ = wait_for_shutdown_signal() => {
            info!("shutdown signal received, draining connection pool");
            let _ = shutdown_tx.send(true);
        }
    }

    // Give in-flight connection tasks (and the reporter) a bounded window to observe
    // the shutdown flip and exit cleanly before the process itself exits. This worker
    // is stateless (per ARCHITECTURE.md's execution-fleet contract: no persistent
    // state, checkpointing only applies to load-phase transitions, not to this raw
    // socket work), so there is nothing to flush beyond in-flight sockets closing.
    let _ = tokio::time::timeout(Duration::from_secs(5), reporter).await;

    info!("protocol-load-worker-rust shut down cleanly");
    Ok(())
}

/// Resolves once either SIGTERM or SIGINT (Ctrl-C) is received. SIGTERM is what
/// Kubernetes sends on pod termination; SIGINT covers local/interactive runs.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Periodically logs a snapshot of the saturator's live counters until shutdown.
///
/// NOTE: production telemetry should push this snapshot as an OTLP metric via
/// `services/shared-libraries/observability-kit` rather than (or in addition to) a log
/// line, per ARCHITECTURE.md's "workers -> OTLP -> OTel Collector" contract.
async fn report_saturation_progress(
    counters: std::sync::Arc<protocol_load_worker_rust::saturation::SaturationCounters>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let report = counters.snapshot();
                info!(?report, "saturation progress");
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    let report = counters.snapshot();
                    info!(?report, "final saturation report");
                    break;
                }
            }
        }
    }
}

#[allow(dead_code)]
fn assert_config_is_reasonable(config: &WorkerConfig) -> anyhow::Result<()> {
    if config.target_concurrency == 0 {
        warn!("target_concurrency is 0; saturator will do nothing");
    }
    if config.ramp_up_batch_size == 0 {
        anyhow::bail!("ramp_up_batch_size must be > 0");
    }
    Ok(())
}
