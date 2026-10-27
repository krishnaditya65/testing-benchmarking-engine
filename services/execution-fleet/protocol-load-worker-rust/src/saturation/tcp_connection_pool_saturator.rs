//! Ramps a pool of raw TCP connections up to a target concurrency and holds them open,
//! writing/reading a small probe payload on each so the socket counts as "live traffic"
//! rather than an idle handshake-only connection.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Tunables for a single saturation run against one target address.
#[derive(Debug, Clone)]
pub struct SaturatorConfig {
    /// Address the pool ramps connections up against.
    pub target: SocketAddr,
    /// Steady-state number of concurrently open connections to hold.
    pub target_concurrency: usize,
    /// How many new connect attempts may be in flight at once during ramp-up. This is
    /// the knob that keeps ramp-up from itself becoming a connect() syscall storm that
    /// starves the event loop.
    pub ramp_up_batch_size: usize,
    /// Per-connect() timeout; a target that never accepts should count as a failure,
    /// not hang a task forever.
    pub connect_timeout: Duration,
    /// Per read/write timeout on the probe payload exchanged after connect.
    pub io_timeout: Duration,
    /// Bytes written to the peer once a connection is established, and the number of
    /// bytes we then attempt to read back before considering the connection "live".
    pub probe_payload: Vec<u8>,
    /// How long each connection is held open (simulating sustained load) before being
    /// closed and, if the pool is still below `target_concurrency`, replaced.
    pub hold_duration: Duration,
}

impl Default for SaturatorConfig {
    fn default() -> Self {
        Self {
            target: "127.0.0.1:9000".parse().expect("valid default addr"),
            target_concurrency: 1_000,
            ramp_up_batch_size: 100,
            connect_timeout: Duration::from_secs(3),
            io_timeout: Duration::from_secs(3),
            probe_payload: b"PING\n".to_vec(),
            hold_duration: Duration::from_secs(30),
        }
    }
}

/// Live, atomically-updated counters describing the saturation run's progress. Cheap to
/// clone (it's an `Arc` of atomics) so it can be handed to a telemetry-export task while
/// the saturator itself keeps mutating it.
#[derive(Debug, Default)]
pub struct SaturationCounters {
    pub opened: AtomicU64,
    pub failed: AtomicU64,
    pub closed: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
}

/// Point-in-time snapshot of [`SaturationCounters`] suitable for logging or an OTLP
/// metric export, since raw atomics aren't `Serialize`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SaturationReport {
    pub opened: u64,
    pub failed: u64,
    pub closed: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

impl SaturationCounters {
    pub fn snapshot(&self) -> SaturationReport {
        SaturationReport {
            opened: self.opened.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        }
    }
}

/// Opens and holds a pool of TCP connections against a single target, ramping up to
/// `target_concurrency` in batches so the worker's own connect-rate doesn't itself
/// become the bottleneck being measured.
pub struct TcpConnectionPoolSaturator {
    config: SaturatorConfig,
    counters: Arc<SaturationCounters>,
    /// Bounds the number of concurrently-running connection tasks. Ramp-up acquires a
    /// permit before spawning each connection task and the task releases it when the
    /// connection closes, which is what lets us hold exactly `target_concurrency`
    /// connections open steady-state rather than overshooting.
    concurrency_limiter: Arc<Semaphore>,
}

impl TcpConnectionPoolSaturator {
    /// Builds a saturator for the given config. Does not connect anything yet - call
    /// [`Self::run`] to start ramp-up.
    pub fn new(config: SaturatorConfig) -> Self {
        let concurrency_limiter = Arc::new(Semaphore::new(config.target_concurrency));
        Self {
            config,
            counters: Arc::new(SaturationCounters::default()),
            concurrency_limiter,
        }
    }

    /// Shared handle to this saturator's live counters, for a telemetry-export task to
    /// poll on an interval without owning the saturator itself.
    pub fn counters(&self) -> Arc<SaturationCounters> {
        Arc::clone(&self.counters)
    }

    /// Ramps the pool up to `target_concurrency` connections in batches of
    /// `ramp_up_batch_size`, then blocks until `shutdown` fires, at which point no new
    /// connections are opened (existing ones drain naturally via `hold_duration` /
    /// task cancellation).
    ///
    /// Each opened connection runs as its own tokio task so a single slow/stuck peer
    /// can never block the rest of the pool.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let total = self.config.target_concurrency;
        let batch = self.config.ramp_up_batch_size.max(1);
        let mut launched = 0usize;

        'ramp_up: while launched < total {
            if *shutdown.borrow() {
                break;
            }
            let this_batch = batch.min(total - launched);
            for _ in 0..this_batch {
                let permit = match Arc::clone(&self.concurrency_limiter).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => break 'ramp_up, // pool already at target_concurrency
                };
                let config = self.config.clone();
                let counters = Arc::clone(&self.counters);
                let mut task_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    // The permit is held for the lifetime of this task and dropped
                    // (releasing a concurrency slot) when the connection closes.
                    let _permit = permit;
                    run_single_connection(config, counters, &mut task_shutdown).await;
                });
                launched += 1;
            }
            debug!(launched, total, "tcp saturation ramp-up progress");
            // Yield to the scheduler between batches instead of connect()-ing
            // thousands of sockets in one poll turn.
            tokio::task::yield_now().await;
        }

        // Ramp-up complete (or aborted by shutdown); wait for the shutdown signal
        // before returning so `run` models "hold until told to stop".
        let _ = shutdown.changed().await;
    }

    /// Convenience for one-shot reporting/logging call sites.
    pub fn report(&self) -> SaturationReport {
        self.counters.snapshot()
    }
}

/// Connects one TCP socket, exchanges the configured probe payload, holds the
/// connection open for `hold_duration` (or until shutdown), then closes it. On
/// success/failure/close, updates the shared counters.
async fn run_single_connection(
    config: SaturatorConfig,
    counters: Arc<SaturationCounters>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) {
    let connect_result = timeout(config.connect_timeout, TcpStream::connect(config.target)).await;

    let mut stream = match connect_result {
        Ok(Ok(stream)) => {
            counters.opened.fetch_add(1, Ordering::Relaxed);
            stream
        }
        Ok(Err(err)) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            warn!(target = %config.target, error = %err, "tcp connect failed");
            return;
        }
        Err(_) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            warn!(target = %config.target, "tcp connect timed out");
            return;
        }
    };

    // NOTE: a production saturator would set SO_REUSEADDR/TCP_NODELAY and, for
    // ephemeral-port-exhaustion mitigation at very high fan-out, bind an explicit
    // outbound port range via socket2 before connect(). Left as a follow-up: it needs
    // an OS-level ulimit/ip_local_port_range budget documented in SCALING.md.
    if let Err(err) = timeout(config.io_timeout, stream.write_all(&config.probe_payload)).await {
        counters.failed.fetch_add(1, Ordering::Relaxed);
        warn!(target = %config.target, error = %err, "tcp probe write timed out");
        return;
    }
    counters
        .bytes_written
        .fetch_add(config.probe_payload.len() as u64, Ordering::Relaxed);

    let mut read_buf = [0u8; 4096];
    match timeout(config.io_timeout, stream.read(&mut read_buf)).await {
        Ok(Ok(n)) => {
            counters.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok(Err(err)) => {
            debug!(target = %config.target, error = %err, "tcp probe read failed after connect");
        }
        Err(_) => {
            debug!(target = %config.target, "tcp probe read timed out after connect");
        }
    }

    // Hold the connection open (simulating sustained saturating load) until either the
    // hold duration elapses or a shutdown is requested, whichever comes first.
    tokio::select! {
        _ = tokio::time::sleep(config.hold_duration) => {}
        _ = shutdown.changed() => {}
    }

    drop(stream);
    counters.closed.fetch_add(1, Ordering::Relaxed);
}
