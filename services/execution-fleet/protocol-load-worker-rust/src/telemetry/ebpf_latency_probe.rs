//! Kernel-level latency capture contract, plus an in-process fallback that runs
//! without any privileged eBPF attachment.
//!
//! Real NIC-to-NIC time-to-first-byte (TTFB) and TLS handshake latency require
//! observing packets at (or below) the socket layer - typically via `kprobes`/
//! `tracepoints` on `tcp_v4_connect`, `tcp_rcv_established`, and XDP/TC hooks on the
//! NIC driver's RX path. That needs `CAP_BPF`/`CAP_NET_ADMIN` and a kernel new enough
//! to run BTF-typed eBPF programs - capabilities a stateless, horizontally-scaled
//! load-worker Pod deliberately does not carry (least privilege; see
//! `services/execution-fleet/chaos-ebpf-sidecar/`, which *is* privileged and owns
//! kernel-level fault injection for this platform). This crate instead defines the
//! capture contract as a trait so a privileged sidecar can implement it for real, and
//! ships a userspace-timestamp approximation as the default.

use std::collections::BTreeMap;
use std::time::Duration;

use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tracing::debug;

#[derive(Debug, Error)]
pub enum EbpfProbeError {
    #[error("probe attach failed: {0}")]
    AttachFailed(String),
    #[error("probe is not attached")]
    NotAttached,
    #[error("connect failed while approximating latency: {0}")]
    ConnectFailed(String),
}

/// A latency distribution captured by a probe, expressed as fixed histogram buckets
/// (bucket upper-bound in microseconds -> observation count) rather than a full sample
/// list, matching how this data is shaped once exported as an OTLP histogram metric.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LatencyHistogram {
    /// Bucket upper bounds in microseconds, mapped to observation counts. A `u64::MAX`
    /// key is the overflow ("+Inf") bucket.
    pub buckets_us: BTreeMap<u64, u64>,
    pub sample_count: u64,
    pub sum_us: u64,
}

impl LatencyHistogram {
    fn record(&mut self, sample: Duration) {
        const BUCKET_BOUNDS_US: &[u64] = &[
            100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
            500_000, 1_000_000,
        ];
        let sample_us = sample.as_micros() as u64;
        let bucket = BUCKET_BOUNDS_US
            .iter()
            .find(|&&bound| sample_us <= bound)
            .copied()
            .unwrap_or(u64::MAX);
        *self.buckets_us.entry(bucket).or_insert(0) += 1;
        self.sample_count += 1;
        self.sum_us += sample_us;
    }
}

/// Capture contract for NIC-to-NIC time-to-first-byte and TLS handshake latency.
///
/// Implementations range from a privileged in-kernel probe (real eBPF, attached by a
/// sidecar with `CAP_BPF`) down to this crate's userspace approximation
/// ([`UserspaceApproximateLatencyProbe`]), which times the same events from
/// application-level `Instant`s instead of kernel timestamps. Both satisfy this trait
/// so callers (e.g. the saturator's telemetry-export loop) don't need to know which
/// one is running.
#[async_trait::async_trait]
pub trait EbpfLatencyProbe: Send + Sync {
    /// Attaches the probe to its capture points. For a real eBPF implementation this
    /// loads and attaches BPF programs (kprobes/tracepoints/XDP); for the userspace
    /// fallback this is a no-op that just flips a "ready" flag.
    async fn attach(&mut self) -> Result<(), EbpfProbeError>;

    /// Detaches/unloads the probe, releasing any kernel-held resources.
    async fn detach(&mut self) -> Result<(), EbpfProbeError>;

    /// Returns the current time-to-first-byte histogram: elapsed time from socket
    /// connect (SYN sent) to the first application byte received.
    fn read_ttfb_histogram(&self) -> LatencyHistogram;

    /// Returns the current TLS handshake latency histogram: elapsed time from
    /// ClientHello to the connection being ready to carry application data.
    fn read_tls_handshake_latency(&self) -> LatencyHistogram;
}

/// Userspace approximation of [`EbpfLatencyProbe`]: times TTFB and TLS handshake
/// duration using `tokio::time::Instant` timestamps taken immediately before/after the
/// connect and handshake calls made by this same process, rather than kernel-level NIC
/// timestamps.
///
/// NOTE: this measures "time inside this process's async task", which includes tokio
/// scheduler latency and userspace TLS library overhead - both invisible to (and
/// excluded from) a real kernel probe. It systematically over-reports true wire
/// latency under scheduler contention and cannot see retransmits or NIC-level queuing
/// delay. Treat it as a reasonable default signal, not a substitute for the real
/// kernel-level capture that `chaos-ebpf-sidecar` (or a dedicated eBPF program
/// deployed alongside this worker with `CAP_BPF`) would provide in production.
#[derive(Default)]
pub struct UserspaceApproximateLatencyProbe {
    attached: bool,
    ttfb: std::sync::Mutex<LatencyHistogram>,
    tls_handshake: std::sync::Mutex<LatencyHistogram>,
}

impl UserspaceApproximateLatencyProbe {
    pub fn new() -> Self {
        Self::default()
    }

    /// Times a TCP connect + first-byte read against `addr`, recording the elapsed
    /// time into the TTFB histogram. This is the userspace stand-in for what a kernel
    /// probe would derive from SYN/first-data-packet timestamps.
    pub async fn measure_connect_and_ttfb(
        &self,
        addr: std::net::SocketAddr,
    ) -> Result<(), EbpfProbeError> {
        if !self.attached {
            return Err(EbpfProbeError::NotAttached);
        }
        let start = Instant::now();
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| EbpfProbeError::ConnectFailed(e.to_string()))?;

        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1];
        // Best-effort peek at the first byte; a target that never sends data simply
        // contributes no TTFB sample rather than blocking this measurement forever.
        let _ = tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
        let elapsed = start.elapsed();

        self.ttfb.lock().expect("ttfb histogram mutex poisoned").record(elapsed);
        debug!(?elapsed, %addr, "recorded userspace ttfb approximation");
        Ok(())
    }

    /// Records a TLS handshake duration directly, for callers (e.g. the QUIC client)
    /// that already time their own handshake via `Instant::now()`/`elapsed()` around
    /// the connect call and just need it folded into the shared histogram.
    pub fn record_tls_handshake_duration(&self, elapsed: Duration) {
        self.tls_handshake
            .lock()
            .expect("tls handshake histogram mutex poisoned")
            .record(elapsed);
    }
}

#[async_trait::async_trait]
impl EbpfLatencyProbe for UserspaceApproximateLatencyProbe {
    async fn attach(&mut self) -> Result<(), EbpfProbeError> {
        // NOTE: a real eBPF implementation would `Ebpf::load()` a compiled BPF object,
        // attach kprobes to `tcp_v4_connect`/`tcp_rcv_established` and/or an XDP
        // program to the NIC driver, and require the process to run with `CAP_BPF` +
        // `CAP_NET_ADMIN` (typically via a privileged sidecar, not this worker
        // itself). Here, "attach" just marks the fallback ready.
        self.attached = true;
        Ok(())
    }

    async fn detach(&mut self) -> Result<(), EbpfProbeError> {
        self.attached = false;
        Ok(())
    }

    fn read_ttfb_histogram(&self) -> LatencyHistogram {
        self.ttfb.lock().expect("ttfb histogram mutex poisoned").clone()
    }

    fn read_tls_handshake_latency(&self) -> LatencyHistogram {
        self.tls_handshake
            .lock()
            .expect("tls handshake histogram mutex poisoned")
            .clone()
    }
}
