//! NTP/PTP-style clock offset estimation, so latency numbers computed from timestamps
//! taken on different hosts (worker vs. target, worker-in-region-A vs.
//! worker-in-region-B) are comparable rather than polluted by each host's local clock
//! skew.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// One round-trip exchange used to estimate offset and delay, named after the classic
/// NTP four-timestamp exchange (RFC 5905 ยง8):
/// - `t0`: client send time (this host's clock, when the request left)
/// - `t1`: server receive time (peer's clock)
/// - `t2`: server send time (peer's clock, when the reply left)
/// - `t3`: client receive time (this host's clock, when the reply arrived)
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ClockSample {
    pub t0: Duration,
    pub t1: Duration,
    pub t2: Duration,
    pub t3: Duration,
}

impl ClockSample {
    /// Classic NTP offset formula: how far ahead (positive) or behind (negative) the
    /// local clock is relative to the peer's, correcting for the round trip by
    /// assuming symmetric network delay.
    ///
    /// offset = ((t1 - t0) + (t2 - t3)) / 2
    fn offset(&self) -> SignedMicros {
        let a = signed_sub(self.t1, self.t0);
        let b = signed_sub(self.t2, self.t3);
        (a + b).scale(0.5)
    }

    /// Classic NTP round-trip delay formula: total time spent on the wire, with the
    /// peer's own processing time subtracted out.
    ///
    /// delay = (t3 - t0) - (t2 - t1)
    fn round_trip_delay(&self) -> Duration {
        let total = self.t3.saturating_sub(self.t0);
        let peer_processing = self.t2.saturating_sub(self.t1);
        total.saturating_sub(peer_processing)
    }
}

/// A signed duration in microseconds, since NTP offsets are legitimately negative
/// (local clock behind peer) and `std::time::Duration` cannot represent that.
#[derive(Debug, Clone, Copy)]
struct SignedMicros {
    micros: i128,
}

impl SignedMicros {
    fn from_micros(micros: i128) -> Self {
        Self { micros }
    }

    fn scale(self, factor: f64) -> Self {
        Self::from_micros((self.micros as f64 * factor) as i128)
    }

    fn as_micros_i128(self) -> i128 {
        self.micros
    }
}

impl std::ops::Add for SignedMicros {
    type Output = SignedMicros;
    fn add(self, rhs: SignedMicros) -> SignedMicros {
        SignedMicros::from_micros(self.micros + rhs.micros)
    }
}

fn signed_sub(a: Duration, b: Duration) -> SignedMicros {
    SignedMicros::from_micros(a.as_micros() as i128 - b.as_micros() as i128)
}

/// Maintains a running estimate of this host's clock offset from a reference
/// (typically the orchestration control plane or a dedicated time source), computed
/// NTP-style from a sliding window of [`ClockSample`]s, and exposes helpers to correct
/// locally-observed timestamps before they're used in cross-region latency math.
///
/// Uses a simple exponentially-weighted moving average over successive offset
/// estimates rather than full Marzullo/clock-selection algorithms - adequate for
/// correcting load-test latency measurements, not for disciplining a system clock the
/// way a real `ptp4l`/`chronyd` deployment would.
pub struct ClockDriftCompensator {
    /// Current best offset estimate, in signed microseconds: positive means this
    /// host's clock is ahead of the reference.
    offset_micros_estimate: i128,
    /// Best (lowest-delay) round-trip observed so far; used to weight new samples,
    /// since NTP theory says the sample with the least round-trip delay carries the
    /// least symmetric-path-delay assumption error.
    best_round_trip: Option<Duration>,
    /// Smoothing factor for the running offset estimate: 0.0 = never update past the
    /// first sample, 1.0 = always trust the newest sample outright.
    smoothing_alpha: f64,
    samples_applied: u64,
}

impl ClockDriftCompensator {
    /// Creates a compensator with no offset estimate yet (`corrected_now()` behaves as
    /// identity until the first sample is applied).
    pub fn new() -> Self {
        Self {
            offset_micros_estimate: 0,
            best_round_trip: None,
            smoothing_alpha: 0.25,
            samples_applied: 0,
        }
    }

    pub fn with_smoothing_alpha(mut self, alpha: f64) -> Self {
        self.smoothing_alpha = alpha.clamp(0.0, 1.0);
        self
    }

    /// Folds one NTP-style round-trip exchange into the running offset estimate.
    /// Samples with a worse round-trip delay than the best one seen so far are still
    /// applied (clocks drift over time, so we can't only ever trust the first sample)
    /// but through the same smoothing as any other sample; `best_round_trip` is
    /// tracked for callers that want to expose measurement quality (e.g. as an OTLP
    /// gauge) rather than to gate acceptance.
    pub fn apply_sample(&mut self, sample: ClockSample) {
        let offset = sample.offset().as_micros_i128();
        let delay = sample.round_trip_delay();

        self.offset_micros_estimate = if self.samples_applied == 0 {
            offset
        } else {
            let alpha = self.smoothing_alpha;
            ((1.0 - alpha) * self.offset_micros_estimate as f64 + alpha * offset as f64) as i128
        };

        self.best_round_trip = Some(match self.best_round_trip {
            Some(current_best) if current_best <= delay => current_best,
            _ => delay,
        });
        self.samples_applied += 1;
    }

    /// Current offset estimate: positive means the local clock is ahead of the
    /// reference clock.
    pub fn current_offset(&self) -> Duration {
        Duration::from_micros(self.offset_micros_estimate.unsigned_abs() as u64)
    }

    pub fn is_offset_negative(&self) -> bool {
        self.offset_micros_estimate < 0
    }

    pub fn samples_applied(&self) -> u64 {
        self.samples_applied
    }

    /// Returns "now" on the reference clock's timeline: the local wall-clock time,
    /// corrected by the current offset estimate. Use this (not `SystemTime::now()`
    /// directly) whenever a timestamp will be compared against one produced by a
    /// different host, e.g. correlating a worker's locally-observed TTFB against a
    /// target's server-side trace timestamp.
    pub fn corrected_now(&self) -> SystemTime {
        self.adjust_timestamp(SystemTime::now())
    }

    /// Applies the current offset estimate to an arbitrary local timestamp, shifting
    /// it onto the reference clock's timeline.
    pub fn adjust_timestamp(&self, local_timestamp: SystemTime) -> SystemTime {
        if self.offset_micros_estimate >= 0 {
            local_timestamp + Duration::from_micros(self.offset_micros_estimate as u64)
        } else {
            local_timestamp - Duration::from_micros((-self.offset_micros_estimate) as u64)
        }
    }
}

impl Default for ClockDriftCompensator {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds a [`ClockSample`] for a client-driven round trip, given a `t0` captured
/// right before send and a `t3` captured right after receive; `t1`/`t2` come from the
/// peer's reply payload (e.g. an NTP-like control message on the load-worker's
/// coordination channel). Kept as a free function since constructing a sample is a
/// call-site concern, not compensator state.
pub fn sample_from_round_trip(
    t0: SystemTime,
    peer_t1: SystemTime,
    peer_t2: SystemTime,
    t3: SystemTime,
) -> ClockSample {
    ClockSample {
        t0: t0.duration_since(UNIX_EPOCH).unwrap_or_default(),
        t1: peer_t1.duration_since(UNIX_EPOCH).unwrap_or_default(),
        t2: peer_t2.duration_since(UNIX_EPOCH).unwrap_or_default(),
        t3: t3.duration_since(UNIX_EPOCH).unwrap_or_default(),
    }
}
