//! Cross-host clock synchronization used to make latency math trustworthy.

mod clock_drift_compensator;

pub use clock_drift_compensator::{ClockDriftCompensator, ClockSample};
