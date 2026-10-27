//! Latency capture: the kernel-level eBPF contract and its userspace fallback.

mod ebpf_latency_probe;

pub use ebpf_latency_probe::{
    EbpfLatencyProbe, EbpfProbeError, LatencyHistogram, UserspaceApproximateLatencyProbe,
};
