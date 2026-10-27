//! IPLD block traversal profiling.
//!
//! This module models a minimal IPLD-like content-addressed block graph and
//! measures the latency of resolving/walking it hop-by-hop, which is the
//! workload this worker exists to benchmark on behalf of the orchestration
//! control plane's DAG test runs.

pub mod ipld_block_traversal_profiler;

pub use ipld_block_traversal_profiler::{
    BlockFetchError, BlockFetcher, CidLike, HopLatencySample, InMemoryBlockFetcher, IpldBlock,
    IpldBlockTraversalProfiler, LatencyStats, TraversalReport,
};
