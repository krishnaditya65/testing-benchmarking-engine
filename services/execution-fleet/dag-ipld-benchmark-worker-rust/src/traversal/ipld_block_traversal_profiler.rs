//! Walks a DAG of content-addressed IPLD-like blocks, timing each CID
//! resolution/link-traversal hop at microsecond precision and aggregating
//! per-hop and end-to-end latency statistics.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// A minimal content identifier: a hash-function tag, a codec tag (as used
/// by real multicodec-based CIDs), and the raw digest bytes.
///
/// NOTE: A production implementation should use the real `cid` crate
/// (`cid::Cid`) with proper multibase/multicodec parsing instead of this
/// local stand-in; this local type exists so the starter repo compiles
/// without requiring network access to fetch that crate's transitive tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CidLike {
    /// Multicodec-style content type tag (e.g. 0x71 for dag-cbor).
    pub codec: u64,
    /// The raw digest bytes produced by the hashing algorithm named in
    /// `multihash_code`.
    pub digest: Vec<u8>,
    /// Multihash function code (e.g. 0x12 for sha2-256), mirroring the
    /// multihash spec's varint function identifiers.
    pub multihash_code: u64,
}

impl CidLike {
    /// Derives a `CidLike` by sha2-256 hashing `payload`, tagging the result
    /// with `codec` and the sha2-256 multihash code (0x12).
    pub fn from_payload(payload: &[u8], codec: u64) -> Self {
        let digest = Sha256::digest(payload).to_vec();
        Self { codec, digest, multihash_code: 0x12 }
    }

    /// Short hex prefix suitable for log lines, mirroring how truncated CIDs
    /// are typically rendered.
    pub fn short_hex(&self) -> String {
        self.digest.iter().take(4).map(|b| format!("{b:02x}")).collect()
    }
}

/// A single IPLD-like block: an opaque payload plus outgoing links (CIDs)
/// to child blocks, matching the shape of a real IPLD dag-cbor/dag-json
/// node closely enough for traversal-timing purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpldBlock {
    pub cid: CidLike,
    pub payload: Vec<u8>,
    pub links: Vec<CidLike>,
}

/// Errors surfaced while resolving a block during traversal.
#[derive(Debug, Error)]
pub enum BlockFetchError {
    #[error("block not found for cid {0:?}")]
    NotFound(CidLike),
    #[error("transport error fetching block: {0}")]
    Transport(String),
}

/// Abstracts the actual I/O boundary for resolving a CID to its block
/// bytes/links. Production implementations talk to a blockstore or the
/// IPFS/IPLD network; this trait lets the profiler stay agnostic of that.
pub trait BlockFetcher {
    /// Resolves `cid` to its block, or an error if unavailable.
    ///
    /// NOTE: A real implementation (e.g. `IpfsHttpBlockFetcher` or
    /// `LibP2pBitswapBlockFetcher`) would issue a network request here —
    /// bitswap over libp2p, or an HTTP gateway `GET /api/v0/block/get` —
    /// and would itself be responsible for retry/backoff policy. This
    /// trait intentionally exposes a synchronous, blocking signature so the
    /// profiler can attribute wall-clock hop latency without an async
    /// runtime's scheduling jitter contaminating the measurement.
    fn fetch(&self, cid: &CidLike) -> Result<IpldBlock, BlockFetchError>;
}

/// An in-memory `BlockFetcher` used for local benchmarking and tests. Real
/// worker deployments would fetch from a remote blockstore instead — see
/// the NOTE on `BlockFetcher::fetch`.
#[derive(Debug, Default)]
pub struct InMemoryBlockFetcher {
    blocks_by_cid: HashMap<CidLike, IpldBlock>,
}

impl InMemoryBlockFetcher {
    pub fn new() -> Self {
        Self { blocks_by_cid: HashMap::new() }
    }

    /// Inserts a block, keyed by its own CID, into the in-memory store.
    pub fn insert(&mut self, block: IpldBlock) {
        self.blocks_by_cid.insert(block.cid.clone(), block);
    }

    /// Convenience constructor: builds a `codec`-tagged block from
    /// `payload` and `links`, inserts it, and returns its derived CID.
    pub fn put_payload(&mut self, payload: &[u8], links: Vec<CidLike>, codec: u64) -> CidLike {
        let cid = CidLike::from_payload(payload, codec);
        self.insert(IpldBlock { cid: cid.clone(), payload: payload.to_vec(), links });
        cid
    }
}

impl BlockFetcher for InMemoryBlockFetcher {
    fn fetch(&self, cid: &CidLike) -> Result<IpldBlock, BlockFetchError> {
        self.blocks_by_cid
            .get(cid)
            .cloned()
            .ok_or_else(|| BlockFetchError::NotFound(cid.clone()))
    }
}

/// One timed hop: resolving a single CID to its block.
#[derive(Debug, Clone, Serialize)]
pub struct HopLatencySample {
    pub cid_short: String,
    pub depth: usize,
    pub latency_micros: u64,
}

/// min/max/mean/p50/p99 computed from a real sorted-vec of latency samples
/// (no external histogram dependency needed at this sample volume).
#[derive(Debug, Clone, Serialize)]
pub struct LatencyStats {
    pub sample_count: usize,
    pub min_micros: u64,
    pub max_micros: u64,
    pub mean_micros: f64,
    pub p50_micros: u64,
    pub p99_micros: u64,
}

impl LatencyStats {
    /// Computes stats from `samples`. Sorts a local copy so callers keep
    /// original insertion order for `samples` elsewhere if they need it.
    pub fn from_micros(mut samples: Vec<u64>) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let sample_count = samples.len();
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        let mean_micros = sum as f64 / sample_count as f64;
        let percentile = |p: f64| -> u64 {
            // Nearest-rank method: index = ceil(p * n) - 1, clamped into range.
            let rank = ((p * sample_count as f64).ceil() as usize).saturating_sub(1);
            samples[rank.min(sample_count - 1)]
        };
        Some(Self {
            sample_count,
            min_micros: samples[0],
            max_micros: samples[sample_count - 1],
            mean_micros,
            p50_micros: percentile(0.50),
            p99_micros: percentile(0.99),
        })
    }
}

/// End-to-end traversal report: per-hop samples, aggregate stats, and the
/// wall-clock total for the whole walk.
#[derive(Debug, Clone, Serialize)]
pub struct TraversalReport {
    pub root_cid_short: String,
    pub blocks_visited: usize,
    pub hop_samples: Vec<HopLatencySample>,
    pub hop_latency_stats: LatencyStats,
    pub end_to_end_micros: u64,
}

/// Breadth-first walks a DAG of `IpldBlock`s starting at a root CID,
/// resolving each hop through a `BlockFetcher` and timing the resolution
/// with `std::time::Instant` at microsecond granularity.
pub struct IpldBlockTraversalProfiler<F: BlockFetcher> {
    fetcher: F,
}

impl<F: BlockFetcher> IpldBlockTraversalProfiler<F> {
    pub fn new(fetcher: F) -> Self {
        Self { fetcher }
    }

    /// Traverses the DAG reachable from `root`, visiting each distinct CID
    /// at most once (cycles/shared subgraphs are common in real IPLD DAGs).
    /// Returns a `TraversalReport` with per-hop and end-to-end latency.
    pub fn profile_traversal_from(&self, root: &CidLike) -> Result<TraversalReport, BlockFetchError> {
        let traversal_start = Instant::now();

        let mut visited = std::collections::HashSet::new();
        let mut queue: VecDeque<(CidLike, usize)> = VecDeque::new();
        queue.push_back((root.clone(), 0));
        visited.insert(root.clone());

        let mut hop_samples = Vec::new();

        while let Some((cid, depth)) = queue.pop_front() {
            let hop_start = Instant::now();
            let block = self.fetcher.fetch(&cid)?;
            let hop_latency_micros = hop_start.elapsed().as_micros() as u64;

            hop_samples.push(HopLatencySample {
                cid_short: cid.short_hex(),
                depth,
                latency_micros: hop_latency_micros,
            });

            for child_cid in &block.links {
                if visited.insert(child_cid.clone()) {
                    queue.push_back((child_cid.clone(), depth + 1));
                }
            }
        }

        let end_to_end_micros = traversal_start.elapsed().as_micros() as u64;
        let latencies: Vec<u64> = hop_samples.iter().map(|s| s.latency_micros).collect();
        let hop_latency_stats = LatencyStats::from_micros(latencies)
            .expect("traversal always visits at least the root block");

        Ok(TraversalReport {
            root_cid_short: root.short_hex(),
            blocks_visited: hop_samples.len(),
            hop_samples,
            hop_latency_stats,
            end_to_end_micros,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample_dag() -> (InMemoryBlockFetcher, CidLike) {
        let mut fetcher = InMemoryBlockFetcher::new();
        let leaf_a = fetcher.put_payload(b"leaf-a", vec![], 0x55);
        let leaf_b = fetcher.put_payload(b"leaf-b", vec![], 0x55);
        let mid = fetcher.put_payload(b"mid", vec![leaf_a, leaf_b], 0x71);
        let root = fetcher.put_payload(b"root", vec![mid], 0x71);
        (fetcher, root)
    }

    #[test]
    fn visits_every_reachable_block_exactly_once() {
        let (fetcher, root) = build_sample_dag();
        let profiler = IpldBlockTraversalProfiler::new(fetcher);
        let report = profiler.profile_traversal_from(&root).unwrap();
        assert_eq!(report.blocks_visited, 4);
        assert_eq!(report.hop_latency_stats.sample_count, 4);
    }

    #[test]
    fn missing_block_surfaces_not_found_error() {
        let fetcher = InMemoryBlockFetcher::new();
        let profiler = IpldBlockTraversalProfiler::new(fetcher);
        let dangling = CidLike::from_payload(b"never-inserted", 0x71);
        let result = profiler.profile_traversal_from(&dangling);
        assert!(matches!(result, Err(BlockFetchError::NotFound(_))));
    }
}
