//! Root-cause isolation for flaky DAG test runs.
//!
//! Given a recorded execution history for a DAG run (one record per node
//! execution, each pointing at its parent node), this walks the parent
//! chain leading up to a symptomatic node and performs a binary-search
//! ("git bisect"-style) walk over that chain to find the earliest ancestor
//! whose execution is anomalous under a caller-supplied predicate — e.g.
//! the first ancestor whose latency crossed a threshold, or the first
//! ancestor that produced corrupted output that then propagated downstream.
//!
//! The bisection assumes the classic bisect invariant: once an ancestor's
//! execution is anomalous, every downstream descendant that depends on it
//! is *also* anomalous (corruption/latency-inflation propagates forward
//! through the DAG). That lets binary search over the root-to-symptom chain
//! find the earliest anomalous point in O(log n) predicate evaluations
//! instead of walking the whole chain linearly.

use std::collections::HashMap;
use std::time::Duration;

use serde::Serialize;

/// Terminal outcome recorded for a single DAG node execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum NodeOutcome {
    Success,
    Corrupted,
    Failed,
}

/// One recorded execution of a DAG node: its identity, its parent in the
/// executed DAG (None for root nodes), when it ran, how long it took, and
/// what it produced.
#[derive(Debug, Clone, Serialize)]
pub struct NodeExecutionRecord {
    pub node_id: String,
    pub parent_node_id: Option<String>,
    pub started_at_unix_millis: u64,
    pub latency: Duration,
    pub outcome: NodeOutcome,
}

/// Caller-supplied test for "is this node execution anomalous". Kept as a
/// trait (rather than a bare closure) so common predicates can be named
/// and reused across diagnostic runs.
pub trait AnomalyPredicate {
    fn is_anomalous(&self, record: &NodeExecutionRecord) -> bool;
}

/// Flags a node execution as anomalous once its latency exceeds a fixed
/// threshold — useful for isolating the origin of a latency spike.
pub struct LatencySpikeThresholdPredicate {
    pub threshold: Duration,
}

impl AnomalyPredicate for LatencySpikeThresholdPredicate {
    fn is_anomalous(&self, record: &NodeExecutionRecord) -> bool {
        record.latency >= self.threshold
    }
}

/// Flags a node execution as anomalous whenever its outcome is
/// `NodeOutcome::Corrupted` — useful for isolating the origin of data
/// corruption that propagated through downstream nodes.
pub struct CorruptedOutcomePredicate;

impl AnomalyPredicate for CorruptedOutcomePredicate {
    fn is_anomalous(&self, record: &NodeExecutionRecord) -> bool {
        record.outcome == NodeOutcome::Corrupted
    }
}

/// Result of a bisection run: which node was isolated as the earliest
/// anomalous ancestor (if any), plus the chain that was searched and how
/// many predicate evaluations ("probes") it took — the whole point of
/// bisecting instead of scanning linearly is keeping this probe count low.
#[derive(Debug, Clone, Serialize)]
pub struct BisectionOutcome {
    /// The earliest ancestor (closest to the DAG root) found anomalous.
    /// `None` if no node on the chain — including the symptomatic node
    /// itself — was anomalous under the predicate.
    pub isolated_node_id: Option<String>,
    /// The full root-to-symptom chain that was searched, in root-first
    /// order, for audit/debugging purposes.
    pub chain_node_ids: Vec<String>,
    /// Number of predicate evaluations performed; O(log(chain length)) for
    /// a well-formed monotonic anomaly, versus O(chain length) for a linear
    /// scan.
    pub probes_evaluated: usize,
}

/// Errors returned while isolating a root cause.
#[derive(Debug, thiserror::Error)]
pub enum RootCauseIsolationError {
    #[error("no execution record found for symptomatic node {0}")]
    SymptomNodeNotFound(String),
    #[error("parent chain for node {0} contains a cycle (parent {1} already visited)")]
    CyclicParentChain(String, String),
}

/// Isolates the earliest DAG-ancestor execution correlated with a
/// downstream symptom (corruption, latency spike, etc.) by bisecting the
/// recorded parent chain.
pub struct DagFlakinessRootCauseIsolator {
    records_by_node_id: HashMap<String, NodeExecutionRecord>,
}

impl DagFlakinessRootCauseIsolator {
    /// Indexes a full DAG execution history (as dispatched/recorded by the
    /// control plane) by node id for O(1) parent lookups during chain
    /// construction.
    pub fn from_execution_history(history: Vec<NodeExecutionRecord>) -> Self {
        let records_by_node_id =
            history.into_iter().map(|record| (record.node_id.clone(), record)).collect();
        Self { records_by_node_id }
    }

    /// Builds the root-to-`symptomatic_node_id` parent chain by walking
    /// `parent_node_id` links backward, then reversing into root-first
    /// order (the order the binary search below expects, since the bisect
    /// invariant is "anomalous-ness only turns on as you move toward the
    /// symptom").
    fn build_root_first_parent_chain(
        &self,
        symptomatic_node_id: &str,
    ) -> Result<Vec<&NodeExecutionRecord>, RootCauseIsolationError> {
        let mut chain_leaf_first = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut current_node_id = symptomatic_node_id.to_string();

        loop {
            let record = self.records_by_node_id.get(&current_node_id).ok_or_else(|| {
                RootCauseIsolationError::SymptomNodeNotFound(current_node_id.clone())
            })?;
            if !visited.insert(current_node_id.clone()) {
                return Err(RootCauseIsolationError::CyclicParentChain(
                    symptomatic_node_id.to_string(),
                    current_node_id,
                ));
            }
            chain_leaf_first.push(record);

            match &record.parent_node_id {
                Some(parent_id) => current_node_id = parent_id.clone(),
                None => break,
            }
        }

        chain_leaf_first.reverse(); // now root-first
        Ok(chain_leaf_first)
    }

    /// Bisects the parent chain leading to `symptomatic_node_id`, returning
    /// the earliest ancestor for which `predicate.is_anomalous(..)` is
    /// true. Runs in O(log n) predicate evaluations under the monotonic
    /// bisect invariant described in the module docs; falls back gracefully
    /// (still correct, just not necessarily minimal-probe) if the anomaly
    /// is non-monotonic, since standard binary search on a non-monotonic
    /// predicate still converges to *some* true/false boundary — callers
    /// after a non-monotonic result should widen to a linear scan if they
    /// need every anomalous node rather than just the first boundary found.
    pub fn isolate_earliest_anomalous_ancestor(
        &self,
        symptomatic_node_id: &str,
        predicate: &dyn AnomalyPredicate,
    ) -> Result<BisectionOutcome, RootCauseIsolationError> {
        let chain = self.build_root_first_parent_chain(symptomatic_node_id)?;
        let chain_node_ids: Vec<String> =
            chain.iter().map(|record| record.node_id.clone()).collect();

        let mut probes_evaluated = 0usize;
        let mut low = 0usize; // inclusive: smallest index not yet proven "good"
        let mut high = chain.len(); // exclusive: smallest index proven anomalous so far
        let mut earliest_anomalous_index: Option<usize> = None;

        while low < high {
            let mid = low + (high - low) / 2;
            probes_evaluated += 1;
            if predicate.is_anomalous(chain[mid]) {
                earliest_anomalous_index = Some(mid);
                high = mid; // search earlier half for an even earlier anomaly
            } else {
                low = mid + 1; // anomaly must be later in the chain, if present
            }
        }

        let isolated_node_id =
            earliest_anomalous_index.map(|index| chain[index].node_id.clone());

        Ok(BisectionOutcome { isolated_node_id, chain_node_ids, probes_evaluated })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, parent: Option<&str>, latency_millis: u64, outcome: NodeOutcome) -> NodeExecutionRecord {
        NodeExecutionRecord {
            node_id: id.to_string(),
            parent_node_id: parent.map(|p| p.to_string()),
            started_at_unix_millis: 0,
            latency: Duration::from_millis(latency_millis),
            outcome,
        }
    }

    /// Chain: root -> a -> b -> c -> symptom, where latency spikes starting
    /// at "b" and stays elevated through every descendant (monotonic).
    fn build_latency_spike_history() -> Vec<NodeExecutionRecord> {
        vec![
            record("root", None, 5, NodeOutcome::Success),
            record("a", Some("root"), 6, NodeOutcome::Success),
            record("b", Some("a"), 400, NodeOutcome::Success),
            record("c", Some("b"), 410, NodeOutcome::Success),
            record("symptom", Some("c"), 420, NodeOutcome::Success),
        ]
    }

    #[test]
    fn isolates_earliest_ancestor_that_crossed_latency_threshold() {
        let isolator = DagFlakinessRootCauseIsolator::from_execution_history(
            build_latency_spike_history(),
        );
        let predicate = LatencySpikeThresholdPredicate { threshold: Duration::from_millis(100) };
        let outcome = isolator
            .isolate_earliest_anomalous_ancestor("symptom", &predicate)
            .unwrap();
        assert_eq!(outcome.isolated_node_id.as_deref(), Some("b"));
        // 5-node chain bisected: at most ceil(log2(5)) + 1 probes.
        assert!(outcome.probes_evaluated <= 3);
    }

    #[test]
    fn returns_none_when_no_ancestor_is_anomalous() {
        let history = vec![
            record("root", None, 5, NodeOutcome::Success),
            record("symptom", Some("root"), 6, NodeOutcome::Success),
        ];
        let isolator = DagFlakinessRootCauseIsolator::from_execution_history(history);
        let predicate = LatencySpikeThresholdPredicate { threshold: Duration::from_millis(1000) };
        let outcome = isolator
            .isolate_earliest_anomalous_ancestor("symptom", &predicate)
            .unwrap();
        assert_eq!(outcome.isolated_node_id, None);
    }

    #[test]
    fn isolates_earliest_corrupted_ancestor() {
        let history = vec![
            record("root", None, 5, NodeOutcome::Success),
            record("a", Some("root"), 5, NodeOutcome::Corrupted),
            record("b", Some("a"), 5, NodeOutcome::Corrupted),
            record("symptom", Some("b"), 5, NodeOutcome::Corrupted),
        ];
        let isolator = DagFlakinessRootCauseIsolator::from_execution_history(history);
        let outcome = isolator
            .isolate_earliest_anomalous_ancestor("symptom", &CorruptedOutcomePredicate)
            .unwrap();
        assert_eq!(outcome.isolated_node_id.as_deref(), Some("a"));
    }

    #[test]
    fn missing_symptom_node_is_reported_as_an_error() {
        let isolator = DagFlakinessRootCauseIsolator::from_execution_history(vec![]);
        let predicate = CorruptedOutcomePredicate;
        let result = isolator.isolate_earliest_anomalous_ancestor("does-not-exist", &predicate);
        assert!(matches!(result, Err(RootCauseIsolationError::SymptomNodeNotFound(_))));
    }
}
