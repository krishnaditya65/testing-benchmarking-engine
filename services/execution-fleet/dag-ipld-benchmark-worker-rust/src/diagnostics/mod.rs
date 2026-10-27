//! Value-add diagnostics on top of the raw profiling data: given a recorded
//! DAG execution history, isolate which upstream node's behavior first
//! correlates with an observed corruption/latency-spike symptom downstream.

pub mod dag_flakiness_root_cause_isolator;

pub use dag_flakiness_root_cause_isolator::{
    AnomalyPredicate, BisectionOutcome, CorruptedOutcomePredicate, DagFlakinessRootCauseIsolator,
    LatencySpikeThresholdPredicate, NodeExecutionRecord, NodeOutcome, RootCauseIsolationError,
};
