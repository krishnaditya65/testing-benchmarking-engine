// Package config holds configuration surfaces for dag-workflow-scheduler
// that are broader than a single call site's env-var lookups (see
// cmd/scheduler/main.go's schedulerConfig for the ordinary case) --
// specifically, config that documents an operational posture even where the
// orchestration logic behind it isn't fully built yet. See
// docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md
// for why this exists now.
package config

import "time"

// TemporalRegionFailoverConfig captures the configuration surface for
// running dag-workflow-scheduler against a Temporal namespace configured for
// multi-region replication (Temporal's "Global Namespace" / multi-cluster
// replication feature), so a regional outage of the primary Temporal cluster
// does not strand every in-flight pipeline run.
//
// STATUS: this struct is a REAL, intentionally-scoped configuration surface
// -- it is meant to be the actual shape read from env/Helm values once
// multi-region failover is implemented. What is explicitly STUBBED and out
// of scope for this hardening pass:
//
//   - No code in this service actually watches PrimaryRegion health or
//     triggers a failover; this struct only describes the config that a
//     future FailoverCoordinator (not implemented) would consume.
//   - client.Dial in cmd/scheduler/main.go still dials a single
//     TEMPORAL_HOST_PORT; it does not yet attempt ReplicaRegions on
//     PrimaryRegion failure.
//   - No handling here for in-flight Temporal Activity tasks (like a
//     dispatched-but-not-yet-acknowledged node) whose task token was minted
//     against the failed region -- Temporal's own namespace failover
//     replays workflow history on the new active cluster, but this
//     service's own idempotent-dispatch layer (see
//     internal/dispatch/idempotent_dispatch_deduplicator.go) is exactly the
//     kind of local safety net that failover makes newly relevant: a
//     production DispatchDeduplicationStore MUST be a store that survives
//     (or is itself replicated across) the same regions, or a
//     region-failover replay could re-dispatch nodes whose dedup record
//     only existed in the failed region's local store.
type TemporalRegionFailoverConfig struct {
	// PrimaryRegion is the region whose Temporal cluster this scheduler
	// dials by default (i.e. what TEMPORAL_HOST_PORT / TEMPORAL_NAMESPACE in
	// cmd/scheduler/main.go's schedulerConfig currently point at
	// unconditionally, with no failover awareness).
	PrimaryRegion string

	// ReplicaRegions lists the regions Temporal's multi-cluster replication
	// keeps a replicated copy of workflow history in, in the order they
	// should be tried if PrimaryRegion becomes unreachable. Each entry is
	// expected to resolve (via infrastructure/terraform, out of scope here)
	// to its own Temporal frontend host:port.
	ReplicaRegions []string

	// FailoverTimeout is how long the (not-yet-implemented) failover
	// coordinator should wait for PrimaryRegion to become healthy again
	// before promoting the next entry in ReplicaRegions to active. This is
	// distinct from any single Activity's StartToCloseTimeout
	// (dispatchActivityStartToCloseTimeout in
	// internal/workflow/dag_pipeline_workflow.go) -- it governs a
	// cluster-level decision, not a single dispatch.
	FailoverTimeout time.Duration
}

// DefaultFailoverTimeout is a starting-point value for FailoverTimeout: long
// enough to not flap on a brief network blip between regions, short enough
// that a genuine regional outage doesn't leave pipeline runs stalled for an
// operator-unfriendly length of time before failover kicks in. Production
// deployments should tune this against their actual cross-region latency
// and Temporal's own replication lag characteristics.
const DefaultFailoverTimeout = 2 * time.Minute
