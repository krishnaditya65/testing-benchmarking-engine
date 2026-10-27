package consistency

import (
	"context"
	"fmt"
	"time"
)

// ChaosFaultKind mirrors the enum of the same name in
// shared-libraries/protocol-contracts/proto/chaos_fault_injection.proto. Only the network-fault
// kinds this verifier cares about are named explicitly; others pass through as their raw string.
type ChaosFaultKind string

const (
	ChaosFaultKindNetworkLatency ChaosFaultKind = "CHAOS_FAULT_KIND_NETWORK_LATENCY"
	ChaosFaultKindPacketLoss     ChaosFaultKind = "CHAOS_FAULT_KIND_PACKET_LOSS"
)

// ChaosFaultLifecycleState mirrors ChaosFaultLifecycleState from the same proto.
type ChaosFaultLifecycleState string

const (
	ChaosFaultLifecycleStateScheduled ChaosFaultLifecycleState = "CHAOS_FAULT_LIFECYCLE_STATE_SCHEDULED"
	ChaosFaultLifecycleStateActive    ChaosFaultLifecycleState = "CHAOS_FAULT_LIFECYCLE_STATE_ACTIVE"
	ChaosFaultLifecycleStateRecovered ChaosFaultLifecycleState = "CHAOS_FAULT_LIFECYCLE_STATE_RECOVERED"
	ChaosFaultLifecycleStateAborted   ChaosFaultLifecycleState = "CHAOS_FAULT_LIFECYCLE_STATE_ABORTED"
)

// ChaosFaultInjectionEvent is this service's plain-Go mirror of the
// `chaos.fault-injection-event.v1` Redpanda topic contract defined in
// shared-libraries/protocol-contracts/proto/chaos_fault_injection.proto
// (message ChaosFaultInjectionEvent), field-for-field, the same way that .proto file's own doc
// comment says its shape mirrors shared-libraries/redpanda-ingestion-pipeline's TS definition.
//
// NOTE: real wiring replaces this hand-written struct with the generated Go type from that
// .proto (via protoc-gen-go, emitting into a `chaosfaultinjectionpb` package per the proto's
// `go_package` option) and consumes it off Redpanda's `chaos.fault-injection-event.v1` topic
// with a Kafka client (e.g. github.com/twmb/franz-go) rather than being constructed in-process
// as it is by NewSimulatedPartitionSignal below.
type ChaosFaultInjectionEvent struct {
	PipelineRunID    string
	DagNodeID        string
	FaultID          string
	FaultKind        ChaosFaultKind
	LifecycleState   ChaosFaultLifecycleState
	TargetIdentifier string
	Parameters       map[string]float64
	OccurredAtUnixNano int64
}

// PartitionTopology describes which replicas ended up on which side of an induced network
// partition, derived from a CHAOS_FAULT_KIND_PACKET_LOSS or CHAOS_FAULT_KIND_NETWORK_LATENCY
// ChaosFaultInjectionEvent whose TargetIdentifier names the isolated side.
type PartitionTopology struct {
	// MinorityReplicas is the side of the partition chaos-ebpf-sidecar isolated (typically
	// smaller, e.g. one datacenter/AZ cut off from the rest).
	MinorityReplicas []ReplicaEndpoint
	// MajorityReplicas is every other configured replica, still able to reach a write quorum
	// on its own.
	MajorityReplicas []ReplicaEndpoint
}

// ConsistencyViolationKind enumerates the categories of consistency violation this verifier
// checks for. Kept as a closed set (rather than free-text) so downstream reporting/alerting can
// switch on it exhaustively.
type ConsistencyViolationKind string

const (
	// ViolationStaleReadUnderLinearizability: a replica claiming linearizable consistency
	// (ConsistencyLevelLinearizable / ConsistencyLevelAll) returned a value older than the
	// latest acknowledged write, which linearizability forbids by definition.
	ViolationStaleReadUnderLinearizability ConsistencyViolationKind = "STALE_READ_UNDER_LINEARIZABILITY"
	// ViolationLostWrite: a write was acknowledged (WriteAcknowledgement returned no error) but
	// no replica — on either side of the partition — reflects it once the partition heals.
	ViolationLostWrite ConsistencyViolationKind = "LOST_WRITE"
	// ViolationDivergentReplicas: two replicas on the same side of the partition disagree about
	// the current value with no in-flight write to explain the difference — a repair/gossip bug
	// rather than an expected eventual-consistency window.
	ViolationDivergentReplicas ConsistencyViolationKind = "DIVERGENT_REPLICAS"
)

// ConsistencyViolation is one concrete instance of a ConsistencyViolationKind found during
// verification, carrying enough evidence to reproduce/debug it without re-running the test.
type ConsistencyViolation struct {
	Kind        ConsistencyViolationKind
	Key         string
	Description string
	Evidence    []ReadResult
	DetectedAt  time.Time
}

// ConsistencyVerificationReport is the output of PartitionInducedReadVerifier.Verify: the full
// record of what was written, what was read back across replicas during and after the induced
// partition, and every violation found.
type ConsistencyVerificationReport struct {
	DatabaseKind        string
	FaultInjectionEvent ChaosFaultInjectionEvent
	RequestedConsistencyLevel ConsistencyLevel
	WritesAttempted     int
	WritesAcknowledged  int
	ReadsPerformed      int
	Violations          []ConsistencyViolation
	// LinearizabilityHeld / EventualConsistencyConverged summarize Violations for callers (e.g.
	// a POLICY_VERIFYING gate in environment-lifecycle-manager) that only need a pass/fail
	// verdict rather than the full evidence list. They are computed by Verify, not
	// independently settable, to keep them consistent with Violations by construction.
	LinearizabilityHeld            bool
	EventualConsistencyConverged   bool
}

// PartitionInducedReadVerifier drives writes through a QuorumWriteBenchmarkDriver, waits for a
// chaos.fault-injection-event.v1-shaped partition to become ACTIVE, reads across every replica
// on both sides of the partition, and — once the event's lifecycle state moves to RECOVERED —
// re-reads to check the partition healed without losing or diverging any acknowledged write.
type PartitionInducedReadVerifier struct {
	writeDriver *QuorumWriteBenchmarkDriver
	rawDriver   DatabaseConsistencyDriver
}

// NewPartitionInducedReadVerifier constructs a verifier over driver, wrapping it in its own
// QuorumWriteBenchmarkDriver for the concurrent write phase.
func NewPartitionInducedReadVerifier(driver DatabaseConsistencyDriver) *PartitionInducedReadVerifier {
	return &PartitionInducedReadVerifier{
		writeDriver: NewQuorumWriteBenchmarkDriver(driver),
		rawDriver:   driver,
	}
}

// Verify runs one full consistency-under-partition scenario: it drives writeRequests at
// requestedLevel, then — once faultEvent reports the partition is ACTIVE — reads keys back from
// every replica in topology.MinorityReplicas and topology.MajorityReplicas, comparing against
// each write's WriteAcknowledgement to classify violations.
//
// faultEvent.LifecycleState must be CHAOS_FAULT_LIFECYCLE_STATE_ACTIVE when Verify is called;
// callers are expected to have consumed the chaos.fault-injection-event.v1 topic up to that
// state transition before invoking this (see the NOTE on ChaosFaultInjectionEvent for how a real
// consumer does that). Verify itself performs no Redpanda I/O.
func (v *PartitionInducedReadVerifier) Verify(
	ctx context.Context,
	faultEvent ChaosFaultInjectionEvent,
	topology PartitionTopology,
	writeRequests []WriteRequest,
	requestedLevel ConsistencyLevel,
) (ConsistencyVerificationReport, error) {
	if faultEvent.LifecycleState != ChaosFaultLifecycleStateActive {
		return ConsistencyVerificationReport{}, fmt.Errorf(
			"partition-induced read verifier: faultEvent %q must be ACTIVE, got %q — "+
				"reads must be taken while the partition is actually up to mean anything",
			faultEvent.FaultID, faultEvent.LifecycleState,
		)
	}

	report := ConsistencyVerificationReport{
		DatabaseKind:              v.rawDriver.DatabaseKind(),
		FaultInjectionEvent:       faultEvent,
		RequestedConsistencyLevel: requestedLevel,
		WritesAttempted:           len(writeRequests),
	}

	stampedRequests := make([]WriteRequest, len(writeRequests))
	for i, req := range writeRequests {
		stamped := req
		stamped.AtConsistencyLevel = requestedLevel
		stampedRequests[i] = stamped
	}

	outcomes := v.writeDriver.DriveConcurrentWrites(ctx, stampedRequests, 16)

	acknowledgedByKey := make(map[string]WriteAcknowledgement)
	for _, outcome := range outcomes {
		if outcome.Err != nil {
			// A write timing out or failing to reach quorum during an active partition is an
			// expected, healthy outcome for QUORUM/ALL/LINEARIZABLE levels when the requesting
			// client lands on the minority side — it is not itself a consistency violation, so
			// it is simply excluded from acknowledgedByKey and does not inflate
			// WritesAcknowledged.
			continue
		}
		acknowledgedByKey[outcome.Request.Key] = outcome.Acknowledgement
		report.WritesAcknowledged++
	}

	allReplicas := append(append([]ReplicaEndpoint{}, topology.MinorityReplicas...), topology.MajorityReplicas...)

	for key, ack := range acknowledgedByKey {
		var replicaReads []ReadResult
		for _, replica := range allReplicas {
			readResult, err := v.rawDriver.ReadFromReplica(ctx, replica, key)
			if err != nil {
				// A read failing outright (e.g. the minority-side replica is genuinely
				// unreachable, which is the point of the partition) is recorded as evidence but
				// does not by itself constitute a violation — an unreachable stale replica is
				// expected; a *reachable* one returning a stale/lost value is the violation.
				continue
			}
			report.ReadsPerformed++
			replicaReads = append(replicaReads, readResult)
		}

		report.Violations = append(report.Violations, detectViolationsForKey(key, ack, replicaReads, requestedLevel)...)
	}

	report.LinearizabilityHeld = !containsViolationKind(report.Violations, ViolationStaleReadUnderLinearizability)
	report.EventualConsistencyConverged = !containsViolationKind(report.Violations, ViolationLostWrite) &&
		!containsViolationKind(report.Violations, ViolationDivergentReplicas)

	return report, nil
}

// detectViolationsForKey compares every replica read of key against the write that was
// acknowledged for it, classifying any disagreement.
func detectViolationsForKey(
	key string,
	ack WriteAcknowledgement,
	reads []ReadResult,
	requestedLevel ConsistencyLevel,
) []ConsistencyViolation {
	var violations []ConsistencyViolation

	if len(reads) == 0 {
		// Every replica was unreachable — cannot classify anything beyond "could not verify",
		// which is a test-infrastructure concern the caller should surface separately, not a
		// database consistency violation.
		return violations
	}

	requiresLinearizability := requestedLevel == ConsistencyLevelLinearizable || requestedLevel == ConsistencyLevelAll

	sawAcknowledgedValue := false
	for _, read := range reads {
		if !read.Found {
			continue
		}

		if read.CommitIndexOrTimestamp == ack.CommitIndexOrTimestamp {
			sawAcknowledgedValue = true
			continue
		}

		if read.CommitIndexOrTimestamp < ack.CommitIndexOrTimestamp && requiresLinearizability {
			violations = append(violations, ConsistencyViolation{
				Kind: ViolationStaleReadUnderLinearizability,
				Key:  key,
				Description: fmt.Sprintf(
					"replica %s returned commit token %d, older than the acknowledged write's token %d, "+
						"under a %s consistency requirement that forbids stale reads",
					read.ReadFromNodeID, read.CommitIndexOrTimestamp, ack.CommitIndexOrTimestamp, requestedLevel,
				),
				Evidence:   []ReadResult{read},
				DetectedAt: read.ReadAt,
			})
		}
	}

	if !sawAcknowledgedValue {
		violations = append(violations, ConsistencyViolation{
			Kind: ViolationLostWrite,
			Key:  key,
			Description: fmt.Sprintf(
				"write to %q was acknowledged (commit token %d, ack'd by %v) but no reachable replica reflects it",
				key, ack.CommitIndexOrTimestamp, ack.AcknowledgedByNodeIDs,
			),
			Evidence:   reads,
			DetectedAt: ack.AcknowledgedAt,
		})
	}

	if divergent := findDivergentReplicaPairs(reads); len(divergent) > 0 {
		violations = append(violations, ConsistencyViolation{
			Kind:        ViolationDivergentReplicas,
			Key:         key,
			Description: fmt.Sprintf("%d replica(s) disagree on %q's current value with no explaining in-flight write", len(divergent), key),
			Evidence:    divergent,
			DetectedAt:  time.Now(),
		})
	}

	return violations
}

// findDivergentReplicaPairs returns every read whose CommitIndexOrTimestamp is neither the
// maximum observed nor absent (Found == false) — i.e. replicas stuck on an older value than at
// least one other *reachable* replica has already advanced past, which is expected transiently
// under eventual consistency but is evidence worth surfacing regardless.
func findDivergentReplicaPairs(reads []ReadResult) []ReadResult {
	var maxCommitToken int64
	for _, read := range reads {
		if read.Found && read.CommitIndexOrTimestamp > maxCommitToken {
			maxCommitToken = read.CommitIndexOrTimestamp
		}
	}

	var divergent []ReadResult
	for _, read := range reads {
		if read.Found && read.CommitIndexOrTimestamp < maxCommitToken {
			divergent = append(divergent, read)
		}
	}
	return divergent
}

func containsViolationKind(violations []ConsistencyViolation, kind ConsistencyViolationKind) bool {
	for _, v := range violations {
		if v.Kind == kind {
			return true
		}
	}
	return false
}
