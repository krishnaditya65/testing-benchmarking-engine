// Package workflow (this file): mid-run DAG mutation via Temporal signals.
//
// DagPipelineWorkflow's topological walk (dag_pipeline_workflow.go) normally
// executes exactly the nodes/edges it was started with. Some pipelines need
// to react to what is happening *during* the run itself — e.g. inject an
// extra diagnostic trace-capture node once a ramp-up phase's p99 crosses a
// threshold, or prune an entire branch that has become pointless once an
// earlier assertion has already failed catastrophically, or abort the whole
// run outright. Temporal models exactly this "durable workflow reacts to an
// external event without polling" need via Signals: an external actor (here,
// LiveTelemetryThresholdWatcher, or an operator via the pipeline-api-gateway
// control surface) calls client.SignalWorkflow with a DagMutationSignal
// payload, and the running DagPipelineWorkflow drains it between node
// executions via DagMutationSignalHandler below.
package workflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// DagMutationSignalChannelName is the Temporal signal channel name that
// DagPipelineWorkflow listens on for DagMutationSignal payloads. External
// callers (LiveTelemetryThresholdWatcher, or an operator-triggered mutation
// via pipeline-api-gateway) target this exact channel name with
// client.SignalWorkflow.
const DagMutationSignalChannelName = "dag-mutation-signal-v1"

// DagMutationType enumerates the mid-run structural changes a
// DagMutationSignal can request. Kept as a closed, explicit set (rather than
// a free-form command string) so DagPipelineWorkflow's switch over it stays
// exhaustive and a new mutation kind is a compile-time-visible change.
type DagMutationType string

const (
	// DagMutationTypeInjectDiagnosticNode adds a new node to the still-running
	// DAG, wired in immediately after the node named in
	// InjectDiagnosticNodeDetail.AfterNodeID, typically a trace-capture step
	// that pulls a detailed OTel trace/profile snapshot from telemetry-query-api
	// for the window in which the anomaly was observed.
	DagMutationTypeInjectDiagnosticNode DagMutationType = "INJECT_DIAGNOSTIC_NODE"
	// DagMutationTypeSkipRemainingBranch prunes every not-yet-resolved
	// descendant of SkipRemainingBranchDetail.FromNodeID, marking them
	// NodeExecutionStatusSkipped instead of dispatching them. Used when live
	// telemetry has already confirmed a branch's outcome is moot (e.g. a
	// load-ramp branch whose target service has already tipped over, so
	// continuing to escalate load on it only wastes execution-fleet capacity).
	DagMutationTypeSkipRemainingBranch DagMutationType = "SKIP_REMAINING_BRANCH"
	// DagMutationTypeAbortRun halts the entire pipeline run at the next
	// signal-check point, regardless of which nodes remain ready. Reserved for
	// telemetry breaches severe enough (e.g. a p99 threshold breached by an
	// order of magnitude) that finishing the DAG provides no value and only
	// risks further damage to the target environment.
	DagMutationTypeAbortRun DagMutationType = "ABORT_RUN"
)

// InjectDiagnosticNodeDetail carries the parameters for
// DagMutationTypeInjectDiagnosticNode.
type InjectDiagnosticNodeDetail struct {
	// AfterNodeID is the existing node the new diagnostic node is wired as a
	// child of; the diagnostic node becomes ready as soon as AfterNodeID
	// resolves, same as any other child.
	AfterNodeID string `json:"afterNodeId"`
	// DiagnosticNodeID must be unique within the run's (possibly already
	// mutated) node set; the workflow rejects the mutation otherwise rather
	// than silently overwriting an existing node.
	DiagnosticNodeID string `json:"diagnosticNodeId"`
	// TraceCaptureTargetSelector identifies what the injected node should
	// capture a trace/profile snapshot of (e.g. a k8s label selector or
	// service name), passed straight through into the synthesized node's
	// Config so DispatchNodeToExecutionFleetActivity treats it like any other
	// dispatched node.
	TraceCaptureTargetSelector string `json:"traceCaptureTargetSelector"`
}

// SkipRemainingBranchDetail carries the parameters for
// DagMutationTypeSkipRemainingBranch.
type SkipRemainingBranchDetail struct {
	// FromNodeID is the node whose not-yet-resolved descendants (inclusive of
	// nodes reachable from FromNodeID that have not yet been dispatched) are
	// pruned.
	FromNodeID string `json:"fromNodeId"`
}

// DagMutationSignal is the payload delivered over DagMutationSignalChannelName.
// Exactly one of the *Detail fields is populated, selected by MutationType —
// modeled as separate optional fields rather than an interface{} payload so
// the struct stays a plain, Temporal-serializable data shape.
type DagMutationSignal struct {
	MutationType DagMutationType `json:"mutationType"`
	// Reason is a free-text explanation surfaced in NodeExecutionResult /
	// workflow logs so operators reviewing a run's history understand why a
	// mid-run mutation happened (e.g. "p99 latency 842ms breached 400ms
	// threshold during RAMP_UP phase").
	Reason string `json:"reason"`
	// IssuedAtUnixMilli is set by the signal's sender (LiveTelemetryThresholdWatcher
	// or an operator-facing mutation endpoint), not by the workflow, since
	// workflow code must stay deterministic and cannot call time.Now() itself.
	IssuedAtUnixMilli int64 `json:"issuedAtUnixMilli"`

	InjectDiagnosticNode *InjectDiagnosticNodeDetail `json:"injectDiagnosticNode,omitempty"`
	SkipRemainingBranch  *SkipRemainingBranchDetail  `json:"skipRemainingBranch,omitempty"`
}

// DagMutationSignalHandler owns the signal channel and the accumulated
// mid-run mutation state for one DagPipelineWorkflow execution. It is
// intentionally *not* itself responsible for touching nodesByID/inDegree/
// ready (dag_pipeline_workflow.go owns that DAG-walk state); it only decides,
// given the signals received so far, what DagPipelineWorkflow's call site
// should do next. Keeping mutation *decisions* here and DAG-walk *mechanics*
// in dag_pipeline_workflow.go keeps each file's responsibility narrow.
type DagMutationSignalHandler struct {
	signalChannel workflow.ReceiveChannel

	aborted            bool
	abortReason        string
	skippedFromNodeIDs map[string]struct{}
	pendingInjections  []InjectDiagnosticNodeDetail

	appliedInjectionNodeIDs map[string]struct{}
}

// NewDagMutationSignalHandler opens the DagMutationSignalChannelName channel
// on ctx. Must be called once per workflow execution, before the DAG-walk
// loop begins, exactly like workflow.WithActivityOptions is set up once at
// the top of DagPipelineWorkflow.
func NewDagMutationSignalHandler(ctx workflow.Context) *DagMutationSignalHandler {
	return &DagMutationSignalHandler{
		signalChannel:           workflow.GetSignalChannel(ctx, DagMutationSignalChannelName),
		skippedFromNodeIDs:      make(map[string]struct{}),
		appliedInjectionNodeIDs: make(map[string]struct{}),
	}
}

// DrainPendingSignals non-blockingly consumes every DagMutationSignal queued
// on the channel since the last drain and folds it into the handler's
// accumulated state. Called by DagPipelineWorkflow between resolving each
// node in its Kahn's-algorithm frontier (see the call site in
// dag_pipeline_workflow.go's main loop) so mutations take effect at the
// earliest safe point — never mid-activity-dispatch, only at a node
// boundary, so a mutation can never race a node that already started.
func (h *DagMutationSignalHandler) DrainPendingSignals(ctx workflow.Context) {
	logger := workflow.GetLogger(ctx)

	for {
		var signal DagMutationSignal
		received := h.signalChannel.ReceiveAsync(&signal)
		if !received {
			return
		}

		logger.Info("dag mutation signal received",
			"mutationType", signal.MutationType, "reason", signal.Reason)

		switch signal.MutationType {
		case DagMutationTypeAbortRun:
			h.aborted = true
			h.abortReason = signal.Reason

		case DagMutationTypeSkipRemainingBranch:
			if signal.SkipRemainingBranch == nil {
				logger.Warn("SKIP_REMAINING_BRANCH signal missing SkipRemainingBranch detail, ignoring")
				continue
			}
			h.skippedFromNodeIDs[signal.SkipRemainingBranch.FromNodeID] = struct{}{}

		case DagMutationTypeInjectDiagnosticNode:
			if signal.InjectDiagnosticNode == nil {
				logger.Warn("INJECT_DIAGNOSTIC_NODE signal missing InjectDiagnosticNode detail, ignoring")
				continue
			}
			h.pendingInjections = append(h.pendingInjections, *signal.InjectDiagnosticNode)

		default:
			logger.Warn("unrecognized dag mutation type, ignoring", "mutationType", signal.MutationType)
		}
	}
}

// IsAborted reports whether an ABORT_RUN signal has been received, and the
// Reason from the signal that triggered it.
func (h *DagMutationSignalHandler) IsAborted() (bool, string) {
	return h.aborted, h.abortReason
}

// SkippedFromNodeIDs returns the set of node IDs named directly in received
// SKIP_REMAINING_BRANCH signals (the roots of pruned branches, not yet
// expanded to their full descendant set — dag_pipeline_workflow.go expands
// this against the live adjacency list, since only it knows the current
// edge set once diagnostic-node injections have run).
func (h *DagMutationSignalHandler) SkippedFromNodeIDs() map[string]struct{} {
	return h.skippedFromNodeIDs
}

// TakePendingInjections returns every diagnostic-node injection requested
// since the last call and clears the pending list, so
// dag_pipeline_workflow.go applies each injection exactly once.
func (h *DagMutationSignalHandler) TakePendingInjections() []InjectDiagnosticNodeDetail {
	pending := h.pendingInjections
	h.pendingInjections = nil
	return pending
}

// BuildDiagnosticNode synthesizes the PipelineNode for an
// InjectDiagnosticNodeDetail. Extracted as its own function so
// dag_pipeline_workflow.go's injection call site stays a few lines: validate
// uniqueness, build the node, wire the edge, extend the walk state.
func (h *DagMutationSignalHandler) BuildDiagnosticNode(detail InjectDiagnosticNodeDetail) (domain.PipelineNode, error) {
	if _, exists := h.appliedInjectionNodeIDs[detail.DiagnosticNodeID]; exists {
		return domain.PipelineNode{}, fmt.Errorf(
			"dag_mutation_signal_handler: diagnostic node id %q was already injected into this run",
			detail.DiagnosticNodeID,
		)
	}
	h.appliedInjectionNodeIDs[detail.DiagnosticNodeID] = struct{}{}

	return domain.PipelineNode{
		ID:   detail.DiagnosticNodeID,
		Type: domain.PipelineNodeTypeDiagnosticCapture,
		Config: map[string]any{
			"traceCaptureTargetSelector": detail.TraceCaptureTargetSelector,
			"injectedViaMutationSignal":  true,
			"injectedAfterNodeId":        detail.AfterNodeID,
		},
		ParentNodeIDs: []string{detail.AfterNodeID},
	}, nil
}
