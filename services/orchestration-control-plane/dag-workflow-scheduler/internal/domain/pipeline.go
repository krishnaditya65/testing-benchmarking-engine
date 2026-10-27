// Package domain holds the plain data shapes that describe a pipeline DAG
// (nodes, edges, and their execution results) shared between the Temporal
// workflow, its activities, the execution-fleet dispatcher, and the
// flakiness bisector. Keeping these types dependency-free (no Temporal or
// Kafka imports) means they stay usable from any layer without pulling in
// unrelated infrastructure code.
package domain

// PipelineNodeType identifies what kind of work a PipelineNode represents.
// Execution-fleet workers use this to decide which protocol driver/assertion
// engine to invoke once the dispatch message reaches them.
type PipelineNodeType string

const (
	// PipelineNodeTypeLoadStep dispatches a protocol load-generation step
	// (e.g. an HTTP/gRPC/WebSocket burst) to protocol-load-worker-go.
	PipelineNodeTypeLoadStep PipelineNodeType = "LOAD_STEP"
	// PipelineNodeTypeAssertion evaluates a contract/assertion check against
	// telemetry produced by an upstream node.
	PipelineNodeTypeAssertion PipelineNodeType = "ASSERTION"
	// PipelineNodeTypeSetup performs environment/fixture preparation before
	// downstream load or assertion nodes run.
	PipelineNodeTypeSetup PipelineNodeType = "SETUP"
	// PipelineNodeTypeDiagnosticCapture pulls a detailed trace/profile
	// snapshot from telemetry-query-api for a bounded window. Unlike the
	// other node types, nodes of this type are never authored up front in a
	// PipelineDefinition — they only ever exist because
	// DagMutationSignalHandler injected one mid-run in reaction to a live
	// telemetry threshold breach (see
	// internal/workflow/dag_mutation_signal_handler.go and
	// internal/workflow/live_telemetry_threshold_watcher.go).
	PipelineNodeTypeDiagnosticCapture PipelineNodeType = "DIAGNOSTIC_CAPTURE"
)

// PipelineNode is a single unit of work in a pipeline DAG. Config is
// intentionally untyped (map[string]any) because each PipelineNodeType
// defines its own schema, mirroring how the eventual
// shared-libraries/protocol-contracts/proto/dag_dispatch.proto payload will
// carry a type-specific config blob.
type PipelineNode struct {
	ID            string           `json:"id"`
	Type          PipelineNodeType `json:"type"`
	Config        map[string]any   `json:"config"`
	ParentNodeIDs []string         `json:"parentNodeIds"`
}

// PipelineEdgeCondition controls whether a child node is eligible to run
// given how its parent node resolved.
type PipelineEdgeCondition string

const (
	// PipelineEdgeConditionOnParentPassed only runs the child if the parent
	// node's own contract/assertion check passed.
	PipelineEdgeConditionOnParentPassed PipelineEdgeCondition = "ON_PARENT_PASSED"
	// PipelineEdgeConditionOnParentFailed only runs the child if the parent
	// node's own contract/assertion check failed (e.g. cleanup/alerting
	// branches).
	PipelineEdgeConditionOnParentFailed PipelineEdgeCondition = "ON_PARENT_FAILED"
	// PipelineEdgeConditionAlways runs the child regardless of the parent's
	// outcome.
	PipelineEdgeConditionAlways PipelineEdgeCondition = "ALWAYS"
)

// PipelineEdge connects two PipelineNodes and gates traversal via Condition.
type PipelineEdge struct {
	FromNodeID string                `json:"fromNodeId"`
	ToNodeID   string                `json:"toNodeId"`
	Condition  PipelineEdgeCondition `json:"condition"`
}

// PipelineDefinition is the full DAG submitted by pipeline-api-gateway for a
// single pipeline run.
type PipelineDefinition struct {
	PipelineID string         `json:"pipelineId"`
	Nodes      []PipelineNode `json:"nodes"`
	Edges      []PipelineEdge `json:"edges"`
}

// NodeExecutionStatus is the terminal state of a node after the workflow has
// decided whether/how it ran.
type NodeExecutionStatus string

const (
	// NodeExecutionStatusPassed means the node ran and its own
	// contract/assertion check succeeded.
	NodeExecutionStatusPassed NodeExecutionStatus = "PASSED"
	// NodeExecutionStatusFailed means the node ran but its own
	// contract/assertion check failed.
	NodeExecutionStatusFailed NodeExecutionStatus = "FAILED"
	// NodeExecutionStatusSkipped means the node never ran because no
	// incoming edge's condition was satisfied by its parents' outcomes.
	NodeExecutionStatusSkipped NodeExecutionStatus = "SKIPPED"
)

// NodeExecutionResult is the outcome recorded for a single PipelineNode
// after the workflow has attempted (or deliberately skipped) it.
type NodeExecutionResult struct {
	NodeID           string              `json:"nodeId"`
	Status           NodeExecutionStatus `json:"status"`
	DetailMessage    string              `json:"detailMessage,omitempty"`
	CompletedAtMilli int64               `json:"completedAtUnixMilli,omitempty"`
}

// PipelineRunResult is the final, durable output of a DagPipelineWorkflow
// execution: one NodeExecutionResult per node in the submitted DAG.
type PipelineRunResult struct {
	PipelineID  string                         `json:"pipelineId"`
	RunID       string                         `json:"runId"`
	NodeResults map[string]NodeExecutionResult `json:"nodeResults"`
}
