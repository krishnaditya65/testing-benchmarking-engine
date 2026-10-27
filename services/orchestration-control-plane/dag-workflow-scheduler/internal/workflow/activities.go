package workflow

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/dispatch"
	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// DispatchNodeToExecutionFleetActivityName is the registered Temporal
// activity name for DagPipelineActivities.DispatchNodeToExecutionFleetActivity,
// used by dag_pipeline_workflow.go to invoke it via workflow.ExecuteActivity
// without importing the concrete method value.
const DispatchNodeToExecutionFleetActivityName = "DispatchNodeToExecutionFleetActivity"

// DagPipelineActivities bundles the activity methods used by
// DagPipelineWorkflow with their dependencies, so worker.RegisterActivity
// can register bound methods instead of relying on package-level globals.
type DagPipelineActivities struct {
	fleetDispatcher dispatch.ExecutionFleetDispatcherPort
}

// NewDagPipelineActivities constructs the activity struct with its
// execution-fleet dispatcher dependency injected.
func NewDagPipelineActivities(fleetDispatcher dispatch.ExecutionFleetDispatcherPort) *DagPipelineActivities {
	return &DagPipelineActivities{fleetDispatcher: fleetDispatcher}
}

// DispatchNodeToExecutionFleetActivity publishes a single PipelineNode onto
// the dag.node.dispatch.v1 topic for the Execution Fleet to pick up, then
// returns the NodeExecutionResult that DagPipelineWorkflow should record for
// that node.
//
// NOTE: real execution-fleet response ingestion (i.e. waiting on telemetry
// or a callback signal that reports the node's actual pass/fail contract
// check) is out of scope for this starter implementation. In production
// this activity would either long-poll a result store keyed by (runID,
// nodeID) or the workflow would expose a Signal channel that an
// execution-fleet-side telemetry consumer pushes results into, and this
// activity would block (via activity.RecordHeartbeat + a wait) until that
// signal arrives or the activity's StartToCloseTimeout elapses. The struct
// shape and function signature below are written as if that real fleet
// response already exists, so wiring in the real polling/signal logic later
// requires no signature change.
func (a *DagPipelineActivities) DispatchNodeToExecutionFleetActivity(ctx context.Context, node domain.PipelineNode, runID string) (domain.NodeExecutionResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("dispatching pipeline node to execution fleet", "nodeID", node.ID, "nodeType", node.Type, "runID", runID)

	dispatchedAt := time.Now().UnixMilli() // activities run outside workflow determinism constraints, so wall-clock time is fine here.

	// Temporal's own per-activity attempt counter doubles as this dispatch's
	// idempotency "attemptGeneration" (see dispatch.DeriveDispatchIdempotencyKey):
	// a worker crash that leaves this attempt still in flight causes Temporal
	// to re-invoke this same function with the same Attempt value, which
	// RedpandaExecutionFleetDispatcher.DispatchNode recognizes as a duplicate
	// of an already-published message rather than re-running the node.
	attemptGeneration := int32(activity.GetInfo(ctx).Attempt)

	msg := dispatch.NodeDispatchMessage{
		RunID:                  runID,
		Node:                   node,
		DispatchedAtUnixMillis: dispatchedAt,
		AttemptGeneration:      attemptGeneration,
	}

	if err := a.fleetDispatcher.DispatchNode(ctx, msg); err != nil {
		return domain.NodeExecutionResult{}, fmt.Errorf("activities: dispatch node %q for run %q to execution fleet: %w", node.ID, runID, err)
	}

	// TODO(execution-fleet-integration): replace this synthesized result with
	// a real wait on execution-fleet telemetry/callback once that pipeline
	// exists. Until then we optimistically record the dispatch as a pass so
	// downstream edge-condition branching in the workflow has something real
	// to evaluate against.
	result := domain.NodeExecutionResult{
		NodeID:           node.ID,
		Status:           domain.NodeExecutionStatusPassed,
		DetailMessage:    "dispatched to execution fleet; awaiting real telemetry-based verdict (see NOTE)",
		CompletedAtMilli: dispatchedAt,
	}

	logger.Info("recorded node execution result", "nodeID", node.ID, "status", result.Status)
	return result, nil
}
