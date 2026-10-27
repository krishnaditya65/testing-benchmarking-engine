// Package workflow contains the Temporal workflow definition (and its
// activities, in activities.go) that durably orchestrates one pipeline run's
// DAG of nodes. The workflow itself never talks to the message bus or does
// I/O directly -- Temporal requires workflow code to be deterministic, so
// all side effects (dispatching a node to the Execution Fleet) happen inside
// activities executed via workflow.ExecuteActivity.
//
// The node/edge/result data shapes (PipelineNode, PipelineEdge,
// PipelineDefinition, ...) live in internal/domain rather than in this
// package, since they are also depended on by internal/dispatch and
// internal/diagnostics; this file re-exports short aliases for readability
// at call sites.
package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// Aliases so this file can read as "the DAG pipeline workflow" without a
// domain. prefix on every type, while the canonical definitions stay in
// internal/domain for reuse across packages.
type (
	PipelineNode          = domain.PipelineNode
	PipelineNodeType      = domain.PipelineNodeType
	PipelineEdge          = domain.PipelineEdge
	PipelineEdgeCondition = domain.PipelineEdgeCondition
	PipelineDefinition    = domain.PipelineDefinition
	NodeExecutionResult   = domain.NodeExecutionResult
	NodeExecutionStatus   = domain.NodeExecutionStatus
	PipelineRunResult     = domain.PipelineRunResult
)

const (
	// dispatchActivityStartToCloseTimeout bounds how long a single node
	// dispatch (publish to Redpanda + eventual fleet verdict) may take
	// before Temporal marks the activity attempt failed and retries it per
	// dispatchActivityRetryPolicy.
	dispatchActivityStartToCloseTimeout = 5 * time.Minute
)

// dispatchActivityRetryPolicy governs retries of
// DispatchNodeToExecutionFleetActivity when the Execution Fleet or Redpanda
// is transiently unavailable.
var dispatchActivityRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    5,
}

// DagPipelineWorkflow is the Temporal workflow entry point for one pipeline
// run. It performs a Kahn's-algorithm topological walk of definition's DAG:
// nodes become eligible to run only once every parent that can reach them
// has resolved, and each outgoing PipelineEdge's Condition decides whether
// the child actually executes or is marked Skipped based on the parent's
// real NodeExecutionResult. This is the "mathematical DAG execution with
// conditional branching" feature described in README.md.
func DagPipelineWorkflow(ctx workflow.Context, definition PipelineDefinition) (PipelineRunResult, error) {
	logger := workflow.GetLogger(ctx)
	runID := workflow.GetInfo(ctx).WorkflowExecution.RunID

	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: dispatchActivityStartToCloseTimeout,
		RetryPolicy:         dispatchActivityRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// mutationHandler owns DagMutationSignalChannelName for the lifetime of
	// this workflow execution. See dag_mutation_signal_handler.go for the
	// full "dynamic DAG mutation mid-run" design; the call site below is
	// where the DAG walk actually reacts to signals it has received.
	mutationHandler := NewDagMutationSignalHandler(ctx)

	nodesByID := make(map[string]PipelineNode, len(definition.Nodes))
	for _, node := range definition.Nodes {
		nodesByID[node.ID] = node
	}

	// childEdgesByParent lets us fan out from a resolved parent to every
	// edge it feeds, and inDegree lets Kahn's algorithm know when a node has
	// heard back from all of its parents and is ready to be evaluated. Both
	// are mutated in place (not just read) once a mutation signal injects a
	// new node or edge mid-run — see applyPendingDiagnosticNodeInjections.
	childEdgesByParent := make(map[string][]PipelineEdge, len(definition.Edges))
	inDegree := make(map[string]int, len(definition.Nodes))
	for _, node := range definition.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range definition.Edges {
		childEdgesByParent[edge.FromNodeID] = append(childEdgesByParent[edge.FromNodeID], edge)
		inDegree[edge.ToNodeID]++
	}

	// liveEdges starts as definition.Edges but grows as diagnostic-node
	// injections add edges; nodeShouldBeSkipped (via resolveNode) must see
	// those injected edges too, or an injected node's single incoming edge
	// would be invisible to it and it would be wrongly skipped.
	liveEdges := append([]PipelineEdge{}, definition.Edges...)

	results := make(map[string]NodeExecutionResult, len(definition.Nodes))

	// prunedNodeIDs accumulates every node ID that a SKIP_REMAINING_BRANCH
	// mutation signal has pruned (the full descendant set of each signaled
	// FromNodeID, expanded against the *current* childEdgesByParent so
	// branches containing mid-run-injected nodes prune correctly too).
	prunedNodeIDs := make(map[string]struct{})

	// ready holds node IDs whose in-degree has reached zero (i.e. every
	// parent edge pointing at them has been resolved, whether the node ends
	// up actually running or being skipped). This is the classic Kahn's
	// algorithm frontier. totalNodeCount tracks how many nodes the DAG has
	// ever contained, including mid-run injections, so the
	// "did every node resolve" check below stays correct after a mutation.
	var ready []string
	for _, node := range definition.Nodes {
		if inDegree[node.ID] == 0 {
			ready = append(ready, node.ID)
		}
	}
	totalNodeCount := len(definition.Nodes)

	visitedCount := 0
	for len(ready) > 0 {
		// Signal-check point: drained here, between node executions, so a
		// mid-run mutation always takes effect at a clean DAG-walk boundary
		// rather than racing an in-flight activity dispatch. This is the
		// call site referenced in dag_mutation_signal_handler.go's package
		// doc comment.
		mutationHandler.DrainPendingSignals(ctx)

		if aborted, reason := mutationHandler.IsAborted(); aborted {
			logger.Warn("pipeline run aborted by dag mutation signal", "pipelineID", definition.PipelineID, "runID", runID, "reason", reason)
			return PipelineRunResult{
				PipelineID:  definition.PipelineID,
				RunID:       runID,
				NodeResults: results,
			}, nil
		}

		expandPrunedNodeIDs(prunedNodeIDs, mutationHandler.SkippedFromNodeIDs(), childEdgesByParent)
		totalNodeCount += applyPendingDiagnosticNodeInjections(
			ctx, mutationHandler, nodesByID, childEdgesByParent, inDegree, results, &ready, &liveEdges,
		)

		nodeID := ready[0]
		ready = ready[1:]
		node, ok := nodesByID[nodeID]
		if !ok {
			return PipelineRunResult{}, fmt.Errorf("dag_pipeline_workflow: edge references unknown node %q", nodeID)
		}

		result, err := resolveNode(ctx, node, liveEdges, results, runID, prunedNodeIDs)
		if err != nil {
			return PipelineRunResult{}, fmt.Errorf("dag_pipeline_workflow: resolve node %q: %w", nodeID, err)
		}
		results[nodeID] = result
		visitedCount++

		for _, edge := range childEdgesByParent[nodeID] {
			inDegree[edge.ToNodeID]--
			if inDegree[edge.ToNodeID] == 0 {
				ready = append(ready, edge.ToNodeID)
			}
		}
	}

	if visitedCount != totalNodeCount {
		// A cycle (or a dangling parent reference) left nodes permanently
		// blocked with a non-zero in-degree. Fail loudly instead of
		// returning a partial result silently.
		return PipelineRunResult{}, fmt.Errorf(
			"dag_pipeline_workflow: DAG for pipeline %q is not a valid DAG (resolved %d of %d nodes; check for cycles or dangling edge references)",
			definition.PipelineID, visitedCount, totalNodeCount,
		)
	}

	logger.Info("pipeline run resolved", "pipelineID", definition.PipelineID, "runID", runID, "nodeCount", totalNodeCount)

	return PipelineRunResult{
		PipelineID:  definition.PipelineID,
		RunID:       runID,
		NodeResults: results,
	}, nil
}

// expandPrunedNodeIDs walks every not-yet-expanded SKIP_REMAINING_BRANCH
// root in skippedFromNodeIDs and adds its full descendant set (via
// childEdgesByParent, so mid-run-injected nodes are included) into
// prunedNodeIDs. Idempotent: re-running it against roots already expanded
// is a no-op, so it's safe to call on every loop iteration rather than only
// once per newly received signal.
func expandPrunedNodeIDs(prunedNodeIDs map[string]struct{}, skippedFromNodeIDs map[string]struct{}, childEdgesByParent map[string][]PipelineEdge) {
	for rootNodeID := range skippedFromNodeIDs {
		queue := []string{rootNodeID}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, edge := range childEdgesByParent[current] {
				if _, already := prunedNodeIDs[edge.ToNodeID]; already {
					continue
				}
				prunedNodeIDs[edge.ToNodeID] = struct{}{}
				queue = append(queue, edge.ToNodeID)
			}
		}
	}
}

// applyPendingDiagnosticNodeInjections splices every INJECT_DIAGNOSTIC_NODE
// mutation received since the last drain into the live DAG-walk state: it
// adds the synthesized node to nodesByID, wires a new PipelineEdge from
// AfterNodeID to it, and either enqueues it directly onto ready (if
// AfterNodeID has already resolved) or bumps its in-degree so it becomes
// ready the normal way once AfterNodeID resolves. Returns how many nodes
// were added, so the caller can keep totalNodeCount accurate.
func applyPendingDiagnosticNodeInjections(
	ctx workflow.Context,
	mutationHandler *DagMutationSignalHandler,
	nodesByID map[string]PipelineNode,
	childEdgesByParent map[string][]PipelineEdge,
	inDegree map[string]int,
	results map[string]NodeExecutionResult,
	ready *[]string,
	liveEdges *[]PipelineEdge,
) int {
	logger := workflow.GetLogger(ctx)
	injected := 0

	for _, detail := range mutationHandler.TakePendingInjections() {
		node, err := mutationHandler.BuildDiagnosticNode(detail)
		if err != nil {
			logger.Warn("dropping invalid diagnostic node injection", "error", err.Error())
			continue
		}
		if _, afterNodeKnown := nodesByID[detail.AfterNodeID]; !afterNodeKnown {
			logger.Warn("dropping diagnostic node injection referencing unknown node", "afterNodeId", detail.AfterNodeID)
			continue
		}

		nodesByID[node.ID] = node
		newEdge := PipelineEdge{
			FromNodeID: detail.AfterNodeID,
			ToNodeID:   node.ID,
			Condition:  domain.PipelineEdgeConditionAlways,
		}
		childEdgesByParent[detail.AfterNodeID] = append(childEdgesByParent[detail.AfterNodeID], newEdge)
		*liveEdges = append(*liveEdges, newEdge)
		injected++

		if _, alreadyResolved := results[detail.AfterNodeID]; alreadyResolved {
			inDegree[node.ID] = 0
			*ready = append(*ready, node.ID)
		} else {
			inDegree[node.ID] = 1
		}

		logger.Info("injected diagnostic node mid-run", "diagnosticNodeId", node.ID, "afterNodeId", detail.AfterNodeID)
	}

	return injected
}

// resolveNode decides whether node is eligible to actually execute (based on
// every already-resolved parent edge's condition, plus any live
// SKIP_REMAINING_BRANCH pruning) and either dispatches it via activity or
// marks it Skipped. A node with no parents always executes. allEdges must
// include any edges added by mid-run diagnostic-node injections (the
// caller's liveEdges slice), not just definition.Edges, or an injected
// node's single incoming edge would be invisible here and it would be
// (incorrectly) skipped for having no satisfied incoming condition.
func resolveNode(ctx workflow.Context, node PipelineNode, allEdges []PipelineEdge, results map[string]NodeExecutionResult, runID string, prunedNodeIDs map[string]struct{}) (NodeExecutionResult, error) {
	if _, pruned := prunedNodeIDs[node.ID]; pruned {
		return NodeExecutionResult{
			NodeID:           node.ID,
			Status:           domain.NodeExecutionStatusSkipped,
			DetailMessage:    "pruned by SKIP_REMAINING_BRANCH dag mutation signal",
			CompletedAtMilli: workflow.Now(ctx).UnixMilli(),
		}, nil
	}

	if shouldSkip, reason := nodeShouldBeSkipped(node, allEdges, results); shouldSkip {
		return NodeExecutionResult{
			NodeID:           node.ID,
			Status:           domain.NodeExecutionStatusSkipped,
			DetailMessage:    reason,
			CompletedAtMilli: workflow.Now(ctx).UnixMilli(),
		}, nil
	}

	// Referenced by registered activity name (rather than a function value)
	// because DispatchNodeToExecutionFleetActivity is registered as a bound
	// method on a *DagPipelineActivities instance in cmd/scheduler/main.go;
	// Temporal resolves activities by name at dispatch time.
	var activityResult NodeExecutionResult
	err := workflow.ExecuteActivity(ctx, DispatchNodeToExecutionFleetActivityName, node, runID).Get(ctx, &activityResult)
	if err != nil {
		return NodeExecutionResult{}, fmt.Errorf("dispatch node %q to execution fleet: %w", node.ID, err)
	}
	return activityResult, nil
}

// nodeShouldBeSkipped implements the conditional-branching rule: node is
// skipped unless it has no parents, or at least one incoming edge's
// Condition is satisfied by its parent's real NodeExecutionResult. This is
// the "only run child if parent's contract assertion passed" requirement --
// PipelineEdgeConditionOnParentPassed edges gate their child on the parent's
// Status being Passed, PipelineEdgeConditionOnParentFailed gates on Failed,
// and PipelineEdgeConditionAlways never gates.
func nodeShouldBeSkipped(node PipelineNode, allEdges []PipelineEdge, results map[string]NodeExecutionResult) (bool, string) {
	if len(node.ParentNodeIDs) == 0 {
		return false, ""
	}

	for _, edge := range allEdges {
		if edge.ToNodeID != node.ID {
			continue
		}
		parentResult, ok := results[edge.FromNodeID]
		if !ok {
			// Parent hasn't resolved yet; Kahn's algorithm guarantees this
			// shouldn't happen once in-degree reaches zero, but guard
			// defensively rather than panicking.
			continue
		}

		switch edge.Condition {
		case domain.PipelineEdgeConditionAlways:
			return false, ""
		case domain.PipelineEdgeConditionOnParentPassed:
			if parentResult.Status == domain.NodeExecutionStatusPassed {
				return false, ""
			}
		case domain.PipelineEdgeConditionOnParentFailed:
			if parentResult.Status == domain.NodeExecutionStatusFailed {
				return false, ""
			}
		}
	}

	return true, fmt.Sprintf("no incoming edge condition satisfied by parent results for node %q", node.ID)
}
