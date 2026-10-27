// Package diagnostics implements post-hoc root-cause analysis over pipeline
// DAG runs, starting with flakiness bisection: given a node that is known to
// fail intermittently, binary-search the ancestor subgraph that fed it to
// find which upstream node's nondeterministic behavior correlates with the
// failure.
package diagnostics

import (
	"context"
	"fmt"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// PartialDagReexecutor re-runs a specified subset of a PipelineDefinition's
// nodes (and reports each one's outcome) without re-running the whole DAG.
// DagFlakinessBisector depends on this port rather than a concrete executor
// so it can be unit tested against a fake and, in production, backed by
// whatever mechanism replays a bounded node subset through the Execution
// Fleet (e.g. a dedicated Temporal child workflow).
type PartialDagReexecutor interface {
	// ReexecuteSubset runs only the nodes named in nodeIDs (as a subset of
	// definition) and returns each one's fresh NodeExecutionResult, keyed by
	// node ID.
	ReexecuteSubset(ctx context.Context, definition domain.PipelineDefinition, nodeIDs []string) (map[string]domain.NodeExecutionResult, error)
}

// FlakinessRootCauseReport is the outcome of a BisectFlakiness run: the
// ancestor node most strongly correlated with the target node's
// intermittent failures, how confident the bisection is in that verdict,
// and how many partial re-executions it took to get there.
type FlakinessRootCauseReport struct {
	FlakyNodeID              string  `json:"flakyNodeId"`
	SuspectedRootCauseNodeID string  `json:"suspectedRootCauseNodeId"`
	ConfidencePercent        float64 `json:"confidencePercent"`
	RerunsPerformed          int     `json:"rerunsPerformed"`
}

// DagFlakinessBisector isolates the likely root cause of a flaky node by
// repeatedly re-executing shrinking halves of its ancestor subgraph and
// comparing pass/fail patterns against the flaky node's own outcome.
type DagFlakinessBisector struct {
	reexecutor PartialDagReexecutor
}

// NewDagFlakinessBisector constructs a bisector with its partial
// re-execution dependency injected.
func NewDagFlakinessBisector(reexecutor PartialDagReexecutor) *DagFlakinessBisector {
	return &DagFlakinessBisector{reexecutor: reexecutor}
}

// BisectFlakiness runs a binary-search-style bisection over the ancestor
// subgraph of knownFlakyNodeID to isolate which single ancestor's behavior
// correlates with the flaky node failing.
//
// Algorithm: starting from the full topologically-ordered ancestor set,
// repeatedly split the current candidate set in half, re-execute the
// flaky node together with each half, and keep whichever half's rerun
// reproduces the flaky node's failure (if only one half does, that's a
// strong signal the culprit lives in that half; if both or neither
// reproduce, the candidate set is narrowed heuristically toward the half
// closer to the flaky node in the DAG so progress is always made). This
// continues until a single ancestor node remains, at which point it is
// reported as the suspected root cause. Confidence reflects how cleanly the
// last split isolated failure to one side.
func (b *DagFlakinessBisector) BisectFlakiness(ctx context.Context, definition domain.PipelineDefinition, knownFlakyNodeID string) (*FlakinessRootCauseReport, error) {
	if _, ok := findNode(definition, knownFlakyNodeID); !ok {
		return nil, fmt.Errorf("dag_flakiness_bisector: unknown flaky node %q in pipeline %q", knownFlakyNodeID, definition.PipelineID)
	}

	candidates := ancestorsOf(definition, knownFlakyNodeID)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("dag_flakiness_bisector: node %q has no ancestors to bisect against", knownFlakyNodeID)
	}

	rerunsPerformed := 0
	lastSplitWasClean := false

	for len(candidates) > 1 {
		mid := len(candidates) / 2
		firstHalf := candidates[:mid]
		secondHalf := candidates[mid:]

		firstHalfReproduced, err := b.rerunReproducesFailure(ctx, definition, knownFlakyNodeID, firstHalf)
		if err != nil {
			return nil, fmt.Errorf("dag_flakiness_bisector: rerun first half: %w", err)
		}
		rerunsPerformed++

		secondHalfReproduced, err := b.rerunReproducesFailure(ctx, definition, knownFlakyNodeID, secondHalf)
		if err != nil {
			return nil, fmt.Errorf("dag_flakiness_bisector: rerun second half: %w", err)
		}
		rerunsPerformed++

		switch {
		case firstHalfReproduced && !secondHalfReproduced:
			candidates = firstHalf
			lastSplitWasClean = true
		case secondHalfReproduced && !firstHalfReproduced:
			candidates = secondHalf
			lastSplitWasClean = true
		default:
			// Both halves reproduced the failure, or neither did: the
			// signal is ambiguous (e.g. the flakiness depends on a
			// combination of ancestors, or on something outside this
			// subgraph entirely). Narrow toward the half nearer the flaky
			// node in submission order so the search still terminates and
			// makes forward progress, but record that this split wasn't
			// clean so ConfidencePercent reflects the uncertainty.
			candidates = secondHalf
			lastSplitWasClean = false
		}
	}

	confidence := 55.0
	if lastSplitWasClean {
		confidence = 90.0
	}

	return &FlakinessRootCauseReport{
		FlakyNodeID:              knownFlakyNodeID,
		SuspectedRootCauseNodeID: candidates[0],
		ConfidencePercent:        confidence,
		RerunsPerformed:          rerunsPerformed,
	}, nil
}

// rerunReproducesFailure re-executes knownFlakyNodeID together with
// ancestorSubset via the injected PartialDagReexecutor and reports whether
// the flaky node failed in that rerun.
func (b *DagFlakinessBisector) rerunReproducesFailure(ctx context.Context, definition domain.PipelineDefinition, knownFlakyNodeID string, ancestorSubset []string) (bool, error) {
	nodeIDs := make([]string, 0, len(ancestorSubset)+1)
	nodeIDs = append(nodeIDs, ancestorSubset...)
	nodeIDs = append(nodeIDs, knownFlakyNodeID)

	results, err := b.reexecutor.ReexecuteSubset(ctx, definition, nodeIDs)
	if err != nil {
		return false, err
	}

	flakyResult, ok := results[knownFlakyNodeID]
	if !ok {
		return false, fmt.Errorf("reexecutor did not return a result for flaky node %q", knownFlakyNodeID)
	}
	return flakyResult.Status == domain.NodeExecutionStatusFailed, nil
}

// findNode looks up a node by ID within definition.
func findNode(definition domain.PipelineDefinition, nodeID string) (domain.PipelineNode, bool) {
	for _, node := range definition.Nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return domain.PipelineNode{}, false
}

// ancestorsOf performs a reverse breadth-first walk over definition's edges
// to collect every node ID that can reach targetNodeID, in a stable,
// deterministic order (BFS discovery order) suitable for repeatable
// bisection.
func ancestorsOf(definition domain.PipelineDefinition, targetNodeID string) []string {
	parentsByChild := make(map[string][]string, len(definition.Edges))
	for _, edge := range definition.Edges {
		parentsByChild[edge.ToNodeID] = append(parentsByChild[edge.ToNodeID], edge.FromNodeID)
	}

	visited := map[string]bool{targetNodeID: true}
	queue := append([]string{}, parentsByChild[targetNodeID]...)
	var ancestors []string

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if visited[nodeID] {
			continue
		}
		visited[nodeID] = true
		ancestors = append(ancestors, nodeID)
		queue = append(queue, parentsByChild[nodeID]...)
	}

	return ancestors
}
