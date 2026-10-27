// dag_structural_diff_engine.go implements the Go evaluation of the
// "DAG definition versioning & structural diffing" feature: given two
// PipelineDefinitionSnapshots (see internal/domain/pipeline_definition_version.go)
// of the same pipeline, report exactly which nodes and edges were added,
// removed, or modified between them.
//
// This is the case the feature exists for: a pipeline had a
// consistency-assertion ASSERTION node wired after its LOAD_TEST node;
// someone edits the pipeline in the canvas, drags a connection around, and
// the assertion node quietly gets disconnected or deleted entirely — nothing
// in a plain "current definition" view surfaces that, since there is nothing
// to compare against. DagStructuralDiffEngine turns that into an explicit,
// structured report between any two versions.
//
// NOTE (shared contract, not shared logic): pipeline-api-gateway's TypeScript
// equivalent (pipeline-definition-diff.service.ts) implements the identical
// diff semantics independently, evaluated over its own DAG snapshot shape.
// The two are kept behaviorally consistent by convention and shared *type*
// contracts (see services/shared-libraries/dag-domain-model/src/diffing/dag-structural-diff.types.ts,
// which pipeline-definition-diff.service.ts imports) rather than shared
// executable logic, matching this repo's existing precedent of duplicating
// per-language evaluation atop shared wire-level contracts (see ADR 0002).
// In production this evaluation logic itself would ideally also move into a
// single shared-libraries package invoked by both languages (e.g. via a
// small WASM-compiled core, or by making dag-workflow-scheduler the sole
// evaluator and having pipeline-api-gateway call it), but that consolidation
// is out of scope here.
package diagnostics

import (
	"fmt"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// DagStructuralChangeKind classifies one entry in a DagStructuralDiffReport.
type DagStructuralChangeKind string

const (
	DagStructuralChangeKindNodeAdded    DagStructuralChangeKind = "NODE_ADDED"
	DagStructuralChangeKindNodeRemoved  DagStructuralChangeKind = "NODE_REMOVED"
	DagStructuralChangeKindNodeModified DagStructuralChangeKind = "NODE_MODIFIED"
	DagStructuralChangeKindEdgeAdded    DagStructuralChangeKind = "EDGE_ADDED"
	DagStructuralChangeKindEdgeRemoved  DagStructuralChangeKind = "EDGE_REMOVED"
)

// NodeStructuralChange describes one added/removed/modified node between two
// snapshots.
type NodeStructuralChange struct {
	Kind   DagStructuralChangeKind `json:"kind"`
	NodeID string                  `json:"nodeId"`
	// FromNode/ToNode are populated according to Kind: FromNode only for
	// NODE_REMOVED/NODE_MODIFIED (the "before" state), ToNode only for
	// NODE_ADDED/NODE_MODIFIED (the "after" state).
	FromNode *domain.PipelineNode `json:"fromNode,omitempty"`
	ToNode   *domain.PipelineNode `json:"toNode,omitempty"`
	// ModifiedConfigFields lists which top-level Config keys changed value,
	// populated only for NODE_MODIFIED. Empty (but Kind still
	// NODE_MODIFIED) means the node's Type or ParentNodeIDs changed instead
	// of its Config.
	ModifiedConfigFields []string `json:"modifiedConfigFields,omitempty"`
	// Human-readable summary, e.g. "consistency-assertion node
	// 'assert-p99-budget' was removed" — this is the string a reviewer
	// actually reads, everything else above is what a UI would render
	// structurally.
	Summary string `json:"summary"`
}

// EdgeStructuralChange describes one added/removed edge between two
// snapshots. Edges are never reported as "modified" — an edge with the same
// endpoints but a changed Condition is treated as a removal of the old edge
// plus an addition of the new one, since the Condition is intrinsic to what
// the edge *means*, not incidental metadata on top of a stable identity.
type EdgeStructuralChange struct {
	Kind    DagStructuralChangeKind `json:"kind"`
	Edge    domain.PipelineEdge     `json:"edge"`
	Summary string                  `json:"summary"`
}

// DagStructuralDiffReport is the full output of diffing two
// PipelineDefinitionSnapshots of the same pipeline.
type DagStructuralDiffReport struct {
	PipelineID  string `json:"pipelineId"`
	FromVersion int    `json:"fromVersion"`
	ToVersion   int    `json:"toVersion"`
	// IdenticalContent is true when both snapshots hash to the same
	// PipelineDefinitionVersionIdentity — a fast-path signal that no
	// structural change occurred at all, independent of whatever changes
	// are (or aren't) enumerated below.
	IdenticalContent bool                   `json:"identicalContent"`
	NodeChanges      []NodeStructuralChange `json:"nodeChanges"`
	EdgeChanges      []EdgeStructuralChange `json:"edgeChanges"`
}

// HasChanges reports whether the report contains any node or edge change —
// convenience for callers (e.g. a CI gate) that only care about "did
// anything change" rather than the details.
func (r DagStructuralDiffReport) HasChanges() bool {
	return len(r.NodeChanges) > 0 || len(r.EdgeChanges) > 0
}

// DagStructuralDiffEngine computes DagStructuralDiffReports between two
// PipelineDefinitionSnapshots. Stateless and side-effect-free — every method
// is a pure function of its inputs, so both dag-workflow-scheduler (over
// snapshots it captured during past runs) and, independently,
// pipeline-api-gateway's pipeline-definition-diff.service.ts can offer this
// feature without a runtime dependency between the two services.
type DagStructuralDiffEngine struct{}

// NewDagStructuralDiffEngine constructs a diff engine. Takes no dependencies
// since the engine is pure.
func NewDagStructuralDiffEngine() *DagStructuralDiffEngine {
	return &DagStructuralDiffEngine{}
}

// Diff computes the structural difference between from and to, which must
// be snapshots of the same PipelineID (different versions of the same
// pipeline) — diffing across different pipelines is rejected as almost
// certainly a caller bug rather than a meaningful comparison.
func (e *DagStructuralDiffEngine) Diff(from, to domain.PipelineDefinitionSnapshot) (DagStructuralDiffReport, error) {
	if from.PipelineID != to.PipelineID {
		return DagStructuralDiffReport{}, fmt.Errorf(
			"dag_structural_diff_engine: cannot diff snapshots of different pipelines (%q version %d vs %q version %d)",
			from.PipelineID, from.Version, to.PipelineID, to.Version,
		)
	}

	fromIdentity, err := domain.ComputeVersionIdentity(from)
	if err != nil {
		return DagStructuralDiffReport{}, fmt.Errorf("dag_structural_diff_engine: hash from-snapshot: %w", err)
	}
	toIdentity, err := domain.ComputeVersionIdentity(to)
	if err != nil {
		return DagStructuralDiffReport{}, fmt.Errorf("dag_structural_diff_engine: hash to-snapshot: %w", err)
	}

	report := DagStructuralDiffReport{
		PipelineID:       from.PipelineID,
		FromVersion:      from.Version,
		ToVersion:        to.Version,
		IdenticalContent: fromIdentity.ContentHash == toIdentity.ContentHash,
	}
	if report.IdenticalContent {
		return report, nil
	}

	report.NodeChanges = diffNodes(from.Nodes, to.Nodes)
	report.EdgeChanges = diffEdges(from.Edges, to.Edges)
	return report, nil
}

func diffNodes(fromNodes, toNodes []domain.PipelineNode) []NodeStructuralChange {
	fromByID := indexNodesByID(fromNodes)
	toByID := indexNodesByID(toNodes)

	var changes []NodeStructuralChange

	for nodeID, fromNode := range fromByID {
		toNode, stillExists := toByID[nodeID]
		if !stillExists {
			fn := fromNode
			changes = append(changes, NodeStructuralChange{
				Kind:     DagStructuralChangeKindNodeRemoved,
				NodeID:   nodeID,
				FromNode: &fn,
				Summary:  fmt.Sprintf("node %q (type %s) was removed", nodeID, fromNode.Type),
			})
			continue
		}

		if modifiedFields, changed := diffNodeContent(fromNode, toNode); changed {
			fn, tn := fromNode, toNode
			changes = append(changes, NodeStructuralChange{
				Kind:                 DagStructuralChangeKindNodeModified,
				NodeID:               nodeID,
				FromNode:             &fn,
				ToNode:               &tn,
				ModifiedConfigFields: modifiedFields,
				Summary:              summarizeNodeModification(nodeID, fromNode, toNode, modifiedFields),
			})
		}
	}

	for nodeID, toNode := range toByID {
		if _, existedBefore := fromByID[nodeID]; existedBefore {
			continue
		}
		tn := toNode
		changes = append(changes, NodeStructuralChange{
			Kind:    DagStructuralChangeKindNodeAdded,
			NodeID:  nodeID,
			ToNode:  &tn,
			Summary: fmt.Sprintf("node %q (type %s) was added", nodeID, toNode.Type),
		})
	}

	return changes
}

// diffNodeContent reports whether fromNode and toNode (same NodeID) differ,
// and if their Config differs specifically, which top-level keys changed.
func diffNodeContent(fromNode, toNode domain.PipelineNode) ([]string, bool) {
	changed := false
	if fromNode.Type != toNode.Type {
		changed = true
	}
	if !stringSlicesEqualUnordered(fromNode.ParentNodeIDs, toNode.ParentNodeIDs) {
		changed = true
	}

	modifiedFields := diffConfigKeys(fromNode.Config, toNode.Config)
	if len(modifiedFields) > 0 {
		changed = true
	}

	return modifiedFields, changed
}

// diffConfigKeys returns every top-level key present in either config whose
// value differs (added, removed, or changed) between fromConfig and
// toConfig, using fmt.Sprintf("%v", ...) equality — sufficient for the
// map[string]any config blobs PipelineNode carries, and avoids a reflection-
// heavy deep-equal dependency for what is fundamentally a diagnostics/display
// feature, not a correctness-critical one.
func diffConfigKeys(fromConfig, toConfig map[string]any) []string {
	seen := make(map[string]struct{}, len(fromConfig)+len(toConfig))
	var changed []string

	for key, fromValue := range fromConfig {
		seen[key] = struct{}{}
		toValue, existsInTo := toConfig[key]
		if !existsInTo || fmt.Sprintf("%v", fromValue) != fmt.Sprintf("%v", toValue) {
			changed = append(changed, key)
		}
	}
	for key := range toConfig {
		if _, alreadySeen := seen[key]; alreadySeen {
			continue
		}
		changed = append(changed, key)
	}

	return changed
}

func summarizeNodeModification(nodeID string, fromNode, toNode domain.PipelineNode, modifiedFields []string) string {
	if fromNode.Type != toNode.Type {
		return fmt.Sprintf("node %q changed type from %s to %s", nodeID, fromNode.Type, toNode.Type)
	}
	if !stringSlicesEqualUnordered(fromNode.ParentNodeIDs, toNode.ParentNodeIDs) {
		return fmt.Sprintf("node %q's parent wiring changed from %v to %v", nodeID, fromNode.ParentNodeIDs, toNode.ParentNodeIDs)
	}
	return fmt.Sprintf("node %q's config field(s) changed: %v", nodeID, modifiedFields)
}

func diffEdges(fromEdges, toEdges []domain.PipelineEdge) []EdgeStructuralChange {
	fromByKey := indexEdgesByKey(fromEdges)
	toByKey := indexEdgesByKey(toEdges)

	var changes []EdgeStructuralChange

	for key, edge := range fromByKey {
		if _, stillExists := toByKey[key]; !stillExists {
			changes = append(changes, EdgeStructuralChange{
				Kind: DagStructuralChangeKindEdgeRemoved,
				Edge: edge,
				Summary: fmt.Sprintf(
					"edge %s -> %s (condition %s) was removed", edge.FromNodeID, edge.ToNodeID, edge.Condition,
				),
			})
		}
	}
	for key, edge := range toByKey {
		if _, existedBefore := fromByKey[key]; !existedBefore {
			changes = append(changes, EdgeStructuralChange{
				Kind: DagStructuralChangeKindEdgeAdded,
				Edge: edge,
				Summary: fmt.Sprintf(
					"edge %s -> %s (condition %s) was added", edge.FromNodeID, edge.ToNodeID, edge.Condition,
				),
			})
		}
	}

	return changes
}

func indexNodesByID(nodes []domain.PipelineNode) map[string]domain.PipelineNode {
	byID := make(map[string]domain.PipelineNode, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	return byID
}

// edgeKey identifies an edge by its endpoints and condition together, so a
// condition change on an otherwise-unchanged edge is reported as a
// remove+add pair (see EdgeStructuralChange's doc comment) rather than
// silently ignored.
type edgeKey struct {
	fromNodeID string
	toNodeID   string
	condition  domain.PipelineEdgeCondition
}

func indexEdgesByKey(edges []domain.PipelineEdge) map[edgeKey]domain.PipelineEdge {
	byKey := make(map[edgeKey]domain.PipelineEdge, len(edges))
	for _, edge := range edges {
		byKey[edgeKey{fromNodeID: edge.FromNodeID, toNodeID: edge.ToNodeID, condition: edge.Condition}] = edge
	}
	return byKey
}

func stringSlicesEqualUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
