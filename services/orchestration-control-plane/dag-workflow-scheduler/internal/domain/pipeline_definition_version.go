package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// PipelineDefinitionSnapshot is an immutable, content-addressed capture of a
// PipelineDefinition's DAG shape at one point in time. dag-workflow-scheduler
// pins a run to the snapshot active when the run was triggered (see
// ARCHITECTURE.md's "durability by design" principle and
// shared-libraries/dag-domain-model's PipelineDefinition.version field) so
// that pipeline-api-gateway's structural-diffing feature — "who silently
// removed the consistency-assertion node between run N and run N+1" — has a
// stable, comparable artifact per version rather than only the
// mutable/current definition row.
type PipelineDefinitionSnapshot struct {
	PipelineID string         `json:"pipelineId"`
	Version    int            `json:"version"`
	Nodes      []PipelineNode `json:"nodes"`
	Edges      []PipelineEdge `json:"edges"`
	// CapturedAtUnixMilli records when this snapshot was taken (typically:
	// when pipeline-api-gateway persisted this version of the definition).
	// Not part of the content hash — two snapshots with identical DAG shape
	// captured at different times must still be recognized as
	// structurally identical.
	CapturedAtUnixMilli int64 `json:"capturedAtUnixMilli"`
}

// PipelineDefinitionVersionIdentity is the hash-based identity of a
// PipelineDefinitionSnapshot's *structural* content (nodes + edges only —
// deliberately excluding PipelineID/Version/CapturedAtUnixMilli), so two
// snapshots that describe the exact same DAG shape (e.g. a save that only
// touched metadata, or an accidental round-trip re-save) hash identically
// and dag_structural_diff_engine.go can short-circuit a diff to "no
// structural change" without walking every node/edge.
type PipelineDefinitionVersionIdentity struct {
	// ContentHash is a hex-encoded SHA-256 digest of the snapshot's
	// canonicalized (sorted, whitespace-free JSON) node/edge content.
	ContentHash string `json:"contentHash"`
}

// ComputeVersionIdentity derives snapshot's content-hash identity.
// Canonicalization (sorting nodes/edges by ID before hashing) is required
// because PipelineDefinition.Nodes/Edges ordering is not semantically
// meaningful — two snapshots with the same nodes/edges in a different slice
// order describe the same DAG and must hash identically, or
// PipelineDefinitionVersionIdentity would falsely report a change on every
// save that happened to reorder the underlying jsonb array.
func ComputeVersionIdentity(snapshot PipelineDefinitionSnapshot) (PipelineDefinitionVersionIdentity, error) {
	canonical, err := canonicalizeForHashing(snapshot)
	if err != nil {
		return PipelineDefinitionVersionIdentity{}, fmt.Errorf("pipeline_definition_version: canonicalize snapshot for pipeline %q version %d: %w", snapshot.PipelineID, snapshot.Version, err)
	}

	digest := sha256.Sum256(canonical)
	return PipelineDefinitionVersionIdentity{ContentHash: hex.EncodeToString(digest[:])}, nil
}

// canonicalDagShape is the JSON shape actually hashed: sorted nodes/edges,
// with only the fields that affect DAG structure (a config value change
// that doesn't alter node identity/type/parentage is deliberately still
// part of the hash, since a changed assertion threshold is a structural
// change worth diffing — see dag_structural_diff_engine.go's
// ModifiedConfigFields reporting).
type canonicalDagShape struct {
	Nodes []PipelineNode `json:"nodes"`
	Edges []PipelineEdge `json:"edges"`
}

func canonicalizeForHashing(snapshot PipelineDefinitionSnapshot) ([]byte, error) {
	sortedNodes := append([]PipelineNode{}, snapshot.Nodes...)
	sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i].ID < sortedNodes[j].ID })

	sortedEdges := append([]PipelineEdge{}, snapshot.Edges...)
	sort.Slice(sortedEdges, func(i, j int) bool {
		if sortedEdges[i].FromNodeID != sortedEdges[j].FromNodeID {
			return sortedEdges[i].FromNodeID < sortedEdges[j].FromNodeID
		}
		return sortedEdges[i].ToNodeID < sortedEdges[j].ToNodeID
	})

	// json.Marshal on a map[string]any-containing struct (PipelineNode.Config)
	// does not guarantee deterministic key order across encodings by default
	// prior to Go's stable map iteration guarantees; encoding/json has sorted
	// map keys alphabetically since Go 1.12, so this is safe, but called out
	// here since silently losing that guarantee would make ContentHash flap.
	return json.Marshal(canonicalDagShape{Nodes: sortedNodes, Edges: sortedEdges})
}
