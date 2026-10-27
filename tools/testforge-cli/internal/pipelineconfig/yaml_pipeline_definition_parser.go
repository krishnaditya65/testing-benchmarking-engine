// Package pipelineconfig parses declarative pipeline definition YAML files
// (e.g. examples/pipelines/checkout-load-test.yaml) into the same DAG node
// shape services/shared-libraries/dag-domain-model defines in TypeScript for
// the control plane, so a pipeline authored for the CLI and one authored in
// workflow-canvas-frontend are structurally identical by the time they reach
// pipeline-api-gateway.
package pipelineconfig

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NodeType enumerates the DAG node kinds testforge understands. These must
// stay in lockstep with the node type union in
// services/shared-libraries/dag-domain-model — that package is the source
// of truth; this list mirrors it for Go-side validation without requiring a
// cross-language codegen step for the CLI alone.
type NodeType string

const (
	NodeTypeAPISmokeTest         NodeType = "api-smoke-test"
	NodeTypeDatabaseSeed         NodeType = "database-seed"
	NodeTypeLoadTest             NodeType = "load-test"
	NodeTypeChaosInjection       NodeType = "chaos-injection"
	NodeTypeDatabaseAssertion    NodeType = "database-consistency-assertion"
	NodeTypeEnvironmentTeardown  NodeType = "environment-teardown"
	NodeTypeEnvironmentProvision NodeType = "environment-provision"
)

// KnownNodeTypes is the set validate.go checks each node's `type` field
// against.
var KnownNodeTypes = map[NodeType]bool{
	NodeTypeAPISmokeTest:         true,
	NodeTypeDatabaseSeed:         true,
	NodeTypeLoadTest:             true,
	NodeTypeChaosInjection:       true,
	NodeTypeDatabaseAssertion:    true,
	NodeTypeEnvironmentTeardown:  true,
	NodeTypeEnvironmentProvision: true,
}

// PipelineDefinition is the root document shape of a testforge pipeline
// YAML file, mirroring the PipelineDefinition GraphQL type in
// docs/api-specifications/pipeline-api-gateway.graphql.
type PipelineDefinition struct {
	APIVersion string                 `yaml:"apiVersion"`
	Kind       string                 `yaml:"kind"`
	Metadata   PipelineMetadata       `yaml:"metadata"`
	Spec       PipelineDefinitionSpec `yaml:"spec"`

	// sourcePath is set by Parse for error messages; not part of the YAML.
	sourcePath string
}

// PipelineMetadata identifies the pipeline and, once submitted, correlates
// every PipelineRun back to the definition that produced it.
type PipelineMetadata struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description,omitempty"`
	Owner       string            `yaml:"owner,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
}

// PipelineDefinitionSpec holds the DAG itself plus the quality gates that
// determine pass/fail for CI purposes.
type PipelineDefinitionSpec struct {
	Nodes        []DAGNode     `yaml:"nodes"`
	QualityGates []QualityGate `yaml:"qualityGates,omitempty"`
}

// DAGNode is one node in the pipeline's directed acyclic graph. dependsOn
// references other nodes' `id` fields; dag-workflow-scheduler rejects any
// definition whose dependsOn graph contains a cycle, but testforge validate
// checks for that locally too so the failure surfaces in `git commit` /
// CI-lint rather than at submission time.
type DAGNode struct {
	ID        string         `yaml:"id"`
	Type      NodeType       `yaml:"type"`
	DependsOn []string       `yaml:"dependsOn,omitempty"`
	Config    map[string]any `yaml:"config,omitempty"`
}

// QualityGate is a single pass/fail assertion evaluated against a
// PipelineRun's results by pipeline-api-gateway once the DAG finishes (or
// partially finishes). testforge-cli's
// internal/cicdintegration.QualityGateExitCodeMapper maps a failed gate's
// Severity to a CI process exit code.
type QualityGate struct {
	Name      string `yaml:"name"`
	Metric    string `yaml:"metric"`
	Threshold string `yaml:"threshold"`
	Severity  string `yaml:"severity"` // "blocking" | "warning"
}

// Parse reads and unmarshals a pipeline definition YAML file from path.
// It does not validate DAG structure — that's ValidateDAG's job — Parse
// only guarantees the document is well-formed YAML matching this shape.
func Parse(path string) (*PipelineDefinition, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pipelineconfig: reading %s: %w", path, err)
	}

	var def PipelineDefinition
	if err := yaml.Unmarshal(raw, &def); err != nil {
		return nil, fmt.Errorf("pipelineconfig: parsing %s: %w", path, err)
	}
	def.sourcePath = path

	if def.Kind != "" && def.Kind != "PipelineDefinition" {
		return nil, fmt.Errorf("pipelineconfig: %s: unsupported kind %q, expected \"PipelineDefinition\"", path, def.Kind)
	}
	if def.Metadata.Name == "" {
		return nil, fmt.Errorf("pipelineconfig: %s: metadata.name is required", path)
	}
	if len(def.Spec.Nodes) == 0 {
		return nil, fmt.Errorf("pipelineconfig: %s: spec.nodes must contain at least one DAG node", path)
	}

	return &def, nil
}

// SourcePath returns the file path Parse loaded this definition from, for
// use in CLI diagnostics.
func (d *PipelineDefinition) SourcePath() string {
	return d.sourcePath
}

// ValidationIssue describes one problem found by ValidateDAG, at a
// severity that lets `testforge validate` distinguish hard failures
// (unknown node type, cycle, dangling dependsOn reference) from style
// warnings it may add later.
type ValidationIssue struct {
	NodeID  string
	Message string
}

func (i ValidationIssue) String() string {
	if i.NodeID == "" {
		return i.Message
	}
	return fmt.Sprintf("node %q: %s", i.NodeID, i.Message)
}

// ValidateDAG checks node-type validity, duplicate IDs, dangling dependsOn
// references, and dependency cycles. It's the shared logic behind
// `testforge validate` and the pre-submission check `testforge run`
// performs before ever contacting pipeline-api-gateway, so a malformed
// pipeline fails fast locally instead of burning a control-plane API call.
func (d *PipelineDefinition) ValidateDAG() []ValidationIssue {
	var issues []ValidationIssue

	seen := make(map[string]bool, len(d.Spec.Nodes))
	for _, n := range d.Spec.Nodes {
		if n.ID == "" {
			issues = append(issues, ValidationIssue{Message: "node is missing required field 'id'"})
			continue
		}
		if seen[n.ID] {
			issues = append(issues, ValidationIssue{NodeID: n.ID, Message: "duplicate node id"})
		}
		seen[n.ID] = true

		if !KnownNodeTypes[n.Type] {
			issues = append(issues, ValidationIssue{NodeID: n.ID, Message: fmt.Sprintf("unknown node type %q", n.Type)})
		}
	}

	for _, n := range d.Spec.Nodes {
		for _, dep := range n.DependsOn {
			if !seen[dep] {
				issues = append(issues, ValidationIssue{NodeID: n.ID, Message: fmt.Sprintf("dependsOn references undefined node %q", dep)})
			}
		}
	}

	if cyclePath, ok := findCycle(d.Spec.Nodes); ok {
		issues = append(issues, ValidationIssue{Message: fmt.Sprintf("dependency cycle detected: %v", cyclePath)})
	}

	return issues
}

// findCycle runs a depth-first-search cycle detection over the dependsOn
// graph, returning the first cycle found for a readable error message.
func findCycle(nodes []DAGNode) ([]string, bool) {
	adjacency := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		adjacency[n.ID] = n.DependsOn
	}

	const (
		unvisited = 0
		visiting  = 1
		done      = 2
	)
	state := make(map[string]int, len(nodes))
	var path []string

	var visit func(id string) ([]string, bool)
	visit = func(id string) ([]string, bool) {
		state[id] = visiting
		path = append(path, id)

		for _, dep := range adjacency[id] {
			switch state[dep] {
			case visiting:
				return append(append([]string{}, path...), dep), true
			case unvisited:
				if cyc, found := visit(dep); found {
					return cyc, true
				}
			}
		}

		path = path[:len(path)-1]
		state[id] = done
		return nil, false
	}

	for _, n := range nodes {
		if state[n.ID] == unvisited {
			if cyc, found := visit(n.ID); found {
				return cyc, true
			}
		}
	}
	return nil, false
}
