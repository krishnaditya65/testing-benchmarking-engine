# production-namespace-requires-teardown-node.rego
#
# Policy: any pipeline definition that targets a production namespace
# (input.runContext.targetNamespace matching production_namespace_prefixes)
# must include at least one ENVIRONMENT_TEARDOWN node. Rationale: a pipeline
# that provisions or mutates production-adjacent resources (via
# environment-lifecycle-manager, see
# services/orchestration-control-plane/environment-lifecycle-manager) but
# has no teardown node leaves that environment/resources orphaned — this has
# happened before as a plain authoring mistake (a pipeline author deletes
# the teardown node while rewiring a branch and doesn't notice), and the
# blast radius of leaked production-namespace resources is high enough that
# this is a BLOCKING violation, not merely a warning.
#
# Loaded (alongside no-chaos-injection-without-approval.rego) into an OPA
# sidecar reachable at OPA_SIDECAR_BASE_URL; see the NOTE in
# ../opa-rego-policy-evaluator.adapter.ts for how that sidecar is expected
# to be run.
package testforge.policies

import future.keywords.in

production_namespace_prefixes := {"prod-", "production-"}

is_production_namespace(namespace) if {
	some prefix in production_namespace_prefixes
	startswith(namespace, prefix)
}

has_teardown_node if {
	some node in input.nodes
	node.stepType == "EnvironmentTeardown"
}

# violations is the same shared partial set no-chaos-injection-without-approval.rego
# contributes to; see that file's header comment for how the two combine.
violations contains violation if {
	is_production_namespace(input.runContext.targetNamespace)
	not has_teardown_node

	violation := {
		"policy_id": "production-namespace-requires-teardown-node",
		"rule_name": "production_run_requires_teardown",
		"severity": "BLOCKING",
		"message": sprintf(
			"pipeline %q targets production namespace %q but contains no EnvironmentTeardown node; this run is blocked to prevent orphaned production-adjacent resources",
			[input.pipelineDefinitionId, input.runContext.targetNamespace],
		),
		"node_ids": [],
	}
}
