# no-chaos-injection-without-approval.rego
#
# Policy: a pipeline that contains a CHAOS_INJECTION node targeting anything
# outside an explicitly pre-approved allowlist of chaos-safe target
# selectors must not start automatically — it requires a human sign-off via
# the approval-workflow feature (see ../../approval-workflow/).
#
# Rationale: chaos-ebpf-sidecar fault injection (see
# services/execution-fleet/chaos-ebpf-sidecar) is capable of process kills,
# CPU starvation, and disk I/O throttling against real infrastructure.
# Letting *any* pipeline definition trigger that automatically, with no
# human in the loop, is an unacceptable blast-radius risk — this policy is
# the guardrail pipeline-api-gateway's PolicyGateService enforces before
# dag-workflow-scheduler ever sees the run.
#
# Loaded (alongside production-namespace-requires-teardown-node.rego) into
# an OPA sidecar reachable at OPA_SIDECAR_BASE_URL; see the NOTE in
# ../opa-rego-policy-evaluator.adapter.ts for how that sidecar is expected
# to be run.
package testforge.policies

import future.keywords.in

# chaos_safe_target_selectors lists target selectors pre-approved for
# unattended chaos injection (e.g. dedicated chaos-testing namespaces that
# are never production-adjacent). Anything else routes through approval.
chaos_safe_target_selectors := {
	"namespace=chaos-testing-sandbox",
	"namespace=perf-lab",
}

# violations is a partial set every policy file in this package contributes
# to; opa-rego-policy-evaluator.adapter.ts queries
# `/v1/data/testforge/policies/violations` once and receives the union of
# every contributing rule across all loaded .rego files.
violations contains violation if {
	some node in input.nodes
	node.stepType == "ChaosInjection"
	not node.config.targetSelector in chaos_safe_target_selectors

	violation := {
		"policy_id": "no-chaos-injection-without-approval",
		"rule_name": "chaos_injection_requires_approval",
		"severity": "REQUIRES_APPROVAL",
		"message": sprintf(
			"chaos injection node %q targets %q, which is not in the pre-approved chaos-safe selector list; a human must approve this run before it starts",
			[node.id, node.config.targetSelector],
		),
		"node_ids": [node.id],
	}
}
