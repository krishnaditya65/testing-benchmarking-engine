# Scaling notes

`contract-healing-service`'s domain logic (`OpenApiSchemaDiffDetector`,
`JsonPathExtractionAutoHealer`) is pure and I/O-free — no shared mutable state, no database — so
it scales the same way any stateless CPU-bound library does: run it wherever it's invoked from
(a CI job step, a webhook handler triggered by a schema-registry publish event, a scheduled
reconciliation job) with no coordination needed between concurrent invocations.

**Cost characteristics:** the rename-matching heuristic in `matchRenameCandidates` is
`O(removed * added)` per schema comparison (every removed field is scored against every added
field at the same container path before the greedy assignment). For realistic schema diffs
(tens of fields changing per release, not thousands), this is negligible; a schema with an
unusually large number of simultaneously removed+added fields at the same nesting level would be
the one case worth watching, since the heuristic explicitly does not restrict comparison to
same-type fields.

**The actual bottleneck in a real deployment is not this service** — it's the
`SourceControlPort` adapter (GitHub API rate limits on branch/PR creation) and whatever schema
source feeds `OpenApiSchemaDiffDetector` (fetching/parsing large OpenAPI documents). Both are
adapter-boundary concerns, not domain-logic ones, and can be scaled/cached independently (e.g.
memoizing parsed OpenAPI specs per spec version, batching multiple healing proposals for the
same pipeline into a single PR — which `HealingPullRequestDraft.proposals: HealingProposal[]`
is already shaped to support).

**Failure isolation:** a mis-scored rename heuristic produces a wrong `HealingProposal`, not a
wrong live change, as long as callers respect `AUTO_APPLY_CONFIDENCE_THRESHOLD` and route
everything below it through human-reviewed PRs (see README). The blast radius of a bug here is
bounded by that threshold discipline, not by anything about how the service is deployed or
scaled.
