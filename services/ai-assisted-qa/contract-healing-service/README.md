# contract-healing-service

Implements the feature-manifest bullet **"Autonomous Contract Healing"** (section 7,
AI-Assisted QA & Analytics): when a backend's OpenAPI/Protobuf contract evolves — a renamed
JSON key, a changed Protobuf field type — pipeline nodes that extract data out of that backend's
responses (`DynamicVariableExtraction`s attached to an `ApiRequestNode`, see
`shared-libraries/dag-domain-model`) would otherwise start silently extracting `undefined` and
failing downstream assertions with no useful signal. This service detects the schema change,
proposes (and, above a confidence threshold, applies) the JSONPath rewrite that keeps the
extraction pointed at the right field.

## Why a standalone service (see ADR 0009)

Schema-diffing and JSONPath healing are pure, stateless domain logic with no dependency on the
Temporal/Postgres/GraphQL machinery `pipeline-api-gateway` owns — see
`docs/architecture-decision-records/0009-ai-assisted-qa-service-group.md` for the full
reasoning behind `services/ai-assisted-qa/` as its own top-level group.

## Structure

```
src/
├── domain/
│   └── api-schema-diff.types.ts              SchemaDiff discriminated union (RenamedField |
│                                              TypeChanged | FieldRemoved | FieldAdded)
├── diffing/
│   ├── openapi-schema-diff-detector.ts       OpenApiSchemaDiffDetector — structural diff +
│   │                                          rename-vs-removal heuristic
│   └── openapi-schema-diff-detector.spec.ts
├── healing/
│   ├── jsonpath-extraction-auto-healer.ts    JsonPathExtractionAutoHealer — rewrites a
│   │                                          DynamicVariableExtraction's JSONPath expression
│   ├── jsonpath-extraction-auto-healer.spec.ts
│   └── healing-proposal-pull-request-drafter.ts  SourceControlPort + GitHub stub adapter
└── index.ts
```

## OpenApiSchemaDiffDetector

Given two `OpenApiObjectSchema` versions of the same logical schema, flattens every field
(recursing into nested objects/array-item objects) and classifies each into one of:

- **FIELD_RENAMED** — a field disappeared from the old schema and a field appeared in the new
  one at the same container path, and the two are similar enough to call it a rename.
- **TYPE_CHANGED** — a field kept its name/path but its declared type changed (flags whether the
  change is a wire-compatible widening, e.g. `integer` -> `number`, vs. a breaking narrowing).
- **FIELD_REMOVED** / **FIELD_ADDED** — no plausible counterpart found; these are the "a human
  needs to look at this" cases.

**The rename-vs-removal heuristic** (not naive equality — see the file's top comment for the
full rationale): for every `(removed, added)` pair at the same container path, compute

```
score = normalizedLevenshteinSimilarity(oldName, newName) * 0.7
      + typeCompatibilityScore(oldType, newType)           * 0.3
```

and greedily accept the highest-scoring pairs above `renameConfidenceThreshold` (default `0.55`)
as renames, each field consumed at most once. `customerEmail -> customerEmailAddress` (same
type, high name similarity) clears the threshold; `emailAddress -> phoneNumber` (same type,
low name similarity) does not, and is correctly reported as an independent removal + addition.

## JsonPathExtractionAutoHealer

Takes a `DynamicVariableExtraction` (a JSONPath expression like `$.order.customerEmail`,
conceptually part of `ApiRequestNode`'s response-extraction config — see the type's doc comment
for why it's defined locally here rather than imported, pending that shape landing in
`shared-libraries/dag-domain-model`) and a `SchemaDiff[]`. If any `FIELD_RENAMED` diff's
previous field matches a segment of the expression (by name **and** container path), it rewrites
that segment to the new field name — preserving array-index suffixes like `[0]` — and returns a
`HealingProposal` with the new expression, a confidence score carried through from the triggering
diff, and a human-readable rationale. Returns `null` when nothing in the expression is affected,
or when the only relevant change is a `FIELD_REMOVED` with no rename counterpart (that case is
NOT healable — surface it to a human, don't guess).

### Auto-apply threshold

`AUTO_APPLY_CONFIDENCE_THRESHOLD = 0.85` (exported constant, `jsonpath-extraction-auto-healer.ts`).
A `HealingProposal` at or above this confidence is safe to auto-merge; anything below it MUST go
through human review via a normal (non-auto-merge) pull request. This is a deliberately
conservative, code-level constant rather than a runtime-tunable config value — auto-applying a
wrong rewrite fails silently (the pipeline keeps "passing" while reading `undefined` further
downstream), so raising it should require a reviewed code change, not an env var flip.

## SourceControlPort / GitHubHealingPullRequestAdapter

Hexagonal port (`src/healing/healing-proposal-pull-request-drafter.ts`), same pattern as
`pipeline-api-gateway`'s `SecretProviderPort`/`DagWorkflowSchedulerClientPort`: the healing
domain logic depends only on `SourceControlPort.draftHealingPullRequest(...)`, never on a
concrete Git host SDK. `GitHubHealingPullRequestAdapter` is shaped like a real `octokit`
integration (branch-per-healing-run, PR body summarizing every proposal's rationale) but is
stubbed — it performs no network I/O and returns a deterministic result, since this environment
has no live GitHub credentials. The method body's comment block shows the exact `octokit` calls
a live implementation would make.

## Running locally

```bash
npm install
npm test    # vitest run — diff-detector and healer unit tests, concrete fixtures
npm run build
```
