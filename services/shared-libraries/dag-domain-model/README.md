# dag-domain-model

The canonical TypeScript DAG domain model for the entire platform. This is the **single source
of truth** for what a pipeline, node, and edge look like — imported, never duplicated.

## Implements

- ARCHITECTURE.md naming conventions: *"Shared types live in `shared-libraries/dag-domain-model`
  and are imported — never copy-pasted — by both the frontend and the gateway, so DAG node/edge
  shapes cannot drift."*

## Who imports this

- `services/orchestration-control-plane/workflow-canvas-frontend` — renders `PipelineNode`
  variants on the React Flow canvas and runs `DagStructuralValidator` client-side for instant
  feedback as a user edits a graph.
- `services/orchestration-control-plane/pipeline-api-gateway` — persists `PipelineDefinition`
  and re-runs `DagStructuralValidator` server-side as the authoritative save-time gate (never
  trust client-side validation alone).
- `services/orchestration-control-plane/dag-workflow-scheduler` (Go) does **not** import this
  package directly (different language) but its dispatch payloads
  (`DagNodeDispatchCommand.nodeConfigJson`) are shaped to decode against these node variants —
  see `shared-libraries/protocol-contracts` for the language-agnostic wire contract.

**Do not redeclare `PipelineNode`, `PipelineEdge`, or `PipelineDefinition` in any other
package.** If a service needs a derived/narrower view of these types, define that view locally
but compose it from the exported types here (`Pick<ApiRequestNode, ...>`, etc.), don't restate
the fields.

## Contents

- `src/types/pipeline-node.ts` — `PipelineNode`, a discriminated union on `kind`:
  `ApiRequestNode | LoadTestNode | DataSeedNode | AssertionNode | ChaosInjectionNode |
  EnvironmentTeardownNode`.
- `src/types/pipeline-edge.ts` — `PipelineEdge` with a `PipelineEdgePredicate` supporting
  `ALWAYS | ON_SUCCESS | ON_FAILURE | EXPRESSION` conditional branching.
- `src/types/pipeline-definition.ts` — `PipelineDefinition`, the top-level saved/versioned DAG.
- `src/validation/dag-structural-validator.ts` — `DagStructuralValidator`, a pure, I/O-free
  class checking:
  1. referential integrity (edges reference real node ids),
  2. acyclicity (DFS white/gray/black cycle detection),
  3. reachability (every node reachable from some root via BFS).

  Called from both the frontend (instant canvas feedback) and the gateway (authoritative
  save-time gate) so both layers agree on what "valid" means — see call sites above.
- `src/diffing/dag-structural-diff.types.ts` — the **type contract** (not the evaluation logic)
  for "DAG definition versioning & structural diffing": `DiffableDagSnapshot`,
  `DagStructuralDiffReport`, `NodeStructuralChange`, `EdgeStructuralChange`. Imported by
  `pipeline-api-gateway`'s `src/test-definition/pipeline-definition-diff.service.ts`. The
  Go `dag-workflow-scheduler` service (`internal/diagnostics/dag_structural_diff_engine.go`)
  implements the *same* report shape independently in Go, per this file's own header comment on
  why the evaluation logic itself is duplicated rather than shared across the language boundary
  (see ADR 0002's precedent) — only the shape is meant to be identical between the two, and this
  file is the TypeScript half of that shared shape.

## Usage

```ts
import { DagStructuralValidator, type PipelineDefinition } from "@shared-libraries/dag-domain-model";

const validator = new DagStructuralValidator();
const result = validator.validate(pipelineDefinition);
if (!result.isValid) {
  // result.issues[].kind is one of DANGLING_EDGE_REFERENCE | CYCLE_DETECTED |
  // UNREACHABLE_NODE | NO_ROOT_NODE | DUPLICATE_NODE_ID
}
```
