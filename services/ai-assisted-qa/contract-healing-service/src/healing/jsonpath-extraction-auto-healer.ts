/**
 * jsonpath-extraction-auto-healer.ts
 *
 * Given a `SchemaDiff` produced by `openapi-schema-diff-detector.ts` and a pipeline's existing
 * `DynamicVariableExtraction` (a JSONPath expression that pulls a value out of an upstream
 * ApiRequestNode's response body, to interpolate into a downstream node via `{{nodeId.field}}` —
 * see `ApiRequestNode.bodyTemplateJson` in `shared-libraries/dag-domain-model`), rewrites the
 * expression to track a renamed field and reports how confident it is.
 *
 * Only `FIELD_RENAMED` diffs are healable by rewriting an expression in place: a `FIELD_REMOVED`
 * has no target to rewrite to (the healer reports zero proposals and the caller should surface
 * it to a human), `FIELD_ADDED` has nothing for an *existing* extraction to reference, and
 * `TYPE_CHANGED` doesn't change which JSONPath segment resolves the field at all.
 */
import type { RenamedField, SchemaDiff } from "../domain/api-schema-diff.types";

/**
 * DynamicVariableExtraction
 *
 * The shape of a pipeline node's "pull this field out of the response and bind it to a
 * variable" config. This is conceptually part of `ApiRequestNode`'s dynamic-extraction
 * configuration in `shared-libraries/dag-domain-model` (today that package models the request
 * side of an ApiRequestNode but not yet a first-class extraction sub-type) — defined locally
 * here rather than invented as a divergent shape, and should be replaced with an import from
 * `@shared-libraries/dag-domain-model` once that package grows a
 * `DynamicVariableExtraction`/`ApiRequestNode.responseExtractions` type, per this monorepo's
 * "shared types are imported, never copy-pasted" convention (see ARCHITECTURE.md).
 */
export interface DynamicVariableExtraction {
  extractionId: string;
  /** The ApiRequestNode (see shared-libraries/dag-domain-model) this extraction reads from. */
  sourceNodeId: string;
  /** The variable name downstream nodes reference via `{{sourceNodeId.variableName}}`. */
  variableName: string;
  /** JSONPath-style expression, root-relative to the response body, e.g.
   * `"$.order.customerEmail"` or `"$.items[0].sku"`. This healer only handles dot/bracket-index
   * JSONPath subset used by the platform's extraction configs, not the full JSONPath grammar
   * (filters, recursive descent `..`, unions) — see `parseJsonPathSegments`. */
  jsonPathExpression: string;
}

export interface HealingProposal {
  extractionId: string;
  originalExpression: string;
  proposedExpression: string;
  /** 0..1, carried through from the triggering SchemaDiff's rename confidence, since the
   * healer's own rewrite is mechanical once a rename is trusted. */
  confidenceScore: number;
  /** Human-readable explanation for the PR description / audit log — see
   * `healing-proposal-pull-request-drafter.ts`. */
  rationale: string;
}

/**
 * Confidence at or above which a proposal is safe to auto-apply directly to the pipeline
 * definition instead of only drafting a PR for human review. Deliberately conservative and
 * documented here rather than tucked in a config file: auto-applying a wrong rewrite silently
 * breaks a test's data extraction in a way that's hard to notice (the pipeline may still "pass"
 * while reading `undefined`), so raising this constant should be a deliberate, reviewed
 * decision, not something changed by injecting an env var.
 */
export const AUTO_APPLY_CONFIDENCE_THRESHOLD = 0.85;

/**
 * JsonPathExtractionAutoHealer
 *
 * Stateless. `healExtraction` handles one (SchemaDiff[], DynamicVariableExtraction) pair;
 * callers loop over every extraction referencing a given source node's schema.
 */
export class JsonPathExtractionAutoHealer {
  /**
   * Attempts to heal a single extraction against a full diff report's changes. Returns `null`
   * when no diff in `diffs` affects this expression (nothing to heal) or when the only relevant
   * diff is a `FIELD_REMOVED` with no rename counterpart (nothing *can* be healed
   * automatically — that case should be surfaced to a human directly, not as a proposal).
   */
  healExtraction(extraction: DynamicVariableExtraction, diffs: SchemaDiff[]): HealingProposal | null {
    const segments = parseJsonPathSegments(extraction.jsonPathExpression);
    if (segments.length === 0) return null;

    for (const diff of diffs) {
      if (diff.kind !== "FIELD_RENAMED") continue;
      const healed = tryHealAgainstRename(segments, diff);
      if (healed) {
        return {
          extractionId: extraction.extractionId,
          originalExpression: extraction.jsonPathExpression,
          proposedExpression: healed,
          confidenceScore: diff.renameConfidence,
          rationale: buildRationale(diff, extraction.jsonPathExpression, healed),
        };
      }
    }
    return null;
  }
}

/** Splits `"$.order.customerEmail"` -> `["order", "customerEmail"]` and
 * `"$.items[0].sku"` -> `["items[0]", "sku"]`. Leading `$.`/`$` is stripped; empty input yields
 * an empty array (caller treats that as "not a healable expression"). */
function parseJsonPathSegments(expression: string): string[] {
  const withoutRoot = expression.replace(/^\$\.?/, "");
  if (withoutRoot.length === 0) return [];
  return withoutRoot.split(".").filter((segment) => segment.length > 0);
}

function segmentFieldName(segment: string): string {
  return segment.replace(/\[\d+\]$/, "");
}

function segmentArraySuffix(segment: string): string {
  const match = segment.match(/\[\d+\]$/);
  return match ? match[0] : "";
}

/**
 * Rewrites the segment matching `diff.previousField` (by name and container path, path derived
 * from the preceding segments joined with `.`) to `diff.newField.name`, preserving any `[n]`
 * array-index suffix. Returns `null` if the expression doesn't reference the renamed field at
 * all, so the caller can keep checking other diffs.
 */
function tryHealAgainstRename(segments: string[], diff: RenamedField): string | null {
  for (let index = 0; index < segments.length; index++) {
    const fieldName = segmentFieldName(segments[index]);
    if (fieldName !== diff.previousField.name) continue;

    const containerPath = segments
      .slice(0, index)
      .map(segmentFieldName)
      .join(".");
    if (containerPath !== diff.previousField.containerPath) continue;

    const rewritten = [...segments];
    rewritten[index] = `${diff.newField.name}${segmentArraySuffix(segments[index])}`;
    return `$.${rewritten.join(".")}`;
  }
  return null;
}

function buildRationale(diff: RenamedField, originalExpression: string, proposedExpression: string): string {
  const confidencePercent = Math.round(diff.renameConfidence * 100);
  return (
    `Backend schema diff detected field "${diff.previousField.name}" was renamed to ` +
    `"${diff.newField.name}" (${confidencePercent}% confidence, name+type similarity heuristic). ` +
    `Rewrote extraction expression from ${JSON.stringify(originalExpression)} to ` +
    `${JSON.stringify(proposedExpression)} so this pipeline node keeps resolving the same logical ` +
    `field instead of extracting undefined and failing downstream assertions silently.`
  );
}
