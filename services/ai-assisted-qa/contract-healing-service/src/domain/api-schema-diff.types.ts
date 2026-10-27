/**
 * api-schema-diff.types.ts
 *
 * Domain types for a detected change between two versions of a backend's OpenAPI schema.
 * `openapi-schema-diff-detector.ts` produces these; `jsonpath-extraction-auto-healer.ts`
 * consumes them to rewrite broken JSONPath extraction expressions. Kept as a discriminated
 * union on `kind` (same pattern as `PipelineNode` in `shared-libraries/dag-domain-model`) so
 * every consumer can exhaustively switch over the change shape instead of guessing which
 * optional fields are populated.
 */

/** Where in a schema a field lives, expressed as a dot path from the response root — e.g.
 * `"order.customer"` for a field nested under `order.customer.<fieldName>`. Intentionally
 * simpler than full JSON Pointer/JSONPath: this is a structural location for diffing, not an
 * extraction expression (see `jsonpath-extraction-auto-healer.ts` for the latter). */
export type SchemaFieldContainerPath = string;

export type JsonSchemaPrimitiveType =
  | "string"
  | "number"
  | "integer"
  | "boolean"
  | "object"
  | "array"
  | "null";

/** A minimal structural description of a single field, enough to compare compatibility across
 * schema versions without pulling in a full JSON-Schema validation library. */
export interface FieldShape {
  name: string;
  containerPath: SchemaFieldContainerPath;
  type: JsonSchemaPrimitiveType;
  /** Present when `type === "array"`, describing the element type. */
  itemType?: JsonSchemaPrimitiveType;
  /** Protobuf-side type name when this field diff originated from a .proto comparison rather
   * than OpenAPI (e.g. "int64", "string", "OrderStatus" for an enum) — carried alongside `type`
   * so a rationale can mention the wire-level type, not just the JSON-Schema approximation. */
  protoTypeName?: string;
}

/** A field present in the old schema is gone in the new one, and no plausible rename candidate
 * was found (see `openapi-schema-diff-detector.ts`'s rename-vs-removal heuristic). This is the
 * "cannot heal automatically, needs a human" case. */
export interface FieldRemoved {
  kind: "FIELD_REMOVED";
  removedField: FieldShape;
}

/** A field present in the new schema has no old-schema counterpart and was not matched as a
 * rename target. Informational for now — nothing to heal since no existing extraction could
 * have referenced a field that didn't exist yet. */
export interface FieldAdded {
  kind: "FIELD_ADDED";
  addedField: FieldShape;
}

/** The detector's structural-similarity heuristic (name edit-distance + type compatibility)
 * matched a removed field in the old schema to an added field in the new schema strongly
 * enough to call it a rename rather than an independent removal+addition. This is the primary
 * case `jsonpath-extraction-auto-healer.ts` acts on. */
export interface RenamedField {
  kind: "FIELD_RENAMED";
  previousField: FieldShape;
  newField: FieldShape;
  /** 0..1, how confident the detector is this is a genuine rename vs. coincidental similarity.
   * Combines name-similarity and type-compatibility sub-scores — see
   * `openapi-schema-diff-detector.ts`'s `scoreRenameCandidate`. */
  renameConfidence: number;
}

/** A field kept its name and container path but its declared type changed (e.g. a Protobuf
 * `int32` widened to `int64`, or an OpenAPI `string` became `integer`). Extraction expressions
 * referencing this field still resolve correctly (the JSONPath is unaffected), but a downstream
 * assertion comparing the extracted value against a stale expected type could start failing —
 * flagged here so callers can decide whether to also review assertions, even though
 * `jsonpath-extraction-auto-healer.ts` does not rewrite anything for this kind today. */
export interface TypeChanged {
  kind: "TYPE_CHANGED";
  fieldPath: SchemaFieldContainerPath;
  fieldName: string;
  previousType: JsonSchemaPrimitiveType;
  newType: JsonSchemaPrimitiveType;
  /** True when `previousType`/`newType` are considered wire-compatible (e.g. integer -> number,
   * or a Protobuf int32 -> int64 widening) so callers can distinguish "cosmetic" widenings from
   * breaking narrowings (e.g. string -> integer). */
  isWireCompatible: boolean;
}

/** The discriminated union every consumer switches over. Named `SchemaDiff` (singular per
 * detected change) rather than `SchemaDiffResult` — a full comparison run produces
 * `SchemaDiff[]`, see `openapi-schema-diff-detector.ts`'s `detectDiff` return type. */
export type SchemaDiff = RenamedField | TypeChanged | FieldRemoved | FieldAdded;

/** The full output of comparing two OpenAPI spec versions for one named schema/operation. */
export interface SchemaDiffReport {
  schemaName: string;
  previousSpecVersion: string;
  newSpecVersion: string;
  diffs: SchemaDiff[];
  generatedAtIso: string;
}
