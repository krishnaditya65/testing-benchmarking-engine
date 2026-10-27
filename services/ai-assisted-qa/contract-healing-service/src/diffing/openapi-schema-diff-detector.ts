/**
 * openapi-schema-diff-detector.ts
 *
 * Structural diff between two versions of an OpenAPI (JSON Schema) object shape. This is a
 * *real* diff, not a naive deep-equal: a field that vanished from the old schema and a field
 * that appeared in the new schema are, by default, indistinguishable from "removed one field,
 * added an unrelated one" — the whole point of "Autonomous Contract Healing" is telling those
 * apart from an honest rename so extraction logic can be healed instead of just failing loudly.
 *
 * Heuristic: for every (removed, added) pair at compatible container paths, score how likely it
 * is to be a rename using
 *   score = (1 - normalizedLevenshtein(oldName, newName)) * NAME_WEIGHT
 *         + typeCompatibilityScore(oldType, newType)      * TYPE_WEIGHT
 * and keep the highest-scoring pair above `renameConfidenceThreshold` as a `FIELD_RENAMED`
 * (each field can only be consumed by one match, greedy highest-score-first assignment).
 * Anything left over is a genuine `FIELD_REMOVED`/`FIELD_ADDED`. Fields present in both schemas
 * at the same name/path but with a changed `type` are reported as `TYPE_CHANGED` and excluded
 * from rename candidacy (a same-name field is never a "rename").
 */
import type {
  FieldAdded,
  FieldRemoved,
  FieldShape,
  JsonSchemaPrimitiveType,
  RenamedField,
  SchemaDiff,
  SchemaDiffReport,
  TypeChanged,
} from "../domain/api-schema-diff.types";

/**
 * Minimal JSON-Schema-shaped object description — enough of the OpenAPI `components.schemas.*`
 * subset to diff field sets and types. Real OpenAPI documents are far richer (refs,
 * oneOf/allOf, formats); this service only needs `properties`/`type`/`items`, which covers the
 * concrete schema-evolution cases (renamed keys, changed primitive types) the manifest bullet
 * calls out. A production version would resolve `$ref` and flatten `allOf` before reaching this
 * shape.
 */
export interface OpenApiObjectSchema {
  type: "object";
  properties: Record<string, OpenApiFieldSchema>;
}

export interface OpenApiFieldSchema {
  type: JsonSchemaPrimitiveType;
  /** Present when `type === "object"`; nested field definitions. */
  properties?: Record<string, OpenApiFieldSchema>;
  /** Present when `type === "array"`. */
  items?: { type: JsonSchemaPrimitiveType; properties?: Record<string, OpenApiFieldSchema> };
  /** Original Protobuf wire type name, when this spec was generated from a .proto definition
   * (e.g. via a proto-to-OpenAPI transcoder) — carried through so a TYPE_CHANGED diff can
   * mention the Protobuf type, not just the lossier JSON-Schema approximation. */
  protoTypeName?: string;
}

export interface SchemaDiffDetectorOptions {
  /** Minimum combined score (0..1) for a (removed, added) field pair to be reported as a
   * rename instead of an independent removal + addition. Defaults to 0.55, tuned so that
   * "email" -> "emailAddress" (high name similarity, same type) clears it, but
   * "email" -> "phoneNumber" (dissimilar name, same type) does not. */
  renameConfidenceThreshold?: number;
}

const DEFAULT_RENAME_CONFIDENCE_THRESHOLD = 0.55;
const NAME_SIMILARITY_WEIGHT = 0.7;
const TYPE_COMPATIBILITY_WEIGHT = 0.3;

/**
 * OpenApiSchemaDiffDetector
 *
 * Stateless — construct once, call `detectDiff` per (schemaName, previousSchema, newSchema)
 * comparison. `contract-healing-service`'s only structural-diff entry point; nothing else in
 * this service should hand-roll schema comparison.
 */
export class OpenApiSchemaDiffDetector {
  private readonly renameConfidenceThreshold: number;

  constructor(options: SchemaDiffDetectorOptions = {}) {
    this.renameConfidenceThreshold = options.renameConfidenceThreshold ?? DEFAULT_RENAME_CONFIDENCE_THRESHOLD;
  }

  detectDiff(
    schemaName: string,
    previousSpecVersion: string,
    previousSchema: OpenApiObjectSchema,
    newSpecVersion: string,
    newSchema: OpenApiObjectSchema,
  ): SchemaDiffReport {
    const previousFields = flattenFields(previousSchema);
    const newFields = flattenFields(newSchema);

    const previousByKey = new Map(previousFields.map((f) => [fieldKey(f), f] as const));
    const newByKey = new Map(newFields.map((f) => [fieldKey(f), f] as const));

    const typeChanges: TypeChanged[] = [];
    const removedCandidates: FieldShape[] = [];
    const addedCandidates: FieldShape[] = [];

    for (const field of previousFields) {
      const key = fieldKey(field);
      const matchInNew = newByKey.get(key);
      if (!matchInNew) {
        removedCandidates.push(field);
        continue;
      }
      if (matchInNew.type !== field.type) {
        typeChanges.push({
          kind: "TYPE_CHANGED",
          fieldPath: field.containerPath,
          fieldName: field.name,
          previousType: field.type,
          newType: matchInNew.type,
          isWireCompatible: isWireCompatibleWidening(field.type, matchInNew.type),
        });
      }
    }
    for (const field of newFields) {
      if (!previousByKey.has(fieldKey(field))) {
        addedCandidates.push(field);
      }
    }

    const { renamed, unmatchedRemoved, unmatchedAdded } = matchRenameCandidates(
      removedCandidates,
      addedCandidates,
      this.renameConfidenceThreshold,
    );

    const diffs: SchemaDiff[] = [
      ...renamed,
      ...typeChanges,
      ...unmatchedRemoved.map((f): FieldRemoved => ({ kind: "FIELD_REMOVED", removedField: f })),
      ...unmatchedAdded.map((f): FieldAdded => ({ kind: "FIELD_ADDED", addedField: f })),
    ];

    return {
      schemaName,
      previousSpecVersion,
      newSpecVersion,
      diffs,
      generatedAtIso: new Date().toISOString(),
    };
  }
}

function fieldKey(field: FieldShape): string {
  return `${field.containerPath}::${field.name}`;
}

function flattenFields(schema: OpenApiObjectSchema, containerPath = ""): FieldShape[] {
  const fields: FieldShape[] = [];
  for (const [name, def] of Object.entries(schema.properties)) {
    fields.push({
      name,
      containerPath,
      type: def.type,
      itemType: def.items?.type,
      protoTypeName: def.protoTypeName,
    });
    if (def.type === "object" && def.properties) {
      const nestedPath = containerPath ? `${containerPath}.${name}` : name;
      fields.push(...flattenFields({ type: "object", properties: def.properties }, nestedPath));
    }
    if (def.type === "array" && def.items?.properties) {
      const nestedPath = containerPath ? `${containerPath}.${name}[]` : `${name}[]`;
      fields.push(...flattenFields({ type: "object", properties: def.items.properties }, nestedPath));
    }
  }
  return fields;
}

/**
 * Greedy highest-score-first bipartite matching between removed and added fields at the same
 * container path (a rename that also moves a field to a different nesting level is out of
 * scope for this heuristic — cross-path renames are reported as an independent
 * removal + addition, which is the conservative/safe failure mode). Each field is matched at
 * most once.
 */
function matchRenameCandidates(
  removed: FieldShape[],
  added: FieldShape[],
  threshold: number,
): { renamed: RenamedField[]; unmatchedRemoved: FieldShape[]; unmatchedAdded: FieldShape[] } {
  type Candidate = { removedField: FieldShape; addedField: FieldShape; score: number };
  const candidates: Candidate[] = [];

  for (const removedField of removed) {
    for (const addedField of added) {
      if (removedField.containerPath !== addedField.containerPath) continue;
      const score = scoreRenameCandidate(removedField, addedField);
      if (score >= threshold) {
        candidates.push({ removedField, addedField, score });
      }
    }
  }
  candidates.sort((a, b) => b.score - a.score);

  const consumedRemoved = new Set<FieldShape>();
  const consumedAdded = new Set<FieldShape>();
  const renamed: RenamedField[] = [];

  for (const candidate of candidates) {
    if (consumedRemoved.has(candidate.removedField) || consumedAdded.has(candidate.addedField)) continue;
    consumedRemoved.add(candidate.removedField);
    consumedAdded.add(candidate.addedField);
    renamed.push({
      kind: "FIELD_RENAMED",
      previousField: candidate.removedField,
      newField: candidate.addedField,
      renameConfidence: candidate.score,
    });
  }

  return {
    renamed,
    unmatchedRemoved: removed.filter((f) => !consumedRemoved.has(f)),
    unmatchedAdded: added.filter((f) => !consumedAdded.has(f)),
  };
}

/** score in [0, 1]: weighted blend of field-name similarity and type compatibility. */
function scoreRenameCandidate(previousField: FieldShape, newField: FieldShape): number {
  const nameSimilarity = normalizedNameSimilarity(previousField.name, newField.name);
  const typeCompatibility = typeCompatibilityScore(previousField.type, newField.type);
  return nameSimilarity * NAME_SIMILARITY_WEIGHT + typeCompatibility * TYPE_COMPATIBILITY_WEIGHT;
}

/** 1 - (levenshtein distance / longer string length), so identical names score 1 and
 * completely dissimilar equal-length names score close to 0. */
function normalizedNameSimilarity(a: string, b: string): number {
  const distance = levenshteinDistance(a.toLowerCase(), b.toLowerCase());
  const longerLength = Math.max(a.length, b.length, 1);
  return 1 - distance / longerLength;
}

function typeCompatibilityScore(previousType: JsonSchemaPrimitiveType, newType: JsonSchemaPrimitiveType): number {
  if (previousType === newType) return 1;
  if (isWireCompatibleWidening(previousType, newType)) return 0.7;
  return 0;
}

/** Type pairs that represent a safe widening rather than a breaking narrowing — e.g. an
 * `integer` field re-declared as `number` (Protobuf int32 -> int64 style widening), still
 * readable by the same JSONPath extraction without a rewrite. */
function isWireCompatibleWidening(previousType: JsonSchemaPrimitiveType, newType: JsonSchemaPrimitiveType): boolean {
  return (previousType === "integer" && newType === "number") || (previousType === "number" && newType === "integer");
}

/** Classic O(n*m) dynamic-programming edit distance. Field names in practice are short
 * (single identifiers), so this is comfortably fast without needing a smarter algorithm. */
function levenshteinDistance(a: string, b: string): number {
  const rows = a.length + 1;
  const cols = b.length + 1;
  const matrix: number[][] = Array.from({ length: rows }, () => new Array<number>(cols).fill(0));

  for (let i = 0; i < rows; i++) matrix[i][0] = i;
  for (let j = 0; j < cols; j++) matrix[0][j] = j;

  for (let i = 1; i < rows; i++) {
    for (let j = 1; j < cols; j++) {
      const substitutionCost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1, // deletion
        matrix[i][j - 1] + 1, // insertion
        matrix[i - 1][j - 1] + substitutionCost, // substitution
      );
    }
  }
  return matrix[rows - 1][cols - 1];
}
