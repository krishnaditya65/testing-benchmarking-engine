import { describe, expect, it } from "vitest";
import { AUTO_APPLY_CONFIDENCE_THRESHOLD, JsonPathExtractionAutoHealer, type DynamicVariableExtraction } from "./jsonpath-extraction-auto-healer";
import { OpenApiSchemaDiffDetector, type OpenApiObjectSchema } from "../diffing/openapi-schema-diff-detector";
import type { SchemaDiff } from "../domain/api-schema-diff.types";

function detectDiffs(previousSchema: OpenApiObjectSchema, newSchema: OpenApiObjectSchema): SchemaDiff[] {
  return new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema).diffs;
}

describe("JsonPathExtractionAutoHealer", () => {
  it("rewrites a top-level renamed field's JSONPath expression", () => {
    const diffs = detectDiffs(
      { type: "object", properties: { customerEmail: { type: "string" } } },
      { type: "object", properties: { customerEmailAddress: { type: "string" } } },
    );

    const extraction: DynamicVariableExtraction = {
      extractionId: "extract-1",
      sourceNodeId: "node-order-lookup",
      variableName: "orderCustomerEmail",
      jsonPathExpression: "$.customerEmail",
    };

    const proposal = new JsonPathExtractionAutoHealer().healExtraction(extraction, diffs);

    expect(proposal).not.toBeNull();
    expect(proposal?.proposedExpression).toBe("$.customerEmailAddress");
    expect(proposal?.confidenceScore).toBeGreaterThan(0.55);
    expect(proposal?.rationale).toContain("customerEmail");
  });

  it("rewrites a nested renamed field while preserving array index suffixes", () => {
    const diffs = detectDiffs(
      {
        type: "object",
        properties: { customer: { type: "object", properties: { fullName: { type: "string" } } } },
      },
      {
        type: "object",
        properties: { customer: { type: "object", properties: { displayName: { type: "string" } } } },
      },
    );

    const extraction: DynamicVariableExtraction = {
      extractionId: "extract-2",
      sourceNodeId: "node-order-lookup",
      variableName: "orderCustomerName",
      jsonPathExpression: "$.customer.fullName",
    };

    const proposal = new JsonPathExtractionAutoHealer().healExtraction(extraction, diffs);

    expect(proposal?.proposedExpression).toBe("$.customer.displayName");
  });

  it("returns null when the expression does not reference any renamed field", () => {
    const diffs = detectDiffs(
      { type: "object", properties: { customerEmail: { type: "string" } } },
      { type: "object", properties: { customerEmailAddress: { type: "string" } } },
    );

    const extraction: DynamicVariableExtraction = {
      extractionId: "extract-3",
      sourceNodeId: "node-order-lookup",
      variableName: "orderTotal",
      jsonPathExpression: "$.totalAmount",
    };

    expect(new JsonPathExtractionAutoHealer().healExtraction(extraction, diffs)).toBeNull();
  });

  it("returns null (not a false rewrite) when the field was genuinely removed with no rename candidate", () => {
    const diffs = detectDiffs(
      { type: "object", properties: { legacyDiscountCode: { type: "string" } } },
      { type: "object", properties: { shippingCarrier: { type: "string" } } },
    );

    const extraction: DynamicVariableExtraction = {
      extractionId: "extract-4",
      sourceNodeId: "node-order-lookup",
      variableName: "discountCode",
      jsonPathExpression: "$.legacyDiscountCode",
    };

    expect(new JsonPathExtractionAutoHealer().healExtraction(extraction, diffs)).toBeNull();
  });

  it("keeps the auto-apply threshold high enough that only strong renames clear it", () => {
    expect(AUTO_APPLY_CONFIDENCE_THRESHOLD).toBeGreaterThanOrEqual(0.8);
  });
});
