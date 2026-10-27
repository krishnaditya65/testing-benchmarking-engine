import { describe, expect, it } from "vitest";
import { OpenApiSchemaDiffDetector, type OpenApiObjectSchema } from "./openapi-schema-diff-detector";

describe("OpenApiSchemaDiffDetector", () => {
  it("detects a renamed field via name-similarity + type-compatibility heuristic", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: {
        customerEmail: { type: "string" },
        totalAmount: { type: "number" },
      },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: {
        customerEmailAddress: { type: "string" },
        totalAmount: { type: "number" },
      },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff(
      "Order",
      "v1",
      previousSchema,
      "v2",
      newSchema,
    );

    expect(report.diffs).toHaveLength(1);
    const [diff] = report.diffs;
    expect(diff.kind).toBe("FIELD_RENAMED");
    if (diff.kind === "FIELD_RENAMED") {
      expect(diff.previousField.name).toBe("customerEmail");
      expect(diff.newField.name).toBe("customerEmailAddress");
      expect(diff.renameConfidence).toBeGreaterThan(0.55);
    }
  });

  it("reports an unrelated removal and addition separately, not as a false-positive rename", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { legacyDiscountCode: { type: "string" } },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { shippingCarrier: { type: "string" } },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema);

    expect(report.diffs).toHaveLength(2);
    expect(report.diffs.map((d) => d.kind).sort()).toEqual(["FIELD_ADDED", "FIELD_REMOVED"]);
  });

  it("detects a type change on a field that kept its name", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { itemCount: { type: "integer" } },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { itemCount: { type: "string" } },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema);

    expect(report.diffs).toHaveLength(1);
    const [diff] = report.diffs;
    expect(diff.kind).toBe("TYPE_CHANGED");
    if (diff.kind === "TYPE_CHANGED") {
      expect(diff.previousType).toBe("integer");
      expect(diff.newType).toBe("string");
      expect(diff.isWireCompatible).toBe(false);
    }
  });

  it("treats a wire-compatible integer -> number widening as a compatible type change", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { orderId: { type: "integer" } },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { orderId: { type: "number" } },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema);

    const [diff] = report.diffs;
    expect(diff.kind).toBe("TYPE_CHANGED");
    if (diff.kind === "TYPE_CHANGED") {
      expect(diff.isWireCompatible).toBe(true);
    }
  });

  it("detects a rename nested under an object container path", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: {
        customer: {
          type: "object",
          properties: { fullName: { type: "string" } },
        },
      },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: {
        customer: {
          type: "object",
          properties: { displayName: { type: "string" } },
        },
      },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema);

    const renamed = report.diffs.find((d) => d.kind === "FIELD_RENAMED");
    expect(renamed).toBeDefined();
    if (renamed?.kind === "FIELD_RENAMED") {
      expect(renamed.previousField.containerPath).toBe("customer");
      expect(renamed.newField.containerPath).toBe("customer");
    }
  });

  it("does not match dissimilar-name same-type fields as a rename above the default threshold", () => {
    const previousSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { emailAddress: { type: "string" } },
    };
    const newSchema: OpenApiObjectSchema = {
      type: "object",
      properties: { phoneNumber: { type: "string" } },
    };

    const report = new OpenApiSchemaDiffDetector().detectDiff("Order", "v1", previousSchema, "v2", newSchema);

    expect(report.diffs.every((d) => d.kind !== "FIELD_RENAMED")).toBe(true);
  });
});
