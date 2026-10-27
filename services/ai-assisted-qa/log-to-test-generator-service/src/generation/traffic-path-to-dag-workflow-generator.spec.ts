import { describe, expect, it } from "vitest";
import { TrafficPathToDagWorkflowGenerator } from "./traffic-path-to-dag-workflow-generator";
import type { MinedTrafficPath } from "../pathmining/frequent-traffic-path-miner";
import type { CrashSequence } from "../pathmining/crash-sequence-extractor";

describe("TrafficPathToDagWorkflowGenerator", () => {
  const options = { targetBaseUrl: "https://checkout.internal.example.com", ownerId: "ai-assisted-qa-bot" };

  it("converts a mined traffic path into a linear ApiRequestNode chain with ON_SUCCESS edges", () => {
    const minedPath: MinedTrafficPath = {
      steps: [
        { httpMethod: "GET", requestPath: "/cart" },
        { httpMethod: "POST", requestPath: "/checkout" },
        { httpMethod: "POST", requestPath: "/confirm" },
      ],
      occurrenceCount: 42,
      distinctSessionCount: 40,
      exampleSessionIds: ["s1", "s2"],
    };

    const pipeline = new TrafficPathToDagWorkflowGenerator().generateFromTrafficPath(minedPath, options);

    expect(pipeline.nodes).toHaveLength(3);
    expect(pipeline.nodes.every((n) => n.kind === "API_REQUEST")).toBe(true);
    expect(pipeline.edges).toHaveLength(2);
    expect(pipeline.edges.every((e) => e.predicate.operator === "ON_SUCCESS")).toBe(true);

    const first = pipeline.nodes[0];
    if (first.kind === "API_REQUEST") {
      expect(first.targetUrl).toBe("https://checkout.internal.example.com/cart");
      expect(first.method).toBe("GET");
    }
  });

  it("appends a blocking AssertionNode expecting the crash status when converting a crash sequence", () => {
    const crashSequence: CrashSequence = {
      sessionId: "s9",
      traceId: "trace-9",
      steps: [
        { httpMethod: "GET", requestPath: "/cart", statusCode: 200, durationMs: 20 },
        { httpMethod: "POST", requestPath: "/checkout", statusCode: 500, durationMs: 30 },
      ],
      crashStatusCode: 500,
      crashOccurredAtEpochMs: 123456,
    };

    const pipeline = new TrafficPathToDagWorkflowGenerator().generateFromCrashSequence(crashSequence, options);

    expect(pipeline.nodes).toHaveLength(3); // 2 request nodes + 1 assertion node
    const assertionNode = pipeline.nodes[pipeline.nodes.length - 1];
    expect(assertionNode.kind).toBe("ASSERTION");
    if (assertionNode.kind === "ASSERTION") {
      expect(assertionNode.expectedValue).toBe(500);
      expect(assertionNode.operator).toBe("EQUALS");
      expect(assertionNode.isBlocking).toBe(true);
      expect(assertionNode.subjectNodeId).toBe(pipeline.nodes[1].nodeId);
    }

    // Last edge connects the final request node to the assertion node.
    const lastEdge = pipeline.edges[pipeline.edges.length - 1];
    expect(lastEdge.targetNodeId).toBe(assertionNode.nodeId);
  });

  it("produces node/edge ids that are unique within the generated pipeline", () => {
    const minedPath: MinedTrafficPath = {
      steps: [
        { httpMethod: "GET", requestPath: "/a" },
        { httpMethod: "GET", requestPath: "/b" },
      ],
      occurrenceCount: 1,
      distinctSessionCount: 1,
      exampleSessionIds: ["s1"],
    };
    const pipeline = new TrafficPathToDagWorkflowGenerator().generateFromTrafficPath(minedPath, options);

    const nodeIds = pipeline.nodes.map((n) => n.nodeId);
    expect(new Set(nodeIds).size).toBe(nodeIds.length);
  });
});
