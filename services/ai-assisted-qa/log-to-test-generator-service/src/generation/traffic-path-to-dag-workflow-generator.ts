/**
 * traffic-path-to-dag-workflow-generator.ts
 *
 * Converts a `MinedTrafficPath` (from `frequent-traffic-path-miner.ts`) or a `CrashSequence`
 * (from `crash-sequence-extractor.ts`) into a candidate `PipelineDefinition` — the canonical DAG
 * shape from `@shared-libraries/dag-domain-model`, the same shape `pipeline-api-gateway`
 * persists and `dag-workflow-scheduler` executes. Every generated pipeline is a linear chain of
 * `ApiRequestNode`s (one per mined request step, `ON_SUCCESS` edges between them); a crash
 * sequence additionally gets a trailing `AssertionNode` asserting the final node reproduces the
 * observed crash status code, so running the generated pipeline is a genuine regression
 * reproduction, not just a replay.
 *
 * Output is explicitly a *candidate* — `pipeline-api-gateway`'s
 * `DagStructuralValidator`/`test-definition.service.ts` remains the authoritative save-time gate
 * (per that package's own README: "never trust client-side validation alone"). This generator
 * does not call the gateway itself; a caller (e.g. a CLI or scheduled job) is expected to POST
 * the result to `pipeline-api-gateway`'s `createPipelineDefinition` mutation.
 */
import { randomUUID } from "node:crypto";
import type {
  ApiRequestNode,
  AssertionNode,
  HttpProtocol,
  PipelineDefinition,
  PipelineEdge,
  PipelineNode,
} from "@shared-libraries/dag-domain-model";
import type { MinedTrafficPath } from "../pathmining/frequent-traffic-path-miner";
import type { CrashSequence } from "../pathmining/crash-sequence-extractor";

export interface TrafficPathToDagWorkflowGeneratorOptions {
  /** Base URL prepended to each step's `requestPath` to form `ApiRequestNode.targetUrl`. Mined
   * log entries only carry a path (e.g. "/api/cart"), never a full origin. */
  targetBaseUrl: string;
  /** Milliseconds nodes get before timing out. Defaults to 10_000. */
  requestTimeoutMs?: number;
  ownerId: string;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const HTTP_METHOD_TO_PROTOCOL: HttpProtocol = "REST";

/**
 * TrafficPathToDagWorkflowGenerator
 *
 * Stateless. Two entry points mirroring the two mining strategies:
 * `generateFromTrafficPath` (the common-path case) and `generateFromCrashSequence` (the
 * regression-reproduction case, which appends the trailing assertion).
 */
export class TrafficPathToDagWorkflowGenerator {
  generateFromTrafficPath(
    minedPath: MinedTrafficPath,
    options: TrafficPathToDagWorkflowGeneratorOptions,
  ): PipelineDefinition {
    const nodes: ApiRequestNode[] = minedPath.steps.map((step, index) =>
      buildApiRequestNode(step.httpMethod, step.requestPath, index, options),
    );
    const edges = buildLinearOnSuccessEdges(nodes);

    return buildPipelineDefinition({
      namePrefix: "mined-traffic-path",
      description:
        `Auto-generated from production traffic mining: this ${minedPath.steps.length}-step ` +
        `sequence occurred ${minedPath.occurrenceCount} times across ${minedPath.distinctSessionCount} ` +
        `distinct session(s) (example sessions: ${minedPath.exampleSessionIds.join(", ")}).`,
      nodes,
      edges,
      ownerId: options.ownerId,
    });
  }

  generateFromCrashSequence(
    crashSequence: CrashSequence,
    options: TrafficPathToDagWorkflowGeneratorOptions,
  ): PipelineDefinition {
    const requestNodes: ApiRequestNode[] = crashSequence.steps.map((step, index) =>
      buildApiRequestNode(step.httpMethod, step.requestPath, index, options),
    );
    const lastRequestNode = requestNodes[requestNodes.length - 1];

    const assertionNode: AssertionNode = {
      nodeId: `node-${requestNodes.length}-crash-assertion`,
      displayName: `Reproduces ${crashSequence.crashStatusCode} crash`,
      canvasPosition: { x: requestNodes.length * 220, y: 0 },
      kind: "ASSERTION",
      subjectNodeId: lastRequestNode.nodeId,
      metricPath: "response.statusCode",
      operator: "EQUALS",
      expectedValue: crashSequence.crashStatusCode,
      isBlocking: true,
    };

    const nodes: PipelineNode[] = [...requestNodes, assertionNode];
    const edges: PipelineEdge[] = [
      ...buildLinearOnSuccessEdges(requestNodes),
      {
        edgeId: `edge-${lastRequestNode.nodeId}-to-${assertionNode.nodeId}`,
        sourceNodeId: lastRequestNode.nodeId,
        targetNodeId: assertionNode.nodeId,
        predicate: { operator: "ALWAYS" },
      },
    ];

    return buildPipelineDefinition({
      namePrefix: "crash-reproduction",
      description:
        `Auto-generated from a production crash sequence: session ${crashSequence.sessionId} ` +
        `(trace ${crashSequence.traceId}) hit ${crashSequence.crashStatusCode} on the final step. ` +
        `The trailing ASSERTION node expects that status code to reproduce the regression.`,
      nodes,
      edges,
      ownerId: options.ownerId,
    });
  }
}

function buildApiRequestNode(
  httpMethod: string,
  requestPath: string,
  index: number,
  options: TrafficPathToDagWorkflowGeneratorOptions,
): ApiRequestNode {
  return {
    nodeId: `node-${index}-${sanitizeForId(httpMethod)}-${sanitizeForId(requestPath)}`,
    displayName: `${httpMethod} ${requestPath}`,
    canvasPosition: { x: index * 220, y: 0 },
    kind: "API_REQUEST",
    protocol: HTTP_METHOD_TO_PROTOCOL,
    targetUrl: `${options.targetBaseUrl.replace(/\/$/, "")}${requestPath}`,
    method: httpMethod,
    headers: {},
    timeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
  };
}

function buildLinearOnSuccessEdges(nodes: ApiRequestNode[]): PipelineEdge[] {
  const edges: PipelineEdge[] = [];
  for (let i = 0; i < nodes.length - 1; i++) {
    edges.push({
      edgeId: `edge-${nodes[i].nodeId}-to-${nodes[i + 1].nodeId}`,
      sourceNodeId: nodes[i].nodeId,
      targetNodeId: nodes[i + 1].nodeId,
      predicate: { operator: "ON_SUCCESS" },
    });
  }
  return edges;
}

function buildPipelineDefinition(args: {
  namePrefix: string;
  description: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  ownerId: string;
}): PipelineDefinition {
  const nowIso = new Date().toISOString();
  return {
    pipelineId: `${args.namePrefix}-${randomUUID()}`,
    name: `${args.namePrefix}-${randomUUID().slice(0, 8)}`,
    description: args.description,
    version: 1,
    nodes: args.nodes,
    edges: args.edges,
    createdAtIso: nowIso,
    updatedAtIso: nowIso,
    ownerId: args.ownerId,
  };
}

function sanitizeForId(value: string): string {
  return value.replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-+|-+$/g, "").toLowerCase() || "root";
}
