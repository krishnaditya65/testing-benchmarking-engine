import { GraphQLClient, gql } from 'graphql-request';
import { createClient as createGraphQLWsClient, type Client as GraphQLWsClient } from 'graphql-ws';

import type { PipelineDefinition, PipelineSummary } from './dag-domain-types';

const GATEWAY_HTTP_URL =
  process.env.NEXT_PUBLIC_PIPELINE_API_GATEWAY_URL ?? 'http://localhost:4000/graphql';

/** Derives the ws:// (or wss://) subscription endpoint from the http(s) gateway URL. */
function deriveGatewayWebSocketUrl(httpUrl: string): string {
  return httpUrl.replace(/^http/, 'ws');
}

const GATEWAY_WS_URL = deriveGatewayWebSocketUrl(GATEWAY_HTTP_URL);

/**
 * Query/mutation client for pipeline-api-gateway. TODO: attach a real auth
 * token (JWT from session cookie exchange) once auth is wired up — see
 * SCALING.md for how session state is expected to live server-side.
 */
export const pipelineApiGatewayClient = new GraphQLClient(GATEWAY_HTTP_URL, {
  headers: () => ({
    'content-type': 'application/json',
  }),
});

let cachedSubscriptionClient: GraphQLWsClient | null = null;

/** Lazily creates (and reuses) the graphql-ws client used for live run status subscriptions. */
export function getPipelineApiGatewaySubscriptionClient(): GraphQLWsClient {
  if (!cachedSubscriptionClient) {
    cachedSubscriptionClient = createGraphQLWsClient({
      url: GATEWAY_WS_URL,
      retryAttempts: Infinity,
      shouldRetry: () => true,
    });
  }
  return cachedSubscriptionClient;
}

// ---------------------------------------------------------------------------
// Query / mutation documents
// ---------------------------------------------------------------------------

export const GET_PIPELINE_DEFINITION_QUERY = gql`
  query GetPipelineDefinition($pipelineId: ID!) {
    pipeline(pipelineId: $pipelineId) {
      pipelineId
      displayName
      description
      updatedAt
      nodes {
        id
        type
        position {
          x
          y
        }
        data
      }
      edges {
        id
        source
        target
        sourceHandle
        data
      }
    }
  }
`;

export const LIST_PIPELINES_QUERY = gql`
  query ListPipelines {
    pipelines {
      pipelineId
      displayName
      nodeCount
      updatedAt
    }
  }
`;

export const SAVE_PIPELINE_DEFINITION_MUTATION = gql`
  mutation SavePipelineDefinition($input: SavePipelineDefinitionInput!) {
    savePipelineDefinition(input: $input) {
      pipelineId
      updatedAt
    }
  }
`;

export interface SavePipelineDefinitionInput {
  pipelineId: string;
  displayName: string;
  description?: string;
  nodes: PipelineDefinition['nodes'];
  edges: PipelineDefinition['edges'];
}

export async function fetchPipelineDefinition(pipelineId: string): Promise<PipelineDefinition> {
  const response = await pipelineApiGatewayClient.request<{ pipeline: PipelineDefinition }>(
    GET_PIPELINE_DEFINITION_QUERY,
    { pipelineId },
  );
  return response.pipeline;
}

export async function listPipelineSummaries(): Promise<PipelineSummary[]> {
  const response = await pipelineApiGatewayClient.request<{ pipelines: PipelineSummary[] }>(
    LIST_PIPELINES_QUERY,
  );
  return response.pipelines;
}

export async function savePipelineDefinition(
  input: SavePipelineDefinitionInput,
): Promise<{ pipelineId: string; updatedAt: string }> {
  const response = await pipelineApiGatewayClient.request<{
    savePipelineDefinition: { pipelineId: string; updatedAt: string };
  }>(SAVE_PIPELINE_DEFINITION_MUTATION, { input });
  return response.savePipelineDefinition;
}
