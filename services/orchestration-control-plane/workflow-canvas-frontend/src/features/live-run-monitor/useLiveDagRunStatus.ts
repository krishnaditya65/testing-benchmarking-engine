'use client';

import { useEffect, useRef, useState } from 'react';

import { getPipelineApiGatewaySubscriptionClient } from '@/lib/graphql-client';

export type PipelineRunStatus = 'pending' | 'running' | 'passed' | 'failed';
export type PipelineNodeRunStatus = 'pending' | 'running' | 'passed' | 'failed' | 'skipped';

/** Per-node telemetry correlated with its run status, as emitted by the gateway subscription. */
export interface PipelineNodeTelemetry {
  nodeId: string;
  status: PipelineNodeRunStatus;
  p99LatencyMs: number | null;
  errorRatePercent: number | null;
}

interface PipelineRunStatusUpdatedPayload {
  pipelineRunStatusUpdated: {
    runId: string;
    runStatus: PipelineRunStatus;
    nodeStatuses: PipelineNodeTelemetry[];
  };
}

const PIPELINE_RUN_STATUS_SUBSCRIPTION = /* GraphQL */ `
  subscription OnPipelineRunStatusUpdated($runId: ID!) {
    pipelineRunStatusUpdated(runId: $runId) {
      runId
      runStatus
      nodeStatuses {
        nodeId
        status
        p99LatencyMs
        errorRatePercent
      }
    }
  }
`;

export interface UseLiveDagRunStatusResult {
  runStatus: PipelineRunStatus | null;
  nodeStatuses: PipelineNodeTelemetry[];
  isConnected: boolean;
  error: string | null;
}

/**
 * Subscribes to pipeline-api-gateway's `pipelineRunStatusUpdated(runId)`
 * GraphQL subscription over WebSocket (graphql-ws) and exposes the latest
 * run-level and per-node status/telemetry. The subscription is opened on
 * mount / whenever runId changes, and always torn down on unmount or before
 * re-subscribing — graphql-ws's client handles reconnect-with-backoff
 * underneath, so a network blip resumes the same subscription automatically.
 */
export function useLiveDagRunStatus(runId: string | null): UseLiveDagRunStatusResult {
  const [runStatus, setRunStatus] = useState<PipelineRunStatus | null>(null);
  const [nodeStatuses, setNodeStatuses] = useState<PipelineNodeTelemetry[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Tracks the most recent subscription so a stale unsubscribe from a
  // previous effect run can never clobber state set by a newer one.
  const activeRunIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!runId) {
      setRunStatus(null);
      setNodeStatuses([]);
      setIsConnected(false);
      setError(null);
      return undefined;
    }

    activeRunIdRef.current = runId;
    setError(null);

    const subscriptionClient = getPipelineApiGatewaySubscriptionClient();

    const unsubscribe = subscriptionClient.subscribe<PipelineRunStatusUpdatedPayload>(
      { query: PIPELINE_RUN_STATUS_SUBSCRIPTION, variables: { runId } },
      {
        next: (message) => {
          if (activeRunIdRef.current !== runId) return;
          setIsConnected(true);
          const payload = message.data?.pipelineRunStatusUpdated;
          if (!payload) return;
          setRunStatus(payload.runStatus);
          setNodeStatuses(payload.nodeStatuses);
        },
        error: (err) => {
          if (activeRunIdRef.current !== runId) return;
          setIsConnected(false);
          setError(err instanceof Error ? err.message : 'Live run status subscription failed');
        },
        complete: () => {
          if (activeRunIdRef.current !== runId) return;
          setIsConnected(false);
        },
      },
    );

    setIsConnected(true);

    return () => {
      unsubscribe();
      if (activeRunIdRef.current === runId) {
        activeRunIdRef.current = null;
      }
    };
  }, [runId]);

  return { runStatus, nodeStatuses, isConnected, error };
}
