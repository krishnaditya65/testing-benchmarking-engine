import DagPipelineCanvas from '@/features/dag-canvas/DagPipelineCanvas';
import { fetchPipelineDefinition } from '@/lib/graphql-client';
import type { PipelineDefinition } from '@/lib/dag-domain-types';

interface PipelineCanvasPageProps {
  params: { pipelineId: string };
}

function emptyPipelineDefinition(pipelineId: string): PipelineDefinition {
  return {
    pipelineId,
    displayName: 'Untitled pipeline',
    nodes: [],
    edges: [],
    updatedAt: new Date(0).toISOString(),
  };
}

/**
 * Route: /pipelines/[pipelineId]
 * Loads the pipeline's current DAG definition from pipeline-api-gateway and
 * renders it in the DagPipelineCanvas editor. If the pipeline doesn't exist
 * yet in the gateway (or the gateway is unreachable during local dev), falls
 * back to an empty canvas so the page still renders.
 */
export default async function PipelineCanvasPage({ params }: PipelineCanvasPageProps) {
  let pipeline: PipelineDefinition;

  try {
    pipeline = await fetchPipelineDefinition(params.pipelineId);
  } catch {
    pipeline = emptyPipelineDefinition(params.pipelineId);
  }

  return (
    <div style={{ height: '100vh', width: '100vw' }}>
      <DagPipelineCanvas pipeline={pipeline} />
    </div>
  );
}
