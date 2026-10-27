import Link from 'next/link';

import { listPipelineSummaries } from '@/lib/graphql-client';
import type { PipelineSummary } from '@/lib/dag-domain-types';

/**
 * Landing page: lists pipelines known to pipeline-api-gateway and links into
 * the DAG canvas for each. Falls back to an empty-state message if the
 * gateway is unreachable (e.g. during local frontend-only development)
 * rather than crashing the page.
 */
export default async function PipelineListPage() {
  let pipelines: PipelineSummary[] = [];
  let loadError: string | null = null;

  try {
    pipelines = await listPipelineSummaries();
  } catch (error) {
    loadError = error instanceof Error ? error.message : 'Unable to reach pipeline-api-gateway';
  }

  return (
    <main style={{ maxWidth: 720, margin: '0 auto', padding: '32px 16px' }}>
      <h1 style={{ fontSize: 22, fontWeight: 700 }}>Pipelines</h1>
      <p style={{ color: '#64748b', fontSize: 14 }}>
        Visual DAG test workflows managed by this Orchestration Control Plane.
      </p>

      {loadError ? (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            borderRadius: 8,
            background: '#fef2f2',
            color: '#991b1b',
            fontSize: 13,
          }}
        >
          Could not load pipelines from pipeline-api-gateway: {loadError}
        </div>
      ) : null}

      {!loadError && pipelines.length === 0 ? (
        <p style={{ marginTop: 16, color: '#94a3b8', fontSize: 13 }}>
          No pipelines yet. Create one from pipeline-api-gateway, then open it here at
          /pipelines/&lt;pipelineId&gt;.
        </p>
      ) : null}

      <ul style={{ listStyle: 'none', padding: 0, marginTop: 16 }}>
        {pipelines.map((pipeline) => (
          <li key={pipeline.pipelineId} style={{ marginBottom: 8 }}>
            <Link
              href={`/pipelines/${pipeline.pipelineId}`}
              style={{
                display: 'block',
                padding: '10px 14px',
                border: '1px solid #e2e8f0',
                borderRadius: 8,
                textDecoration: 'none',
                color: '#0f172a',
              }}
            >
              <strong style={{ fontSize: 14 }}>{pipeline.displayName}</strong>
              <div style={{ fontSize: 12, color: '#64748b', marginTop: 2 }}>
                {pipeline.nodeCount} steps · updated {pipeline.updatedAt}
              </div>
            </Link>
          </li>
        ))}
      </ul>
    </main>
  );
}
