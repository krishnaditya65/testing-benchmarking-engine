'use client';

import { useLiveDagRunStatus, type PipelineNodeRunStatus } from './useLiveDagRunStatus';

interface TelemetryCorrelationDashboardProps {
  runId: string;
}

const STATUS_BADGE_STYLE: Record<PipelineNodeRunStatus, { background: string; color: string }> = {
  pending: { background: '#e2e8f0', color: '#475569' },
  running: { background: '#dbeafe', color: '#1d4ed8' },
  passed: { background: '#dcfce7', color: '#15803d' },
  failed: { background: '#fee2e2', color: '#b91c1c' },
  skipped: { background: '#f1f5f9', color: '#94a3b8' },
};

function StatusBadge({ status }: { status: PipelineNodeRunStatus }) {
  const style = STATUS_BADGE_STYLE[status];
  return (
    <span
      style={{
        display: 'inline-block',
        fontSize: 11,
        fontWeight: 700,
        textTransform: 'uppercase',
        letterSpacing: 0.4,
        padding: '2px 8px',
        borderRadius: 999,
        background: style.background,
        color: style.color,
      }}
    >
      {status}
    </span>
  );
}

/** Flags telemetry that likely explains a failed node, to draw the operator's eye. */
function isTelemetryAnomalous(errorRatePercent: number | null, p99LatencyMs: number | null): boolean {
  return (errorRatePercent !== null && errorRatePercent > 5) || (p99LatencyMs !== null && p99LatencyMs > 2000);
}

/**
 * Renders live per-node run status correlated with the telemetry that
 * accompanies it (p99 latency, error rate), so an operator can see *why* a
 * node failed without leaving the run view. Backed by useLiveDagRunStatus's
 * GraphQL subscription to pipeline-api-gateway.
 */
export default function TelemetryCorrelationDashboard({ runId }: TelemetryCorrelationDashboardProps) {
  const { runStatus, nodeStatuses, isConnected, error } = useLiveDagRunStatus(runId);

  return (
    <section style={{ padding: 16, fontFamily: 'system-ui, -apple-system, sans-serif' }}>
      <header style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
        <h2 style={{ fontSize: 16, fontWeight: 700, margin: 0 }}>Run {runId}</h2>
        {runStatus ? <StatusBadge status={runStatus as PipelineNodeRunStatus} /> : null}
        <span
          style={{
            marginLeft: 'auto',
            fontSize: 12,
            color: isConnected ? '#15803d' : '#94a3b8',
          }}
        >
          {isConnected ? '● live' : '○ connecting…'}
        </span>
      </header>

      {error ? (
        <div style={{ color: '#b91c1c', fontSize: 13, marginBottom: 12 }}>
          Live status subscription error: {error}
        </div>
      ) : null}

      {nodeStatuses.length === 0 ? (
        <p style={{ color: '#94a3b8', fontSize: 13 }}>Waiting for the first status update…</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: 'left', borderBottom: '1px solid #e2e8f0' }}>
              <th style={{ padding: '6px 8px' }}>Node</th>
              <th style={{ padding: '6px 8px' }}>Status</th>
              <th style={{ padding: '6px 8px' }}>p99 latency</th>
              <th style={{ padding: '6px 8px' }}>Error rate</th>
            </tr>
          </thead>
          <tbody>
            {nodeStatuses.map((node) => {
              const anomalous = isTelemetryAnomalous(node.errorRatePercent, node.p99LatencyMs);
              return (
                <tr
                  key={node.nodeId}
                  style={{
                    borderBottom: '1px solid #f1f5f9',
                    background: anomalous ? '#fff7ed' : undefined,
                  }}
                >
                  <td style={{ padding: '6px 8px', fontFamily: 'monospace' }}>{node.nodeId}</td>
                  <td style={{ padding: '6px 8px' }}>
                    <StatusBadge status={node.status} />
                  </td>
                  <td style={{ padding: '6px 8px' }}>
                    {node.p99LatencyMs !== null ? `${node.p99LatencyMs} ms` : '—'}
                  </td>
                  <td style={{ padding: '6px 8px', color: anomalous ? '#c2410c' : undefined }}>
                    {node.errorRatePercent !== null ? `${node.errorRatePercent.toFixed(2)}%` : '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </section>
  );
}
