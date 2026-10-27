import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

/** HTTP verbs this control plane can schedule for a single request step. */
export type ApiRequestHttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';

export interface ApiRequestNodeData {
  label: string;
  method: ApiRequestHttpMethod;
  url: string;
  headers: Record<string, string>;
  requestBodyTemplate?: string;
}

const METHOD_BADGE_COLOR: Record<ApiRequestHttpMethod, string> = {
  GET: '#2563eb',
  POST: '#16a34a',
  PUT: '#d97706',
  PATCH: '#7c3aed',
  DELETE: '#dc2626',
};

/**
 * Represents a single HTTP call step ("multi-protocol pipelining" surface —
 * this is the HTTP-protocol member of the pipeline step family alongside
 * load, data-seed, assertion, chaos, and teardown steps).
 */
function ApiRequestNode({ data, selected }: NodeProps<ApiRequestNodeData>) {
  const headerCount = Object.keys(data.headers ?? {}).length;

  return (
    <div
      style={{
        border: selected ? '2px solid #1d4ed8' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#ffffff',
        minWidth: 220,
        boxShadow: '0 1px 2px rgba(0,0,0,0.06)',
      }}
    >
      <Handle type="target" position={Position.Left} />
      <div style={{ padding: '8px 12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span
            style={{
              fontSize: 11,
              fontWeight: 700,
              color: '#fff',
              background: METHOD_BADGE_COLOR[data.method],
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            {data.method}
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'API Request'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#475569', wordBreak: 'break-all' }}>
          {data.url || 'https://…'}
        </div>
        <div style={{ marginTop: 4, fontSize: 11, color: '#94a3b8' }}>
          {headerCount} header{headerCount === 1 ? '' : 's'}
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export default memo(ApiRequestNode);
