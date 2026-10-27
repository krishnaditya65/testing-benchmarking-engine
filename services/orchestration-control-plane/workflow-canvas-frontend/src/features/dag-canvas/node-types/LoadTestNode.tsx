import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

export interface LoadTestNodeData {
  label: string;
  targetService: string;
  virtualUsers: number;
  durationSeconds: number;
  rampUpSeconds: number;
}

/**
 * Represents a load-generation step. This node only *describes* the load
 * shape an operator wants scheduled; actual load execution happens on
 * dedicated worker fleets dispatched via `dag.node.dispatch.v1`, entirely
 * outside this control-plane frontend's concern.
 */
function LoadTestNode({ data, selected }: NodeProps<LoadTestNodeData>) {
  return (
    <div
      style={{
        border: selected ? '2px solid #b45309' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#fffbeb',
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
              background: '#b45309',
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            LOAD
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'Load Test'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#78350f' }}>
          target: {data.targetService || 'unset'}
        </div>
        <div style={{ marginTop: 4, fontSize: 11, color: '#92400e' }}>
          {data.virtualUsers} VUs · {data.durationSeconds}s duration · {data.rampUpSeconds}s ramp-up
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export default memo(LoadTestNode);
