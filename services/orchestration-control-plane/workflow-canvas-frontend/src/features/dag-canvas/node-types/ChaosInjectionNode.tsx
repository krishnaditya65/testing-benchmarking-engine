import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

export type ChaosFaultType =
  | 'latencyInjection'
  | 'errorInjection'
  | 'podKill'
  | 'networkPartition'
  | 'resourceExhaustion';

export interface ChaosInjectionNodeData {
  label: string;
  faultType: ChaosFaultType;
  targetService: string;
  /** Percentage (0-100) of instances/traffic the fault is applied to. */
  blastRadiusPercent: number;
  durationSeconds: number;
}

const FAULT_LABEL: Record<ChaosFaultType, string> = {
  latencyInjection: 'Latency injection',
  errorInjection: 'Error injection',
  podKill: 'Pod kill',
  networkPartition: 'Network partition',
  resourceExhaustion: 'Resource exhaustion',
};

/** Represents a resiliency-testing step that injects a controlled fault into a target service. */
function ChaosInjectionNode({ data, selected }: NodeProps<ChaosInjectionNodeData>) {
  return (
    <div
      style={{
        border: selected ? '2px solid #b91c1c' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#fef2f2',
        minWidth: 230,
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
              background: '#b91c1c',
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            CHAOS
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'Chaos Injection'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#7f1d1d' }}>
          {FAULT_LABEL[data.faultType]} on {data.targetService || 'unset'}
        </div>
        <div style={{ marginTop: 4, fontSize: 11, color: '#991b1b' }}>
          blast radius {data.blastRadiusPercent}% · {data.durationSeconds}s
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export default memo(ChaosInjectionNode);
