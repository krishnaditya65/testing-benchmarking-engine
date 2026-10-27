import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

export type DataSeedStrategy = 'fixtureFile' | 'sqlScript' | 'syntheticGenerator';

export interface DataSeedNodeData {
  label: string;
  strategy: DataSeedStrategy;
  targetDatastore: string;
  seedSourceRef: string;
  recordCount?: number;
}

const STRATEGY_LABEL: Record<DataSeedStrategy, string> = {
  fixtureFile: 'Fixture file',
  sqlScript: 'SQL script',
  syntheticGenerator: 'Synthetic generator',
};

/** Represents a data-preparation step that seeds state before dependent steps run. */
function DataSeedNode({ data, selected }: NodeProps<DataSeedNodeData>) {
  return (
    <div
      style={{
        border: selected ? '2px solid #0f766e' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#f0fdfa',
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
              background: '#0f766e',
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            SEED
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'Data Seed'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#134e4a' }}>
          {STRATEGY_LABEL[data.strategy]} → {data.targetDatastore || 'unset'}
        </div>
        <div style={{ marginTop: 4, fontSize: 11, color: '#0f766e', wordBreak: 'break-all' }}>
          {data.seedSourceRef || 'no source configured'}
          {typeof data.recordCount === 'number' ? ` · ${data.recordCount} records` : ''}
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export default memo(DataSeedNode);
