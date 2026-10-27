import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

export interface EnvironmentTeardownNodeData {
  label: string;
  targetEnvironment: string;
  releaseProvisionedInfrastructure: boolean;
  purgeSeededData: boolean;
}

/**
 * Represents the terminal cleanup step of a pipeline: releasing any
 * environment/infrastructure provisioned earlier in the DAG and optionally
 * purging data seeded by an upstream DataSeedNode.
 */
function EnvironmentTeardownNode({ data, selected }: NodeProps<EnvironmentTeardownNodeData>) {
  return (
    <div
      style={{
        border: selected ? '2px solid #334155' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#f8fafc',
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
              background: '#334155',
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            TEARDOWN
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'Environment Teardown'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#1e293b' }}>
          env: {data.targetEnvironment || 'unset'}
        </div>
        <div style={{ marginTop: 4, fontSize: 11, color: '#475569' }}>
          {data.releaseProvisionedInfrastructure ? 'releases infra' : 'keeps infra'} ·{' '}
          {data.purgeSeededData ? 'purges seeded data' : 'keeps seeded data'}
        </div>
      </div>
      {/* Terminal step: no source handle — nothing legally runs after teardown. */}
    </div>
  );
}

export default memo(EnvironmentTeardownNode);
