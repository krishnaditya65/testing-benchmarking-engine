import { memo } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';

export type AssertionComparisonOperator =
  | 'equals'
  | 'notEquals'
  | 'greaterThan'
  | 'lessThan'
  | 'contains'
  | 'matchesRegex';

export interface AssertionNodeData {
  label: string;
  /** JSONPath into the upstream step's response used for dynamic variable extraction. */
  jsonPathExpression: string;
  comparisonOperator: AssertionComparisonOperator;
  expectedValue: string;
  /** When set, the extracted value is bound to this name for downstream steps to reference. */
  extractAsVariableName?: string;
}

const OPERATOR_SYMBOL: Record<AssertionComparisonOperator, string> = {
  equals: '==',
  notEquals: '!=',
  greaterThan: '>',
  lessThan: '<',
  contains: 'contains',
  matchesRegex: '~=',
};

/**
 * Represents an automated contract-validation step. Extracts a value out of
 * an upstream response via `jsonPathExpression` (dynamic variable
 * extraction), compares it against `expectedValue`, and downstream edges
 * branch on whether this assertion passed or failed
 * (see dag-edge-types.ts PipelineEdgeCondition).
 */
function AssertionNode({ data, selected }: NodeProps<AssertionNodeData>) {
  return (
    <div
      style={{
        border: selected ? '2px solid #6d28d9' : '1px solid #cbd5e1',
        borderRadius: 8,
        background: '#f5f3ff',
        minWidth: 240,
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
              background: '#6d28d9',
              borderRadius: 4,
              padding: '2px 6px',
            }}
          >
            ASSERT
          </span>
          <strong style={{ fontSize: 13 }}>{data.label || 'Assertion'}</strong>
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: '#4c1d95', fontFamily: 'monospace' }}>
          {data.jsonPathExpression || '$.body'} {OPERATOR_SYMBOL[data.comparisonOperator]}{' '}
          {data.expectedValue || '""'}
        </div>
        {data.extractAsVariableName ? (
          <div style={{ marginTop: 4, fontSize: 11, color: '#7c3aed' }}>
            → extracts as ${data.extractAsVariableName}
          </div>
        ) : null}
      </div>
      {/* Two labeled source handles so pass/fail branches are visually distinct on the canvas. */}
      <Handle
        id="onAssertionPassed"
        type="source"
        position={Position.Right}
        style={{ top: '35%', background: '#16a34a' }}
      />
      <Handle
        id="onAssertionFailed"
        type="source"
        position={Position.Right}
        style={{ top: '65%', background: '#dc2626' }}
      />
    </div>
  );
}

export default memo(AssertionNode);
