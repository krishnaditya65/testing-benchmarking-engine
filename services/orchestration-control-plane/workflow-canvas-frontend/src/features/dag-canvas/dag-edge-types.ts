/**
 * Conditional branching is executed by the scheduler (outside this service),
 * but it must be *authorable* here: an operator wires an edge out of an
 * AssertionNode and picks which branch it represents. The scheduler reads
 * `data.condition` at run time to decide which downstream nodes to dispatch.
 */
export type PipelineEdgeCondition =
  | 'always'
  | 'onParentAssertionPassed'
  | 'onParentAssertionFailed';

export interface PipelineEdgeData {
  condition: PipelineEdgeCondition;
  /** Optional human-readable note shown as an edge label in the canvas. */
  label?: string;
}

export const PIPELINE_EDGE_CONDITION_OPTIONS: ReadonlyArray<{
  value: PipelineEdgeCondition;
  label: string;
}> = [
  { value: 'always', label: 'Always run' },
  { value: 'onParentAssertionPassed', label: 'On parent assertion passed' },
  { value: 'onParentAssertionFailed', label: 'On parent assertion failed' },
];

export function describePipelineEdgeCondition(condition: PipelineEdgeCondition): string {
  const match = PIPELINE_EDGE_CONDITION_OPTIONS.find((option) => option.value === condition);
  return match?.label ?? condition;
}

export function createDefaultPipelineEdgeData(): PipelineEdgeData {
  return { condition: 'always' };
}
