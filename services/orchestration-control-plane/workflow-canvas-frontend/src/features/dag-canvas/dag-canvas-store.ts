import { create } from 'zustand';
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  type Connection,
  type NodeChange,
  type EdgeChange,
} from 'reactflow';

import type { PipelineNode, PipelineEdge, PipelineNodeKind } from '@/lib/dag-domain-types';
import { createDefaultPipelineEdgeData } from './dag-edge-types';

let nodeIdCounter = 0;

/** Generates a canvas-local id for a newly dropped node; the gateway assigns durable ids on save. */
function generatePipelineNodeId(kind: PipelineNodeKind): string {
  nodeIdCounter += 1;
  return `${kind}-${Date.now()}-${nodeIdCounter}`;
}

interface DagCanvasState {
  pipelineId: string | null;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  selectedNodeId: string | null;
  isDirty: boolean;

  loadPipeline: (pipelineId: string, nodes: PipelineNode[], edges: PipelineEdge[]) => void;
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  addPipelineNode: (kind: PipelineNodeKind, position: { x: number; y: number }) => PipelineNode;
  selectNode: (nodeId: string | null) => void;
  updateSelectedNodeData: (partialData: Record<string, unknown>) => void;
  markSaved: () => void;
}

/**
 * Client-side canvas state, deliberately decoupled from the
 * DagPipelineCanvas React component so node/edge mutation logic can be unit
 * tested and reused (e.g. by the node palette) without mounting React Flow.
 */
export const useDagCanvasStore = create<DagCanvasState>((set, get) => ({
  pipelineId: null,
  nodes: [],
  edges: [],
  selectedNodeId: null,
  isDirty: false,

  loadPipeline: (pipelineId, nodes, edges) =>
    set({ pipelineId, nodes, edges, selectedNodeId: null, isDirty: false }),

  onNodesChange: (changes) =>
    set((state) => ({
      nodes: applyNodeChanges(changes, state.nodes) as PipelineNode[],
      isDirty: true,
    })),

  onEdgesChange: (changes) =>
    set((state) => ({
      edges: applyEdgeChanges(changes, state.edges) as PipelineEdge[],
      isDirty: true,
    })),

  onConnect: (connection) =>
    set((state) => ({
      edges: addEdge(
        { ...connection, data: createDefaultPipelineEdgeData() },
        state.edges,
      ) as PipelineEdge[],
      isDirty: true,
    })),

  addPipelineNode: (kind, position) => {
    const id = generatePipelineNodeId(kind);
    const newNode = {
      id,
      type: kind,
      position,
      data: defaultDataForNodeKind(kind),
    } as PipelineNode;

    set((state) => ({ nodes: [...state.nodes, newNode], isDirty: true }));
    return newNode;
  },

  selectNode: (nodeId) => set({ selectedNodeId: nodeId }),

  updateSelectedNodeData: (partialData) => {
    const { selectedNodeId } = get();
    if (!selectedNodeId) return;

    set((state) => ({
      nodes: state.nodes.map((node) =>
        node.id === selectedNodeId
          ? { ...node, data: { ...node.data, ...partialData } }
          : node,
      ),
      isDirty: true,
    }));
  },

  markSaved: () => set({ isDirty: false }),
}));

function defaultDataForNodeKind(kind: PipelineNodeKind) {
  switch (kind) {
    case 'apiRequest':
      return { label: 'API Request', method: 'GET', url: '', headers: {} };
    case 'loadTest':
      return {
        label: 'Load Test',
        targetService: '',
        virtualUsers: 50,
        durationSeconds: 60,
        rampUpSeconds: 10,
      };
    case 'dataSeed':
      return {
        label: 'Data Seed',
        strategy: 'fixtureFile' as const,
        targetDatastore: '',
        seedSourceRef: '',
      };
    case 'assertion':
      return {
        label: 'Assertion',
        jsonPathExpression: '$.body',
        comparisonOperator: 'equals' as const,
        expectedValue: '',
      };
    case 'chaosInjection':
      return {
        label: 'Chaos Injection',
        faultType: 'latencyInjection' as const,
        targetService: '',
        blastRadiusPercent: 10,
        durationSeconds: 30,
      };
    case 'environmentTeardown':
      return {
        label: 'Environment Teardown',
        targetEnvironment: '',
        releaseProvisionedInfrastructure: true,
        purgeSeededData: true,
      };
    default: {
      const _exhaustiveCheck: never = kind;
      throw new Error(`Unhandled pipeline node kind: ${String(_exhaustiveCheck)}`);
    }
  }
}
