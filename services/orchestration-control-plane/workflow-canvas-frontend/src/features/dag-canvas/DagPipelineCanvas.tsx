'use client';

import { useCallback, useEffect, useMemo, useState, type DragEvent, type MouseEvent } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type Node,
} from 'reactflow';
import 'reactflow/dist/style.css';

import { pipelineNodeTypeRegistry } from './node-types';
import PipelineNodePalette, { PIPELINE_NODE_DRAG_MIME_TYPE } from './PipelineNodePalette';
import { useDagCanvasStore } from './dag-canvas-store';
import { createDefaultPipelineEdgeData } from './dag-edge-types';
import type { PipelineNodeKind, PipelineDefinition } from '@/lib/dag-domain-types';
import { savePipelineDefinition } from '@/lib/graphql-client';

interface DagPipelineCanvasProps {
  pipeline: PipelineDefinition;
}

/**
 * The main visual DAG pipeline builder canvas. Renders nodes/edges via React
 * Flow, supports dragging new steps in from PipelineNodePalette, wiring
 * conditional edges between them, and persisting the result back to
 * pipeline-api-gateway over GraphQL.
 */
function DagPipelineCanvasInner({ pipeline }: DagPipelineCanvasProps) {
  const loadPipeline = useDagCanvasStore((state) => state.loadPipeline);
  const selectNode = useDagCanvasStore((state) => state.selectNode);
  const markSaved = useDagCanvasStore((state) => state.markSaved);
  const isDirty = useDagCanvasStore((state) => state.isDirty);

  const [nodes, setNodes, onNodesChangeInternal] = useNodesState(pipeline.nodes);
  const [edges, setEdges, onEdgesChangeInternal] = useEdgesState(pipeline.edges);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const { screenToFlowPosition } = useReactFlow();

  // Keep the decoupled store in sync so other consumers (e.g. a future
  // properties inspector panel) can read canvas state without depending on
  // React Flow's internal hooks.
  useEffect(() => {
    loadPipeline(pipeline.pipelineId, pipeline.nodes, pipeline.edges);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipeline.pipelineId]);

  const onNodesChange = useCallback<typeof onNodesChangeInternal>(
    (changes) => {
      onNodesChangeInternal(changes);
      useDagCanvasStore.getState().onNodesChange(changes);
    },
    [onNodesChangeInternal],
  );

  const onEdgesChange = useCallback<typeof onEdgesChangeInternal>(
    (changes) => {
      onEdgesChangeInternal(changes);
      useDagCanvasStore.getState().onEdgesChange(changes);
    },
    [onEdgesChangeInternal],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      const newEdge = {
        ...connection,
        id: `edge-${connection.source}-${connection.target}-${connection.sourceHandle ?? 'default'}`,
        data: createDefaultPipelineEdgeData(),
      };
      setEdges((current) => [...current, newEdge]);
      useDagCanvasStore.getState().onConnect(connection);
    },
    [setEdges],
  );

  const onNodeClick = useCallback(
    (_event: MouseEvent, node: Node) => {
      selectNode(node.id);
    },
    [selectNode],
  );

  const onPaneClick = useCallback(() => {
    selectNode(null);
  }, [selectNode]);

  const onDragOver = useCallback((event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      const kind = event.dataTransfer.getData(PIPELINE_NODE_DRAG_MIME_TYPE) as PipelineNodeKind;
      if (!kind) return;

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      // addPipelineNode both appends to the decoupled store and returns the
      // created node so we can mirror it into local React Flow state.
      const createdNode = useDagCanvasStore.getState().addPipelineNode(kind, position);
      setNodes((current) => [...current, createdNode]);
    },
    [screenToFlowPosition, setNodes],
  );

  const handleSavePipeline = useCallback(async () => {
    setIsSaving(true);
    setSaveError(null);
    try {
      await savePipelineDefinition({
        pipelineId: pipeline.pipelineId,
        displayName: pipeline.displayName,
        description: pipeline.description,
        nodes,
        edges,
      });
      markSaved();
    } catch (error) {
      setSaveError(error instanceof Error ? error.message : 'Failed to save pipeline');
    } finally {
      setIsSaving(false);
    }
  }, [pipeline.pipelineId, pipeline.displayName, pipeline.description, nodes, edges, markSaved]);

  const nodeTypes = useMemo(() => pipelineNodeTypeRegistry, []);

  return (
    <div style={{ display: 'flex', height: '100%', width: '100%' }}>
      <PipelineNodePalette />
      <div style={{ flex: 1, position: 'relative' }}>
        <div
          style={{
            position: 'absolute',
            top: 12,
            right: 12,
            zIndex: 10,
            display: 'flex',
            alignItems: 'center',
            gap: 8,
          }}
        >
          {saveError ? <span style={{ fontSize: 12, color: '#dc2626' }}>{saveError}</span> : null}
          <span style={{ fontSize: 12, color: '#64748b' }}>{isDirty ? 'Unsaved changes' : 'Saved'}</span>
          <button
            type="button"
            onClick={handleSavePipeline}
            disabled={isSaving}
            style={{
              padding: '6px 14px',
              borderRadius: 6,
              border: 'none',
              background: isSaving ? '#94a3b8' : '#1d4ed8',
              color: '#fff',
              fontSize: 13,
              fontWeight: 600,
              cursor: isSaving ? 'default' : 'pointer',
            }}
          >
            {isSaving ? 'Saving…' : 'Save pipeline'}
          </button>
        </div>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={onNodeClick}
          onPaneClick={onPaneClick}
          onDragOver={onDragOver}
          onDrop={onDrop}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap pannable zoomable />
        </ReactFlow>
      </div>
    </div>
  );
}

/** Public entry point: wraps the canvas in ReactFlowProvider so screenToFlowPosition works. */
export default function DagPipelineCanvas(props: DagPipelineCanvasProps) {
  return (
    <ReactFlowProvider>
      <DagPipelineCanvasInner {...props} />
    </ReactFlowProvider>
  );
}
