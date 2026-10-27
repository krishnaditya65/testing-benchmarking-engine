'use client';

import type { DragEvent } from 'react';

import type { PipelineNodeKind } from '@/lib/dag-domain-types';

interface PaletteEntry {
  kind: PipelineNodeKind;
  title: string;
  description: string;
  accentColor: string;
}

const PALETTE_ENTRIES: PaletteEntry[] = [
  {
    kind: 'apiRequest',
    title: 'API Request',
    description: 'Single HTTP call step',
    accentColor: '#2563eb',
  },
  {
    kind: 'loadTest',
    title: 'Load Test',
    description: 'Virtual-user driven load shape',
    accentColor: '#b45309',
  },
  {
    kind: 'dataSeed',
    title: 'Data Seed',
    description: 'Prepare fixtures/state before downstream steps',
    accentColor: '#0f766e',
  },
  {
    kind: 'assertion',
    title: 'Assertion',
    description: 'Contract validation + variable extraction',
    accentColor: '#6d28d9',
  },
  {
    kind: 'chaosInjection',
    title: 'Chaos Injection',
    description: 'Inject a controlled fault',
    accentColor: '#b91c1c',
  },
  {
    kind: 'environmentTeardown',
    title: 'Environment Teardown',
    description: 'Release infra / purge seeded data',
    accentColor: '#334155',
  },
];

/** MIME type used to carry the dragged node kind through the browser's native DnD API. */
export const PIPELINE_NODE_DRAG_MIME_TYPE = 'application/x-pipeline-node-kind';

function handlePaletteEntryDragStart(event: DragEvent<HTMLDivElement>, kind: PipelineNodeKind) {
  event.dataTransfer.setData(PIPELINE_NODE_DRAG_MIME_TYPE, kind);
  event.dataTransfer.effectAllowed = 'move';
}

/**
 * Sidebar listing every available pipeline step type. Each entry is
 * draggable onto DagPipelineCanvas, which reads PIPELINE_NODE_DRAG_MIME_TYPE
 * on drop to decide which node kind to instantiate.
 */
export default function PipelineNodePalette() {
  return (
    <aside
      style={{
        width: 220,
        borderRight: '1px solid #e2e8f0',
        padding: 12,
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        overflowY: 'auto',
      }}
    >
      <h2 style={{ fontSize: 13, fontWeight: 700, margin: '4px 0 8px', color: '#0f172a' }}>
        Step Palette
      </h2>
      {PALETTE_ENTRIES.map((entry) => (
        <div
          key={entry.kind}
          draggable
          onDragStart={(event) => handlePaletteEntryDragStart(event, entry.kind)}
          style={{
            cursor: 'grab',
            border: `1px solid ${entry.accentColor}33`,
            borderLeft: `4px solid ${entry.accentColor}`,
            borderRadius: 6,
            padding: '8px 10px',
            background: '#fff',
          }}
        >
          <div style={{ fontSize: 12, fontWeight: 700, color: '#0f172a' }}>{entry.title}</div>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{entry.description}</div>
        </div>
      ))}
    </aside>
  );
}
