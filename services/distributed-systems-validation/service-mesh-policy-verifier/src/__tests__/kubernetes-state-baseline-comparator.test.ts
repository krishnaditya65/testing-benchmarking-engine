import {
  KubernetesStateBaselineComparator,
  InMemoryKubernetesLiveStateApiClientAdapter,
  parseBaselineManifestYaml,
  type KubernetesStateBaseline,
} from '../drift/kubernetes-state-baseline-comparator';

const BASELINE: KubernetesStateBaseline = {
  namespaces: [
    { name: 'testforge-execution-fleet', labels: { 'app.kubernetes.io/managed-by': 'crossplane' } },
    { name: 'testforge-telemetry-lake', labels: { 'app.kubernetes.io/managed-by': 'crossplane' } },
  ],
  rbacRoles: [
    {
      namespace: 'testforge-execution-fleet',
      name: 'protocol-load-worker-role',
      rules: [{ apiGroups: [''], resources: ['pods'], verbs: ['get', 'list'] }],
    },
  ],
  networkPolicies: [
    {
      namespace: 'testforge-execution-fleet',
      name: 'deny-cross-namespace-ingress',
      podSelectorLabels: { app: 'protocol-load-worker-go' },
      ingressAllowedFromCidrs: ['10.0.0.0/16'],
      policyTypes: ['Ingress'],
    },
  ],
};

describe('KubernetesStateBaselineComparator', () => {
  it('reports no drift when live state exactly matches the baseline', () => {
    const comparator = new KubernetesStateBaselineComparator(new InMemoryKubernetesLiveStateApiClientAdapter(BASELINE));
    const report = comparator.compare(BASELINE, BASELINE);

    expect(report.hasDrift).toBe(false);
    expect(report.drifts).toHaveLength(0);
  });

  it('reports an ADDED namespace present live but absent from baseline', () => {
    const comparator = new KubernetesStateBaselineComparator(new InMemoryKubernetesLiveStateApiClientAdapter(BASELINE));
    const liveState: KubernetesStateBaseline = {
      ...BASELINE,
      namespaces: [
        ...BASELINE.namespaces,
        { name: 'shadow-debug-namespace', labels: {} },
      ],
    };

    const report = comparator.compare(BASELINE, liveState);

    expect(report.hasDrift).toBe(true);
    expect(report.drifts).toContainEqual({
      resourceKind: 'Namespace',
      resourceKey: 'shadow-debug-namespace',
      changeKind: 'ADDED',
      fieldDiffs: [],
    });
  });

  it('reports a REMOVED RBAC role present in baseline but missing live', () => {
    const comparator = new KubernetesStateBaselineComparator(new InMemoryKubernetesLiveStateApiClientAdapter(BASELINE));
    const liveState: KubernetesStateBaseline = { ...BASELINE, rbacRoles: [] };

    const report = comparator.compare(BASELINE, liveState);

    expect(report.hasDrift).toBe(true);
    expect(report.drifts).toContainEqual({
      resourceKind: 'RbacRole',
      resourceKey: 'testforge-execution-fleet/protocol-load-worker-role',
      changeKind: 'REMOVED',
      fieldDiffs: [],
    });
  });

  it('reports a MODIFIED NetworkPolicy with the exact field that changed', () => {
    const comparator = new KubernetesStateBaselineComparator(new InMemoryKubernetesLiveStateApiClientAdapter(BASELINE));
    const liveState: KubernetesStateBaseline = {
      ...BASELINE,
      networkPolicies: [
        {
          ...BASELINE.networkPolicies[0]!,
          ingressAllowedFromCidrs: ['10.0.0.0/16', '0.0.0.0/0'],
        },
      ],
    };

    const report = comparator.compare(BASELINE, liveState);

    expect(report.hasDrift).toBe(true);
    const modification = report.drifts.find(
      (d) => d.resourceKind === 'NetworkPolicy' && d.changeKind === 'MODIFIED',
    );
    expect(modification).toBeDefined();
    expect(modification?.fieldDiffs).toContainEqual({
      fieldPath: 'ingressAllowedFromCidrs',
      baselineValue: ['10.0.0.0/16'],
      liveValue: ['10.0.0.0/16', '0.0.0.0/0'],
    });
  });

  it('resolves live state through the configured KubernetesLiveStateApiClientPort', async () => {
    const liveState: KubernetesStateBaseline = {
      ...BASELINE,
      namespaces: [...BASELINE.namespaces, { name: 'extra-namespace', labels: {} }],
    };
    const adapter = new InMemoryKubernetesLiveStateApiClientAdapter(liveState);
    const comparator = new KubernetesStateBaselineComparator(adapter);

    const report = await comparator.compareAgainstLiveCluster(BASELINE);

    expect(report.hasDrift).toBe(true);
    expect(report.drifts.some((d) => d.resourceKey === 'extra-namespace')).toBe(true);
  });

  it('parses a baseline manifest from YAML into the typed KubernetesStateBaseline shape', () => {
    const yamlDocument = `
namespaces:
  - name: testforge-execution-fleet
    labels:
      app.kubernetes.io/managed-by: crossplane
rbacRoles: []
networkPolicies: []
`;

    const parsed = parseBaselineManifestYaml(yamlDocument);

    expect(parsed.namespaces).toHaveLength(1);
    expect(parsed.namespaces[0]).toEqual({
      name: 'testforge-execution-fleet',
      labels: { 'app.kubernetes.io/managed-by': 'crossplane' },
    });
    expect(parsed.rbacRoles).toHaveLength(0);
    expect(parsed.networkPolicies).toHaveLength(0);
  });
});
