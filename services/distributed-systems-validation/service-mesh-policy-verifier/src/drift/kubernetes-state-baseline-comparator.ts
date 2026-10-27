/**
 * Implements "Infrastructure Drift Detection" (feature-manifest section 5, "Distributed Systems
 * & Infrastructure Validation") for Kubernetes cluster state: compares a live cluster's
 * namespaces, RBAC roles, and NetworkPolicies against an expected baseline manifest (YAML) and
 * reports structural drift — resources added, removed, or modified relative to that baseline.
 *
 * The baseline is authored as plain YAML (see `parseBaselineManifestYaml`) rather than requiring
 * a caller to hand-build the typed `KubernetesStateBaseline` shape, so it can live in a repo
 * alongside the Terraform/Crossplane definitions `environment-lifecycle-manager` provisions
 * from, reviewed the same way infrastructure-as-code is.
 */

import { parse as parseYaml } from 'yaml';

export interface NamespaceBaseline {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
}

export interface RbacRoleRuleBaseline {
  readonly apiGroups: readonly string[];
  readonly resources: readonly string[];
  readonly verbs: readonly string[];
}

export interface RbacRoleBaseline {
  readonly namespace: string;
  readonly name: string;
  readonly rules: readonly RbacRoleRuleBaseline[];
}

export interface NetworkPolicyBaseline {
  readonly namespace: string;
  readonly name: string;
  readonly podSelectorLabels: Readonly<Record<string, string>>;
  readonly ingressAllowedFromCidrs: readonly string[];
  readonly policyTypes: readonly ('Ingress' | 'Egress')[];
}

export interface KubernetesStateBaseline {
  readonly namespaces: readonly NamespaceBaseline[];
  readonly rbacRoles: readonly RbacRoleBaseline[];
  readonly networkPolicies: readonly NetworkPolicyBaseline[];
}

/** Parses a baseline manifest YAML document into a typed `KubernetesStateBaseline`. */
export function parseBaselineManifestYaml(yamlDocument: string): KubernetesStateBaseline {
  const parsed = parseYaml(yamlDocument) as Partial<KubernetesStateBaseline> | undefined;
  return {
    namespaces: parsed?.namespaces ?? [],
    rbacRoles: parsed?.rbacRoles ?? [],
    networkPolicies: parsed?.networkPolicies ?? [],
  };
}

export type DriftChangeKind = 'ADDED' | 'REMOVED' | 'MODIFIED';

export interface FieldLevelDiff {
  readonly fieldPath: string;
  readonly baselineValue: unknown;
  readonly liveValue: unknown;
}

export interface ResourceDrift {
  readonly resourceKind: 'Namespace' | 'RbacRole' | 'NetworkPolicy';
  readonly resourceKey: string;
  readonly changeKind: DriftChangeKind;
  /** Populated only for MODIFIED — the exact fields that differ, for a precise drift report. */
  readonly fieldDiffs: readonly FieldLevelDiff[];
}

export interface KubernetesDriftReport {
  readonly drifts: readonly ResourceDrift[];
  readonly hasDrift: boolean;
}

/**
 * Port over the live cluster's current state, shaped identically to `KubernetesStateBaseline` so
 * the same comparator logic runs regardless of whether the "live" side came from a real cluster
 * or (in tests) a hand-built fixture.
 *
 * NOTE: real wiring requires `@kubernetes/client-node`:
 *
 *   const coreApi = kc.makeApiClient(k8s.CoreV1Api);
 *   const namespaces = await coreApi.listNamespace();
 *   const rbacApi = kc.makeApiClient(k8s.RbacAuthorizationV1Api);
 *   const roles = await rbacApi.listRoleForAllNamespaces();
 *   const networkingApi = kc.makeApiClient(k8s.NetworkingV1Api);
 *   const netpols = await networkingApi.listNetworkPolicyForAllNamespaces();
 *   // then map each list's .items into NamespaceBaseline/RbacRoleBaseline/NetworkPolicyBaseline shape.
 */
export interface KubernetesLiveStateApiClientPort {
  fetchLiveState(): Promise<KubernetesStateBaseline>;
}

/** Stub `KubernetesLiveStateApiClientPort` for local development and tests. */
export class InMemoryKubernetesLiveStateApiClientAdapter implements KubernetesLiveStateApiClientPort {
  constructor(private liveState: KubernetesStateBaseline) {}

  setLiveState(liveState: KubernetesStateBaseline): void {
    this.liveState = liveState;
  }

  async fetchLiveState(): Promise<KubernetesStateBaseline> {
    return this.liveState;
  }
}

function namespaceKey(ns: NamespaceBaseline): string {
  return ns.name;
}

function rbacRoleKey(role: RbacRoleBaseline): string {
  return `${role.namespace}/${role.name}`;
}

function networkPolicyKey(policy: NetworkPolicyBaseline): string {
  return `${policy.namespace}/${policy.name}`;
}

/**
 * Generic structural diff over two keyed collections of the same resource shape: reports every
 * key present only in `live` as ADDED, every key present only in `baseline` as REMOVED, and for
 * keys present in both, does a shallow field-by-field comparison (including nested
 * object/array fields via a deep-equality check) to report MODIFIED with the exact fields that
 * changed.
 */
function diffKeyedResources<T extends Record<string, unknown>>(
  resourceKind: ResourceDrift['resourceKind'],
  baselineItems: readonly T[],
  liveItems: readonly T[],
  keyOf: (item: T) => string,
): ResourceDrift[] {
  const baselineByKey = new Map(baselineItems.map((item) => [keyOf(item), item]));
  const liveByKey = new Map(liveItems.map((item) => [keyOf(item), item]));
  const drifts: ResourceDrift[] = [];

  for (const [key, liveItem] of liveByKey) {
    if (!baselineByKey.has(key)) {
      drifts.push({ resourceKind, resourceKey: key, changeKind: 'ADDED', fieldDiffs: [] });
      continue;
    }
    const baselineItem = baselineByKey.get(key)!;
    const fieldDiffs = diffFields(baselineItem, liveItem);
    if (fieldDiffs.length > 0) {
      drifts.push({ resourceKind, resourceKey: key, changeKind: 'MODIFIED', fieldDiffs });
    }
  }

  for (const key of baselineByKey.keys()) {
    if (!liveByKey.has(key)) {
      drifts.push({ resourceKind, resourceKey: key, changeKind: 'REMOVED', fieldDiffs: [] });
    }
  }

  return drifts;
}

/** Shallow-keyed, deep-valued field comparison between two same-shaped records. */
function diffFields(baseline: Record<string, unknown>, live: Record<string, unknown>): FieldLevelDiff[] {
  const fieldDiffs: FieldLevelDiff[] = [];
  const allFieldNames = new Set([...Object.keys(baseline), ...Object.keys(live)]);

  for (const fieldName of allFieldNames) {
    const baselineValue = baseline[fieldName];
    const liveValue = live[fieldName];
    if (!deepEqual(baselineValue, liveValue)) {
      fieldDiffs.push({ fieldPath: fieldName, baselineValue, liveValue });
    }
  }

  return fieldDiffs;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') {
    return false;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false;
    }
    return a.every((value, index) => deepEqual(value, b[index]));
  }
  const aRecord = a as Record<string, unknown>;
  const bRecord = b as Record<string, unknown>;
  const aKeys = Object.keys(aRecord);
  const bKeys = Object.keys(bRecord);
  if (aKeys.length !== bKeys.length) {
    return false;
  }
  return aKeys.every((key) => deepEqual(aRecord[key], bRecord[key]));
}

/**
 * Compares a baseline manifest against live cluster state and reports every added, removed, or
 * modified Namespace, RBAC Role, and NetworkPolicy.
 */
export class KubernetesStateBaselineComparator {
  constructor(private readonly liveStateApiClient: KubernetesLiveStateApiClientPort) {}

  async compareAgainstLiveCluster(baseline: KubernetesStateBaseline): Promise<KubernetesDriftReport> {
    const liveState = await this.liveStateApiClient.fetchLiveState();
    return this.compare(baseline, liveState);
  }

  /** Pure comparison, exercised directly by unit tests without any cluster I/O. */
  compare(baseline: KubernetesStateBaseline, liveState: KubernetesStateBaseline): KubernetesDriftReport {
    const drifts: ResourceDrift[] = [
      ...diffKeyedResources(
        'Namespace',
        baseline.namespaces as unknown as Record<string, unknown>[],
        liveState.namespaces as unknown as Record<string, unknown>[],
        (item) => namespaceKey(item as unknown as NamespaceBaseline),
      ),
      ...diffKeyedResources(
        'RbacRole',
        baseline.rbacRoles as unknown as Record<string, unknown>[],
        liveState.rbacRoles as unknown as Record<string, unknown>[],
        (item) => rbacRoleKey(item as unknown as RbacRoleBaseline),
      ),
      ...diffKeyedResources(
        'NetworkPolicy',
        baseline.networkPolicies as unknown as Record<string, unknown>[],
        liveState.networkPolicies as unknown as Record<string, unknown>[],
        (item) => networkPolicyKey(item as unknown as NetworkPolicyBaseline),
      ),
    ];

    return { drifts, hasDrift: drifts.length > 0 };
  }
}
