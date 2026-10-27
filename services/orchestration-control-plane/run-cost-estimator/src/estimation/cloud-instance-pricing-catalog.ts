/**
 * cloud-instance-pricing-catalog.ts
 *
 * Static on-demand hourly pricing for the instance types
 * execution-fleet's protocol-load-worker-go / protocol-load-worker-rust
 * actually run on (see infrastructure/terraform/modules/cloud-account-bootstrap
 * for the node pools these are provisioned from). "Static" is a deliberate,
 * documented simplification — see the NOTE below — not an oversight.
 *
 * NOTE (approximation, not a billing integration): these are illustrative
 * on-demand list prices as of this catalog's last manual update, not a live
 * feed from AWS/GCP pricing APIs. Real hourly prices vary by region, change
 * over time, and differ further under spot/reserved pricing (which
 * environment-lifecycle-manager may actually provision under, per
 * ARCHITECTURE.md's "Load tests survive spot instance preemption" note) —
 * none of that is modeled here. Good enough for the platform's stated goal
 * ("show projected cost before Run is clicked" as a budgeting signal), not
 * good enough to reconcile against an actual cloud bill. A production
 * version of this catalog would be refreshed from each provider's pricing
 * API on a schedule rather than hand-maintained.
 */

export type CloudProvider = 'AWS' | 'GCP';

/**
 * The instance type families execution-fleet workers are sized against:
 * general-purpose (protocol-load-worker-go, moderate CPU/memory per virtual
 * user) and compute-optimized (protocol-load-worker-rust /
 * dag-ipld-benchmark-worker-rust, CPU-bound saturation workloads that
 * benefit from dedicated cores over burstable ones).
 */
export type WorkerInstanceType =
  | 'GENERAL_PURPOSE_MEDIUM' // e.g. AWS m6i.xlarge / GCP n2-standard-4
  | 'COMPUTE_OPTIMIZED_LARGE'; // e.g. AWS c6i.2xlarge / GCP c2-standard-8

export interface InstancePricingEntry {
  cloudProvider: CloudProvider;
  instanceType: WorkerInstanceType;
  /** The concrete cloud SKU this row approximates, for human cross-referencing only. */
  cloudSkuName: string;
  onDemandHourlyUsd: number;
  /** vCPU count, used only by carbon-footprint-estimator.ts to scale power draw. */
  vCpuCount: number;
}

/**
 * Illustrative on-demand hourly pricing, one row per (cloudProvider,
 * instanceType) pair actually used by execution-fleet's worker sizing.
 */
export const CLOUD_INSTANCE_PRICING_CATALOG: readonly InstancePricingEntry[] = [
  {
    cloudProvider: 'AWS',
    instanceType: 'GENERAL_PURPOSE_MEDIUM',
    cloudSkuName: 'm6i.xlarge',
    onDemandHourlyUsd: 0.192,
    vCpuCount: 4,
  },
  {
    cloudProvider: 'AWS',
    instanceType: 'COMPUTE_OPTIMIZED_LARGE',
    cloudSkuName: 'c6i.2xlarge',
    onDemandHourlyUsd: 0.34,
    vCpuCount: 8,
  },
  {
    cloudProvider: 'GCP',
    instanceType: 'GENERAL_PURPOSE_MEDIUM',
    cloudSkuName: 'n2-standard-4',
    onDemandHourlyUsd: 0.194,
    vCpuCount: 4,
  },
  {
    cloudProvider: 'GCP',
    instanceType: 'COMPUTE_OPTIMIZED_LARGE',
    cloudSkuName: 'c2-standard-8',
    onDemandHourlyUsd: 0.353,
    vCpuCount: 8,
  },
];

/** Looks up a catalog row, throwing rather than returning undefined so a typo'd
 * instance type/provider combination fails loudly at estimate time instead of
 * silently producing a $0 estimate. */
export function lookupInstancePricing(
  cloudProvider: CloudProvider,
  instanceType: WorkerInstanceType,
): InstancePricingEntry {
  const entry = CLOUD_INSTANCE_PRICING_CATALOG.find(
    (row) => row.cloudProvider === cloudProvider && row.instanceType === instanceType,
  );
  if (!entry) {
    throw new Error(
      `cloud-instance-pricing-catalog: no pricing entry for ${cloudProvider} ${instanceType}`,
    );
  }
  return entry;
}
