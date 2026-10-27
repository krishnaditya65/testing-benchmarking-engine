/**
 * carbon-footprint-estimator.ts
 *
 * Rough CO2-equivalent estimate for a pipeline run's compute usage, derived
 * from instance-hours x an average power draw per vCPU x a published
 * average grid carbon-intensity figure.
 *
 * NOTE (this is a coarse approximation, not a certified carbon accounting
 * method): a real carbon accounting tool (e.g. the Cloud Carbon Footprint
 * methodology this loosely follows) would use per-instance-family measured
 * wattage, per-datacenter-region grid intensity (which varies enormously —
 * a us-west-2 workload and an asia-southeast-1 workload have very
 * different carbon costs per kWh), PUE (data center power-usage
 * effectiveness) for the specific facility, and embodied-carbon
 * amortization for the hardware itself. None of that is modeled here. This
 * estimator exists to give pipeline authors a directional "this load test
 * burns roughly X kg of CO2" number alongside the dollar estimate — useful
 * for flagging an obviously oversized run (e.g. a 200k-VU, 4-hour soak test
 * someone meant to run for 4 minutes), not for sustainability reporting.
 */

/**
 * AVERAGE_GRID_CARBON_INTENSITY_KG_PER_KWH is a single global-average figure
 * (~0.42 kg CO2e/kWh), sourced from published estimates of the global
 * electricity grid's average carbon intensity (e.g. IEA/Ember global
 * average figures circa the early 2020s) — deliberately not
 * region-specific, since neither PipelineDefinition nor LoadTestNode
 * currently carries a target cloud region (see cloud-instance-pricing-catalog.ts's
 * NOTE on the same limitation for pricing).
 */
export const AVERAGE_GRID_CARBON_INTENSITY_KG_PER_KWH = 0.42;

/**
 * AVERAGE_WATTS_PER_VCPU approximates sustained power draw per vCPU under
 * load for a modern cloud server-grade CPU (a commonly cited ballpark used
 * in back-of-envelope cloud carbon estimates), before any PUE multiplier.
 */
export const AVERAGE_WATTS_PER_VCPU = 5;

/**
 * DATACENTER_POWER_USAGE_EFFECTIVENESS approximates how much *additional*
 * energy (cooling, power distribution losses, etc.) a datacenter consumes
 * per watt of actual server compute — 1.1 is a commonly cited figure for
 * modern hyperscale cloud datacenters (vs. ~1.5-2.0 for older/smaller
 * facilities), again used here only as a rough multiplier, not a
 * provider-specific measured value.
 */
export const DATACENTER_POWER_USAGE_EFFECTIVENESS = 1.1;

export interface CarbonFootprintEstimateInput {
  /** Total worker-hours across every LoadTestNode in the run (see PipelineCostEstimator). */
  totalWorkerHours: number;
  /** vCPU count of the instance type those worker-hours ran on. */
  vCpuCountPerWorker: number;
}

export interface CarbonFootprintEstimate {
  estimatedKgCo2e: number;
  estimatedKwh: number;
  /** Always true — present as an explicit field (not just a doc comment) so any
   * consumer surfacing this number in a UI is forced to acknowledge/display the caveat. */
  isRoughApproximation: true;
}

/**
 * Estimates CO2-equivalent emissions for the given compute usage:
 *   kWh = workerHours x vCpuCountPerWorker x wattsPerVcpu x PUE / 1000
 *   kgCO2e = kWh x gridCarbonIntensity
 */
export function estimateCarbonFootprint(input: CarbonFootprintEstimateInput): CarbonFootprintEstimate {
  const totalVcpuHours = input.totalWorkerHours * input.vCpuCountPerWorker;
  const rawKwh = (totalVcpuHours * AVERAGE_WATTS_PER_VCPU) / 1000;
  const estimatedKwh = rawKwh * DATACENTER_POWER_USAGE_EFFECTIVENESS;
  const estimatedKgCo2e = estimatedKwh * AVERAGE_GRID_CARBON_INTENSITY_KG_PER_KWH;

  return {
    estimatedKgCo2e: Math.round(estimatedKgCo2e * 1000) / 1000,
    estimatedKwh: Math.round(estimatedKwh * 1000) / 1000,
    isRoughApproximation: true,
  };
}
