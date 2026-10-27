/**
 * Implements "Service Mesh Route Verification" (feature-manifest section 5, "Distributed
 * Systems & Infrastructure Validation") for Istio: given a target service's real
 * `DestinationRule` outlier-detection config, verifies — by replaying a simulated traffic
 * pattern through the same ejection algorithm Istio's Envoy sidecars run — that the circuit
 * breaker actually trips before a failing backend can cascade, and that the service degrades
 * gracefully (remaining healthy replicas absorb traffic) rather than the whole host pool going
 * dark at once.
 *
 * This does not attempt to be a byte-for-byte reimplementation of Envoy's outlier-detection C++
 * (see https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier), but
 * it implements the same core rule Istio's docs describe: consecutive 5xx/gateway errors from
 * one endpoint, observed within `interval`, ejects that endpoint from the load-balancing pool
 * for `baseEjectionTime * ejectionCount`, capped so at most `maxEjectionPercent` of the pool is
 * ever ejected at once.
 */

/** Mirrors Istio's `DestinationRule.spec.trafficPolicy.outlierDetection` field shape. */
export interface IstioOutlierDetectionConfig {
  /** Number of consecutive server-side (5xx/gateway) errors before ejecting an endpoint. */
  readonly consecutiveErrors: number;
  /** ISO-8601-less Istio duration string, e.g. "30s", "1m". Time window errors are tallied over. */
  readonly interval: string;
  /** Base duration an ejected endpoint stays out of the pool, multiplied by ejection count. */
  readonly baseEjectionTime: string;
  /** Never eject more than this percentage of the pool, however many endpoints are failing. */
  readonly maxEjectionPercent: number;
}

/** Mirrors the subset of `DestinationRule.spec.trafficPolicy.connectionPool` this verifier reads. */
export interface IstioConnectionPoolConfig {
  readonly http: {
    readonly maxRequestsPerConnection?: number;
    readonly http1MaxPendingRequests?: number;
  };
}

/** Mirrors a real Istio `DestinationRule` manifest closely enough to drive this verifier from one. */
export interface IstioDestinationRuleConfig {
  readonly host: string;
  readonly trafficPolicy: {
    readonly outlierDetection: IstioOutlierDetectionConfig;
    readonly connectionPool?: IstioConnectionPoolConfig;
  };
}

/** One simulated request's outcome against one backend endpoint, in send order. */
export interface SimulatedRequestOutcome {
  readonly endpointId: string;
  readonly occurredAtMs: number;
  readonly succeeded: boolean;
  /** HTTP status the endpoint returned; only 5xx/gateway-class codes count toward ejection. */
  readonly httpStatusCode: number;
}

/** One endpoint's ejection window, produced by the simulation below. */
export interface EndpointEjection {
  readonly endpointId: string;
  readonly ejectedAtMs: number;
  readonly ejectionDurationMs: number;
  readonly ejectionCount: number;
  readonly triggeringConsecutiveErrorCount: number;
}

export interface CircuitBreakerVerificationResult {
  readonly destinationRuleHost: string;
  readonly totalEndpointsInPool: number;
  /** True if at least one endpoint was actually ejected during the simulated load. */
  readonly circuitTripped: boolean;
  readonly ejections: readonly EndpointEjection[];
  /**
   * True if, at every point in the simulation, the fraction of endpoints ejected stayed within
   * `maxEjectionPercent` and at least one endpoint remained in the pool to serve traffic — the
   * two conditions that separate "graceful degradation" from "the circuit breaker itself caused
   * a full outage by ejecting everything."
   */
  readonly degradedGracefully: boolean;
  readonly evidence: string;
}

const SERVER_ERROR_STATUS_THRESHOLD = 500;

/**
 * Parses Istio's compact duration strings ("30s", "1m", "500ms") into milliseconds. Istio
 * accepts a wider grammar (combined units like "1h30m") via Go's `time.ParseDuration`; this
 * parser covers the single-unit case those fields are conventionally authored with, which is
 * sufficient for the `interval`/`baseEjectionTime` fields this verifier reads.
 */
export function parseIstioDurationToMs(duration: string): number {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(duration.trim());
  if (!match) {
    throw new Error(
      `istio-circuit-breaker-verifier: cannot parse duration "${duration}" — expected a single-unit ` +
        `Istio duration like "30s", "1m", or "500ms"`,
    );
  }
  const [, quantityStr, unit] = match;
  const quantity = Number(quantityStr);
  const unitToMs: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000 };
  return quantity * unitToMs[unit as keyof typeof unitToMs]!;
}

/**
 * Port over the live cluster for reading a `DestinationRule` back from Kubernetes, so a
 * verification run can confirm the config it is testing against is the config that is actually
 * deployed (rather than trusting a caller-supplied copy that may have drifted — see
 * `../drift/kubernetes-state-baseline-comparator.ts` for the general drift-detection version of
 * that same concern).
 *
 * NOTE: real wiring requires `@kubernetes/client-node`'s `CustomObjectsApi`, since
 * `DestinationRule` is an Istio CRD, not a core Kubernetes type:
 *
 *   const kc = new k8s.KubeConfig(); kc.loadFromDefault();
 *   const api = kc.makeApiClient(k8s.CustomObjectsApi);
 *   const { body } = await api.getNamespacedCustomObject(
 *     'networking.istio.io', 'v1beta1', namespace, 'destinationrules', name,
 *   );
 *   // body.spec is the IstioDestinationRuleConfig-shaped payload.
 */
export interface KubernetesMeshApiClientPort {
  fetchIstioDestinationRule(namespace: string, name: string): Promise<IstioDestinationRuleConfig>;
}

/**
 * Stub `KubernetesMeshApiClientPort` for local development and tests: returns whatever
 * `DestinationRule` was registered via `seedDestinationRule`, without touching a real cluster.
 */
export class InMemoryKubernetesMeshApiClientAdapter implements KubernetesMeshApiClientPort {
  private readonly destinationRulesByNamespacedName = new Map<string, IstioDestinationRuleConfig>();

  seedDestinationRule(namespace: string, name: string, config: IstioDestinationRuleConfig): void {
    this.destinationRulesByNamespacedName.set(`${namespace}/${name}`, config);
  }

  async fetchIstioDestinationRule(namespace: string, name: string): Promise<IstioDestinationRuleConfig> {
    const key = `${namespace}/${name}`;
    const config = this.destinationRulesByNamespacedName.get(key);
    if (!config) {
      throw new Error(
        `InMemoryKubernetesMeshApiClientAdapter: no DestinationRule seeded for "${key}" — ` +
          `real wiring fetches this from the live cluster (see this class's doc comment for the ` +
          `@kubernetes/client-node CustomObjectsApi call this replaces)`,
      );
    }
    return config;
  }
}

/**
 * Replays a simulated request timeline through Istio's outlier-detection ejection algorithm and
 * verifies both that the circuit breaker trips and that it does so gracefully.
 */
export class IstioCircuitBreakerVerifier {
  constructor(private readonly meshApiClient: KubernetesMeshApiClientPort) {}

  /**
   * Fetches the live `DestinationRule` for (namespace, name) and verifies it against
   * simulatedLoad. Prefer this over `verifyAgainstConfig` in production so the config under test
   * is guaranteed to be what is actually deployed, not a possibly-stale caller-supplied copy.
   */
  async verifyLiveDestinationRule(
    namespace: string,
    name: string,
    simulatedLoad: readonly SimulatedRequestOutcome[],
  ): Promise<CircuitBreakerVerificationResult> {
    const config = await this.meshApiClient.fetchIstioDestinationRule(namespace, name);
    return this.verifyAgainstConfig(config, simulatedLoad);
  }

  /**
   * Verifies config's outlier-detection policy against simulatedLoad without any cluster I/O —
   * pure function over its inputs, used directly by unit tests and by
   * `verifyLiveDestinationRule` above.
   */
  verifyAgainstConfig(
    config: IstioDestinationRuleConfig,
    simulatedLoad: readonly SimulatedRequestOutcome[],
  ): CircuitBreakerVerificationResult {
    const { outlierDetection } = config.trafficPolicy;
    const intervalMs = parseIstioDurationToMs(outlierDetection.interval);
    const baseEjectionTimeMs = parseIstioDurationToMs(outlierDetection.baseEjectionTime);

    const endpointIds = Array.from(new Set(simulatedLoad.map((r) => r.endpointId)));
    const totalEndpointsInPool = endpointIds.length;

    const consecutiveErrorsByEndpoint = new Map<string, number>();
    const windowStartByEndpoint = new Map<string, number>();
    const ejectedUntilMsByEndpoint = new Map<string, number>();
    const ejectionCountByEndpoint = new Map<string, number>();
    const ejections: EndpointEjection[] = [];
    let everViolatedGracefulDegradation = false;

    const orderedRequests = [...simulatedLoad].sort((a, b) => a.occurredAtMs - b.occurredAtMs);

    for (const request of orderedRequests) {
      const windowStart = windowStartByEndpoint.get(request.endpointId);
      if (windowStart === undefined || request.occurredAtMs - windowStart > intervalMs) {
        windowStartByEndpoint.set(request.endpointId, request.occurredAtMs);
        consecutiveErrorsByEndpoint.set(request.endpointId, 0);
      }

      const isServerError = !request.succeeded && request.httpStatusCode >= SERVER_ERROR_STATUS_THRESHOLD;
      if (isServerError) {
        const nextCount = (consecutiveErrorsByEndpoint.get(request.endpointId) ?? 0) + 1;
        consecutiveErrorsByEndpoint.set(request.endpointId, nextCount);

        const alreadyEjected = (ejectedUntilMsByEndpoint.get(request.endpointId) ?? 0) > request.occurredAtMs;
        if (nextCount >= outlierDetection.consecutiveErrors && !alreadyEjected) {
          const currentlyEjectedCount = countCurrentlyEjected(ejectedUntilMsByEndpoint, request.occurredAtMs);
          const maxEjectable = Math.floor((outlierDetection.maxEjectionPercent / 100) * totalEndpointsInPool);

          if (currentlyEjectedCount >= maxEjectable) {
            // Istio itself refuses to eject past maxEjectionPercent — the endpoint keeps
            // serving despite qualifying, which is the intended safety valve, not a bug in this
            // verifier's simulation.
            continue;
          }

          const ejectionCount = (ejectionCountByEndpoint.get(request.endpointId) ?? 0) + 1;
          ejectionCountByEndpoint.set(request.endpointId, ejectionCount);
          const ejectionDurationMs = baseEjectionTimeMs * ejectionCount;
          ejectedUntilMsByEndpoint.set(request.endpointId, request.occurredAtMs + ejectionDurationMs);

          ejections.push({
            endpointId: request.endpointId,
            ejectedAtMs: request.occurredAtMs,
            ejectionDurationMs,
            ejectionCount,
            triggeringConsecutiveErrorCount: nextCount,
          });
        }
      } else {
        consecutiveErrorsByEndpoint.set(request.endpointId, 0);
      }

      const currentlyEjectedCount = countCurrentlyEjected(ejectedUntilMsByEndpoint, request.occurredAtMs);
      const ejectedFraction = totalEndpointsInPool === 0 ? 0 : currentlyEjectedCount / totalEndpointsInPool;
      const allEndpointsEjected = totalEndpointsInPool > 0 && currentlyEjectedCount === totalEndpointsInPool;
      if (ejectedFraction * 100 > outlierDetection.maxEjectionPercent || allEndpointsEjected) {
        everViolatedGracefulDegradation = true;
      }
    }

    const circuitTripped = ejections.length > 0;
    const degradedGracefully = circuitTripped && !everViolatedGracefulDegradation;

    return {
      destinationRuleHost: config.host,
      totalEndpointsInPool,
      circuitTripped,
      ejections,
      degradedGracefully,
      evidence: circuitTripped
        ? `${ejections.length} ejection(s) across ${new Set(ejections.map((e) => e.endpointId)).size} endpoint(s); ` +
          `graceful degradation ${degradedGracefully ? 'held' : 'VIOLATED'} (maxEjectionPercent=${outlierDetection.maxEjectionPercent}%, pool size=${totalEndpointsInPool})`
        : `no endpoint reached consecutiveErrors=${outlierDetection.consecutiveErrors} within interval=${outlierDetection.interval}; ` +
          `circuit breaker never tripped against the supplied simulated load`,
    };
  }
}

function countCurrentlyEjected(ejectedUntilMsByEndpoint: Map<string, number>, atMs: number): number {
  let count = 0;
  for (const ejectedUntilMs of ejectedUntilMsByEndpoint.values()) {
    if (ejectedUntilMs > atMs) {
      count++;
    }
  }
  return count;
}
