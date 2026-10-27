/**
 * Implements "Service Mesh Route Verification" (feature-manifest section 5, "Distributed
 * Systems & Infrastructure Validation") for Linkerd, analogous to
 * `istio-circuit-breaker-verifier.ts` but for Linkerd's distinct resilience primitives.
 *
 * Linkerd does not have an Istio-style outlier-detection circuit breaker; its resilience model
 * is a per-route **retry budget** (`ServiceProfile.spec.retryBudget`, capping how much retry
 * traffic a route may generate as a ratio of original request volume) combined with a
 * **TrafficSplit** that can route a route's traffic toward a fallback backend. This verifier
 * checks the Linkerd-equivalent property to Istio's: that the retry budget actually throttles
 * retries before they turn into a self-inflicted retry storm, and that traffic instead
 * degrades onto the fallback backend the TrafficSplit names.
 */

/** Mirrors Linkerd's `ServiceProfile.spec.retryBudget` field shape. */
export interface LinkerdRetryBudgetConfig {
  /** Max retries allowed as a ratio of original request volume, e.g. 0.2 = 20% retry overhead. */
  readonly retryRatio: number;
  /** Retry budget floor so low-traffic routes are not starved of any retry allowance at all. */
  readonly minRetriesPerSecond: number;
  /** Linkerd duration string ("10s") — how far back retry/request counts are windowed. */
  readonly ttl: string;
}

/** Mirrors one `ServiceProfile.spec.routes[]` entry this verifier reads. */
export interface LinkerdRouteConfig {
  readonly name: string;
  readonly isRetryable: boolean;
}

/** Mirrors a Linkerd `TrafficSplit.spec.backends[]` entry naming a fallback backend and its weight. */
export interface LinkerdTrafficSplitBackend {
  readonly service: string;
  readonly weight: number;
}

/** Mirrors the subset of a `ServiceProfile` + associated `TrafficSplit` this verifier reads. */
export interface LinkerdFallbackPolicyConfig {
  readonly serviceName: string;
  readonly retryBudget: LinkerdRetryBudgetConfig;
  readonly routes: readonly LinkerdRouteConfig[];
  /** The TrafficSplit backends traffic should shift toward once the primary is failing. */
  readonly trafficSplitBackends: readonly LinkerdTrafficSplitBackend[];
  /** Name of the backend in trafficSplitBackends that is the primary (non-fallback) target. */
  readonly primaryBackendService: string;
}

/** One simulated request against a named route, in send order. */
export interface SimulatedRouteRequestOutcome {
  readonly routeName: string;
  readonly occurredAtMs: number;
  readonly succeeded: boolean;
  /** True if this request is itself a retry of a prior failed request, not an original request. */
  readonly isRetry: boolean;
  readonly servedByBackendService: string;
}

export interface RetryBudgetWindowSample {
  readonly windowStartMs: number;
  readonly originalRequestCount: number;
  readonly retryCount: number;
  readonly allowedRetryCount: number;
  readonly budgetExceeded: boolean;
}

export interface LinkerdFallbackVerificationResult {
  readonly serviceName: string;
  /** True if at least one window's retry volume actually exceeded the configured budget. */
  readonly retryBudgetEverExceeded: boolean;
  readonly retryBudgetWindows: readonly RetryBudgetWindowSample[];
  /**
   * True if requests continued to be served (by the fallback backend) through the failure
   * period without an unbounded retry storm against the primary — the Linkerd-equivalent of
   * Istio verifier's `degradedGracefully`.
   */
  readonly degradedGracefully: boolean;
  readonly evidence: string;
}

function parseLinkerdDurationToMs(duration: string): number {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(duration.trim());
  if (!match) {
    throw new Error(
      `linkerd-fallback-response-verifier: cannot parse duration "${duration}" — expected a ` +
        `single-unit duration like "10s" or "500ms"`,
    );
  }
  const [, quantityStr, unit] = match;
  const quantity = Number(quantityStr);
  const unitToMs: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000 };
  return quantity * unitToMs[unit as keyof typeof unitToMs]!;
}

/**
 * Port over the live cluster for reading a `ServiceProfile`/`TrafficSplit` pair back from
 * Kubernetes, mirroring `KubernetesMeshApiClientPort` in istio-circuit-breaker-verifier.ts.
 *
 * NOTE: real wiring requires `@kubernetes/client-node`'s `CustomObjectsApi` against Linkerd's
 * CRDs (`linkerd.io/v1alpha2` ServiceProfile, `split.smi-spec.io/v1alpha1` TrafficSplit):
 *
 *   const api = kc.makeApiClient(k8s.CustomObjectsApi);
 *   const profile = await api.getNamespacedCustomObject('linkerd.io', 'v1alpha2', namespace, 'serviceprofiles', name);
 *   const split = await api.getNamespacedCustomObject('split.smi-spec.io', 'v1alpha1', namespace, 'trafficsplits', name);
 *   // merge profile.spec.retryBudget/routes with split.spec.backends into a LinkerdFallbackPolicyConfig.
 */
export interface KubernetesLinkerdApiClientPort {
  fetchFallbackPolicyConfig(namespace: string, serviceName: string): Promise<LinkerdFallbackPolicyConfig>;
}

/**
 * Stub `KubernetesLinkerdApiClientPort` for local development and tests.
 */
export class InMemoryKubernetesLinkerdApiClientAdapter implements KubernetesLinkerdApiClientPort {
  private readonly configsByNamespacedName = new Map<string, LinkerdFallbackPolicyConfig>();

  seedFallbackPolicyConfig(namespace: string, serviceName: string, config: LinkerdFallbackPolicyConfig): void {
    this.configsByNamespacedName.set(`${namespace}/${serviceName}`, config);
  }

  async fetchFallbackPolicyConfig(namespace: string, serviceName: string): Promise<LinkerdFallbackPolicyConfig> {
    const key = `${namespace}/${serviceName}`;
    const config = this.configsByNamespacedName.get(key);
    if (!config) {
      throw new Error(
        `InMemoryKubernetesLinkerdApiClientAdapter: no fallback policy config seeded for "${key}" — ` +
          `real wiring fetches ServiceProfile+TrafficSplit from the live cluster (see this class's doc ` +
          `comment for the @kubernetes/client-node calls this replaces)`,
      );
    }
    return config;
  }
}

/**
 * Replays a simulated request timeline against a Linkerd retry-budget + TrafficSplit fallback
 * policy and verifies both that the retry budget actually throttles (rather than an unbounded
 * retry storm) and that the fallback backend absorbs traffic gracefully during the failure.
 */
export class LinkerdFallbackResponseVerifier {
  constructor(private readonly linkerdApiClient: KubernetesLinkerdApiClientPort) {}

  async verifyLiveFallbackPolicy(
    namespace: string,
    serviceName: string,
    simulatedLoad: readonly SimulatedRouteRequestOutcome[],
  ): Promise<LinkerdFallbackVerificationResult> {
    const config = await this.linkerdApiClient.fetchFallbackPolicyConfig(namespace, serviceName);
    return this.verifyAgainstConfig(config, simulatedLoad);
  }

  verifyAgainstConfig(
    config: LinkerdFallbackPolicyConfig,
    simulatedLoad: readonly SimulatedRouteRequestOutcome[],
  ): LinkerdFallbackVerificationResult {
    const windowMs = parseLinkerdDurationToMs(config.retryBudget.ttl);
    const orderedRequests = [...simulatedLoad].sort((a, b) => a.occurredAtMs - b.occurredAtMs);

    const retryBudgetWindows: RetryBudgetWindowSample[] = [];
    let retryBudgetEverExceeded = false;
    let everSawFallbackAbsorbTraffic = false;
    let everSawUnboundedRetryDuringFailure = false;

    if (orderedRequests.length > 0) {
      let windowStartMs = orderedRequests[0]!.occurredAtMs;
      let windowRequests: SimulatedRouteRequestOutcome[] = [];

      const flushWindow = () => {
        if (windowRequests.length === 0) {
          return;
        }
        const originalRequestCount = windowRequests.filter((r) => !r.isRetry).length;
        const retryCount = windowRequests.filter((r) => r.isRetry).length;
        const allowedRetryCount = Math.max(
          config.retryBudget.minRetriesPerSecond * (windowMs / 1000),
          config.retryBudget.retryRatio * originalRequestCount,
        );
        const budgetExceeded = retryCount > allowedRetryCount;
        if (budgetExceeded) {
          retryBudgetEverExceeded = true;
          everSawUnboundedRetryDuringFailure = true;
        }

        const fallbackServed = windowRequests.some(
          (r) => r.servedByBackendService !== config.primaryBackendService && r.succeeded,
        );
        if (fallbackServed) {
          everSawFallbackAbsorbTraffic = true;
        }

        retryBudgetWindows.push({ windowStartMs, originalRequestCount, retryCount, allowedRetryCount, budgetExceeded });
      };

      for (const request of orderedRequests) {
        if (request.occurredAtMs - windowStartMs > windowMs) {
          flushWindow();
          windowStartMs = request.occurredAtMs;
          windowRequests = [];
        }
        windowRequests.push(request);
      }
      flushWindow();
    }

    const primaryFailuresOccurred = orderedRequests.some(
      (r) => r.servedByBackendService === config.primaryBackendService && !r.succeeded,
    );
    const fallbackConfigured = config.trafficSplitBackends.some((b) => b.service !== config.primaryBackendService);

    // Graceful degradation requires: (a) the retry budget was never exceeded (retries stayed
    // bounded rather than storming the already-struggling primary), and (b) if the primary was
    // actually failing, a configured fallback backend picked up traffic instead of every request
    // simply failing outright.
    const degradedGracefully =
      !everSawUnboundedRetryDuringFailure &&
      (!primaryFailuresOccurred || !fallbackConfigured || everSawFallbackAbsorbTraffic);

    return {
      serviceName: config.serviceName,
      retryBudgetEverExceeded,
      retryBudgetWindows,
      degradedGracefully,
      evidence: retryBudgetEverExceeded
        ? `retry budget exceeded in at least one ${config.retryBudget.ttl} window (retryRatio=${config.retryBudget.retryRatio}, ` +
          `minRetriesPerSecond=${config.retryBudget.minRetriesPerSecond}); fallback absorption ${everSawFallbackAbsorbTraffic ? 'observed' : 'NOT observed'}`
        : `retry budget held across ${retryBudgetWindows.length} window(s); ` +
          `${primaryFailuresOccurred ? (everSawFallbackAbsorbTraffic ? 'fallback backend absorbed traffic during primary failures' : 'primary failed but no fallback traffic was observed') : 'no primary failures occurred in the supplied simulated load'}`,
    };
  }
}
