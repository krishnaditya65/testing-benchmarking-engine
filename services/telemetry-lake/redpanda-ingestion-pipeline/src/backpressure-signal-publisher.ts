/**
 * backpressure-signal-publisher.ts
 *
 * Closes the upstream half of the backpressure feedback loop described in this package's
 * README: `BackpressureAwareProducer` (backpressure-aware-producer.ts) already sheds load
 * *locally* once its circuit breaker trips, protecting this process and the downstream
 * Redpanda/ClickHouse ingestion path. On its own, though, that only ever protects the
 * telemetry lake in isolation — `dag-workflow-scheduler` has no way to know ingestion is under
 * pressure, so it keeps dispatching new LOAD_STEP nodes that generate proportionally more
 * telemetry, digging the backlog deeper right as the lake needs relief.
 *
 * `BackpressureSignalPublisher` observes a `BackpressureAwareProducer`'s circuit-breaker state
 * transitions (via `BackpressureAwareProducerOptions.onCircuitStateChange`) and publishes a
 * `TelemetryBackpressureSignal` onto `telemetry.backpressure.v1` for each one. On the
 * dag-workflow-scheduler side, `internal/dispatch/telemetry_backpressure_throttle.go`'s
 * `TelemetryBackpressureThrottle` consumes these signals and the scheduler's dispatch loop
 * consults it before dispatching new nodes — see that file's doc comment for the consumer half
 * of this loop, and both services' READMEs for the full signal path:
 *
 *     BackpressureAwareProducer trips its breaker
 *       -> BackpressureSignalPublisher publishes telemetry.backpressure.v1
 *         -> TelemetryBackpressureThrottle (dag-workflow-scheduler) observes the signal
 *           -> the scheduler's dispatch loop pauses/slows new node dispatch until CLOSED
 *
 * Deliberately published on a *separate* producer from the one being observed
 * (`signalProducer`, not `BackpressureAwareProducer` itself): the whole point of this signal is
 * to fire exactly when the telemetry lake's own ingestion path is degraded, so routing the
 * signal through the same backpressure-shedding path it describes would risk the signal itself
 * being shed at precisely the moment it matters most. Signal volume is inherently tiny (one
 * message per circuit-breaker state transition, not per telemetry event), so `signalProducer`
 * does not need — and intentionally does not get — its own circuit-breaker wrapper.
 */
import { randomUUID } from "node:crypto";
import type { BackpressureAwareProducer, CircuitBreakerState, KafkaJsLikeProducer } from "./backpressure-aware-producer";
import { RedpandaTopic, type TelemetryBackpressureSignal } from "./topic-definitions";

export interface BackpressureSignalPublisherOptions {
  /** Identifies this process as the signal's origin (see
   * TelemetryBackpressureSignal.sourceService); e.g. "redpanda-ingestion-pipeline" or, if this
   * package is embedded directly into another producing service, that service's own name. */
  sourceService: string;
}

/**
 * BackpressureSignalPublisher
 *
 * Publishes a `TelemetryBackpressureSignal` to `telemetry.backpressure.v1` for every
 * circuit-breaker state transition of an observed `BackpressureAwareProducer`. Construct one,
 * then pass `forObservedProducer(...)`'s return value as `onCircuitStateChange` when
 * constructing the `BackpressureAwareProducer` you want it to watch:
 *
 * ```ts
 * const signalPublisher = new BackpressureSignalPublisher(signalKafkaProducer, {
 *   sourceService: "redpanda-ingestion-pipeline",
 * });
 * await signalPublisher.connect();
 *
 * let observedProducer!: BackpressureAwareProducer;
 * observedProducer = new BackpressureAwareProducer(rawKafkaProducer, {
 *   onCircuitStateChange: (previousState, nextState) =>
 *     signalPublisher.publishStateChange(observedProducer, previousState, nextState),
 * });
 * ```
 */
export class BackpressureSignalPublisher {
  private connected = false;

  constructor(
    private readonly signalProducer: KafkaJsLikeProducer,
    private readonly options: BackpressureSignalPublisherOptions,
  ) {}

  async connect(): Promise<void> {
    if (this.connected) return;
    await this.signalProducer.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return;
    await this.signalProducer.disconnect();
    this.connected = false;
  }

  /**
   * Publishes one TelemetryBackpressureSignal describing `observedProducer`'s transition from
   * `previousState` to `nextState`. Intended to be called from the `onCircuitStateChange` hook
   * of the `BackpressureAwareProducer` being observed (see class doc comment for the wiring).
   *
   * Never throws: a failure to publish the *signal* must not itself become a new source of
   * cascading failure on top of the backpressure the signal is trying to report. Failures are
   * swallowed after being surfaced via the returned promise's rejection only if a caller
   * chooses to await and handle it; fire-and-forget callers (the typical case, since
   * `onCircuitStateChange` is synchronous) simply never see them.
   */
  async publishStateChange(
    observedProducer: BackpressureAwareProducer,
    previousState: CircuitBreakerState,
    nextState: CircuitBreakerState,
  ): Promise<void> {
    const signal: TelemetryBackpressureSignal = {
      signalId: randomUUID(),
      sourceService: this.options.sourceService,
      circuitState: nextState,
      queuedMessageCount: observedProducer.getQueuedMessageCount(),
      consecutiveFailureCount: observedProducer.getConsecutiveFailureCount(),
      // Only OPEN/HALF_OPEN transitions carry a meaningful "expect relief around this long"
      // window; a transition back to CLOSED has nothing left to throttle for.
      recommendedThrottleMs: nextState === "CLOSED" ? 0 : observedProducer.getOpenStateCooldownMs(),
      occurredAtUnixNano: (BigInt(Date.now()) * 1_000_000n).toString(),
    };

    try {
      await this.signalProducer.send({
        topic: RedpandaTopic.TELEMETRY_BACKPRESSURE_V1,
        messages: [
          {
            // Keyed by sourceService per topic-definitions.ts's partition-key strategy, so one
            // producer's own sequence of transitions stays ordered on a single partition.
            key: this.options.sourceService,
            value: JSON.stringify(signal),
            headers: { "circuit-state": nextState, "previous-circuit-state": previousState },
          },
        ],
      });
    } catch (error) {
      // Intentionally re-thrown rather than logged-and-swallowed here: this class has no
      // opinion on this process's logging setup (see structured-logger-factory in
      // shared-libraries/observability-kit for the platform standard), so the caller — which
      // does — decides whether to log, retry, or ignore. What it must not do is let a fire-and-
      // forget call from a synchronous onCircuitStateChange hook produce an unhandled
      // rejection; callers following the wiring in this class's doc comment should attach a
      // `.catch(...)`.
      throw error instanceof Error ? error : new Error(String(error));
    }
  }
}
