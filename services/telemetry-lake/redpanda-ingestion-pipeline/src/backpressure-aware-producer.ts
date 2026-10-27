/**
 * backpressure-aware-producer.ts
 *
 * Wraps a kafkajs-style Kafka producer with a circuit breaker so that a traffic spike (the
 * platform is designed to be able to generate up to 10M RPS of load-test traffic, which fans
 * out into a proportional telemetry volume) sheds load at the producer boundary instead of
 * letting an unbounded in-memory send queue collapse the process, or hammering ClickHouse's
 * Kafka-engine consumer group faster than it can materialize rows.
 *
 * Design goals:
 *  - Never buffer unboundedly. A bounded queue + circuit breaker means "shed", not "crash".
 *  - Fail fast and cheaply once open, so upstream callers (OTel Collector exporters, worker
 *    telemetry emitters) can apply their own local sampling/drop decisions instead of blocking.
 *  - Recover automatically via a half-open probe once Redpanda/ClickHouse ingestion catches up.
 */

/** Minimal shape of the kafkajs Producer API this module depends on, kept narrow so tests can
 * provide a fake without pulling in the real kafkajs dependency. */
export interface KafkaJsLikeProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: {
    topic: string;
    messages: Array<{ key: string; value: string; headers?: Record<string, string> }>;
  }): Promise<unknown>;
}

export type CircuitBreakerState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface BackpressureAwareProducerOptions {
  /** Consecutive send failures (or timeouts) before the breaker trips OPEN. */
  failureThreshold: number;
  /** How long the breaker stays OPEN before allowing a single HALF_OPEN probe send. */
  openStateCooldownMs: number;
  /** Successful sends required while HALF_OPEN before returning to CLOSED. */
  halfOpenSuccessesToClose: number;
  /** Hard cap on in-flight/queued messages; once exceeded, new sends are shed immediately
   * even if the breaker is CLOSED, protecting the process's own memory. */
  maxQueuedMessages: number;
  /** Per-send timeout; a send that exceeds this counts as a failure toward the threshold. */
  sendTimeoutMs: number;
  /** Optional hook invoked synchronously whenever the circuit breaker transitions state
   * (CLOSED -> OPEN, OPEN -> HALF_OPEN, HALF_OPEN -> CLOSED, or HALF_OPEN -> OPEN on a failed
   * probe). Primary consumer: `BackpressureSignalPublisher`
   * (backpressure-signal-publisher.ts), which uses this to publish a
   * `telemetry.backpressure.v1` signal so `dag-workflow-scheduler` can throttle dispatch —
   * see that module's doc comment for the full feedback-loop rationale. Left optional (and
   * defaulted to a no-op) so existing callers of this class are entirely unaffected. */
  onCircuitStateChange?: (previousState: CircuitBreakerState, nextState: CircuitBreakerState) => void;
}

export const DEFAULT_BACKPRESSURE_OPTIONS: BackpressureAwareProducerOptions = {
  failureThreshold: 5,
  openStateCooldownMs: 10_000,
  halfOpenSuccessesToClose: 3,
  maxQueuedMessages: 50_000,
  sendTimeoutMs: 2_000,
};

export interface ProducerRecord {
  topic: string;
  /** Partition key — callers must follow the strategy documented in topic-definitions.ts. */
  key: string;
  value: unknown;
  headers?: Record<string, string>;
}

/** Outcome returned to the caller so ingestion-side callers (e.g. the OTel Collector's own
 * shim, or a worker's direct telemetry emitter) can make a local decision — retry later,
 * downgrade to local sampling, or drop — rather than the producer throwing and taking down
 * the caller's hot path. */
export type SendOutcome =
  | { status: "SENT" }
  | { status: "SHED_QUEUE_FULL" }
  | { status: "SHED_CIRCUIT_OPEN" }
  | { status: "FAILED"; error: Error };

/**
 * BackpressureAwareProducer
 *
 * Circuit-breaker + bounded-queue wrapper around a kafkajs-style producer. Ingestion callers
 * should treat every `send()` result as advisory — under a real 10M-RPS spike, shedding some
 * telemetry is the correct behavior, and is far preferable to an unbounded queue eventually
 * OOMing the collector/producer process or a saturated ClickHouse consumer group falling
 * arbitrarily far behind real time.
 */
export class BackpressureAwareProducer {
  private readonly options: BackpressureAwareProducerOptions;
  private state: CircuitBreakerState = "CLOSED";
  private consecutiveFailures = 0;
  private halfOpenSuccesses = 0;
  private openedAtMs: number | null = null;
  private queuedCount = 0;
  private connected = false;

  constructor(
    private readonly producer: KafkaJsLikeProducer,
    options: Partial<BackpressureAwareProducerOptions> = {},
  ) {
    this.options = { ...DEFAULT_BACKPRESSURE_OPTIONS, ...options };
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    await this.producer.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return;
    await this.producer.disconnect();
    this.connected = false;
  }

  getCircuitState(): CircuitBreakerState {
    this.maybeTransitionFromOpenToHalfOpen();
    return this.state;
  }

  /** Current in-flight/queued message count. Exposed (read-only) primarily for
   * `BackpressureSignalPublisher` to snapshot into a `TelemetryBackpressureSignal` at the
   * moment the circuit trips, so the scheduler-side consumer gets a magnitude alongside the
   * OPEN/CLOSED boolean. */
  getQueuedMessageCount(): number {
    return this.queuedCount;
  }

  /** Current consecutive-failure count toward `failureThreshold`. Same rationale as
   * `getQueuedMessageCount()`. */
  getConsecutiveFailureCount(): number {
    return this.consecutiveFailures;
  }

  /** The configured cooldown a trip stays OPEN for, so callers observing a state-change signal
   * can compute an advisory "how long should a downstream consumer expect this to last"
   * without reaching into `options` directly. */
  getOpenStateCooldownMs(): number {
    return this.options.openStateCooldownMs;
  }

  /**
   * Attempt to send one message. Never throws — backpressure and failure are communicated via
   * the returned SendOutcome so hot-path callers never pay exception-handling overhead.
   */
  async send(record: ProducerRecord): Promise<SendOutcome> {
    this.maybeTransitionFromOpenToHalfOpen();

    if (this.state === "OPEN") {
      return { status: "SHED_CIRCUIT_OPEN" };
    }

    if (this.queuedCount >= this.options.maxQueuedMessages) {
      // Queue-full shedding is independent of circuit-breaker state: even a healthy downstream
      // shouldn't let this process accumulate unbounded in-flight sends.
      return { status: "SHED_QUEUE_FULL" };
    }

    this.queuedCount += 1;
    try {
      await this.withTimeout(
        this.producer.send({
          topic: record.topic,
          messages: [
            {
              key: record.key,
              value: JSON.stringify(record.value),
              headers: record.headers,
            },
          ],
        }),
        this.options.sendTimeoutMs,
      );
      this.onSendSuccess();
      return { status: "SENT" };
    } catch (error) {
      this.onSendFailure();
      return { status: "FAILED", error: error instanceof Error ? error : new Error(String(error)) };
    } finally {
      this.queuedCount -= 1;
    }
  }

  /** Batched variant — sends one kafkajs record per call but groups payloads, reducing
   * per-message network round trips during sustained high-throughput ingestion. Still subject
   * to the same queue-full / circuit-open shedding as send(). */
  async sendBatch(topic: string, records: Array<Pick<ProducerRecord, "key" | "value" | "headers">>): Promise<SendOutcome> {
    this.maybeTransitionFromOpenToHalfOpen();

    if (this.state === "OPEN") {
      return { status: "SHED_CIRCUIT_OPEN" };
    }
    if (this.queuedCount + records.length > this.options.maxQueuedMessages) {
      return { status: "SHED_QUEUE_FULL" };
    }

    this.queuedCount += records.length;
    try {
      await this.withTimeout(
        this.producer.send({
          topic,
          messages: records.map((r) => ({
            key: r.key,
            value: JSON.stringify(r.value),
            headers: r.headers,
          })),
        }),
        this.options.sendTimeoutMs,
      );
      this.onSendSuccess();
      return { status: "SENT" };
    } catch (error) {
      this.onSendFailure();
      return { status: "FAILED", error: error instanceof Error ? error : new Error(String(error)) };
    } finally {
      this.queuedCount -= records.length;
    }
  }

  private onSendSuccess(): void {
    if (this.state === "HALF_OPEN") {
      this.halfOpenSuccesses += 1;
      if (this.halfOpenSuccesses >= this.options.halfOpenSuccessesToClose) {
        this.closeCircuit();
      }
      return;
    }
    this.consecutiveFailures = 0;
  }

  private onSendFailure(): void {
    if (this.state === "HALF_OPEN") {
      // A single failed probe during HALF_OPEN re-opens the breaker immediately.
      this.openCircuit();
      return;
    }
    this.consecutiveFailures += 1;
    if (this.consecutiveFailures >= this.options.failureThreshold) {
      this.openCircuit();
    }
  }

  private openCircuit(): void {
    this.transitionTo("OPEN", () => {
      this.openedAtMs = Date.now();
      this.halfOpenSuccesses = 0;
    });
  }

  private closeCircuit(): void {
    this.transitionTo("CLOSED", () => {
      this.consecutiveFailures = 0;
      this.halfOpenSuccesses = 0;
      this.openedAtMs = null;
    });
  }

  private maybeTransitionFromOpenToHalfOpen(): void {
    if (this.state !== "OPEN" || this.openedAtMs === null) return;
    const elapsed = Date.now() - this.openedAtMs;
    if (elapsed >= this.options.openStateCooldownMs) {
      this.transitionTo("HALF_OPEN", () => {
        this.halfOpenSuccesses = 0;
      });
    }
  }

  /** Applies a circuit-state transition's field mutations, then fires
   * `onCircuitStateChange` (if configured) with the before/after states. Centralizing the
   * transition here — rather than setting `this.state` directly at each call site — is what
   * lets `BackpressureSignalPublisher` observe every transition without this class needing to
   * know anything about telemetry.backpressure.v1 or Redpanda. */
  private transitionTo(nextState: CircuitBreakerState, applyFieldMutations: () => void): void {
    const previousState = this.state;
    this.state = nextState;
    applyFieldMutations();
    if (previousState !== nextState) {
      this.options.onCircuitStateChange?.(previousState, nextState);
    }
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`kafka send exceeded ${timeoutMs}ms timeout`)), timeoutMs);
    });
    try {
      return await Promise.race([promise, timeout]);
    } finally {
      clearTimeout(timer!);
    }
  }
}
