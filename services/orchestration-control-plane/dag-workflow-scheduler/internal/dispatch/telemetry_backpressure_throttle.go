// telemetry_backpressure_throttle.go closes the upstream backpressure
// feedback loop described in ARCHITECTURE.md's telemetry-lake section: when
// `redpanda-ingestion-pipeline`'s BackpressureAwareProducer trips its
// circuit breaker (telemetry-lake ingestion -- Redpanda/ClickHouse -- is
// falling behind), it publishes a TelemetryBackpressureSignal onto the
// `telemetry.backpressure.v1` topic (see
// services/telemetry-lake/redpanda-ingestion-pipeline/src/topic-definitions.ts
// and src/backpressure-signal-publisher.ts). Without a consumer on the
// scheduler side, that signal only ever protects the telemetry lake in
// isolation: dag-workflow-scheduler would happily keep dispatching new
// LOAD_STEP nodes that generate proportionally more telemetry, digging the
// ingestion backlog deeper. This file is that consumer.
package dispatch

import (
	"context"
	"sync"
	"time"
)

// TelemetryBackpressureLevel mirrors the CircuitBreakerState values
// BackpressureAwareProducer can be in (see backpressure-aware-producer.ts),
// as carried by TelemetryBackpressureSignal.circuitState on the wire. Kept
// as a distinct Go type (rather than importing a generated TS enum, which
// this repo has no codegen step for yet) so this package has no compile-time
// dependency on the TypeScript side -- only on the JSON shape agreed in
// topic-definitions.ts.
type TelemetryBackpressureLevel string

const (
	// TelemetryBackpressureLevelClosed means the telemetry lake ingestion
	// path is healthy; dispatch should proceed at full rate.
	TelemetryBackpressureLevelClosed TelemetryBackpressureLevel = "CLOSED"
	// TelemetryBackpressureLevelHalfOpen means the producer-side circuit is
	// cautiously probing recovery; dispatch should stay throttled until it
	// observes CLOSED.
	TelemetryBackpressureLevelHalfOpen TelemetryBackpressureLevel = "HALF_OPEN"
	// TelemetryBackpressureLevelOpen means the telemetry lake is shedding
	// load; dispatch should pause issuing new nodes that would generate
	// further telemetry.
	TelemetryBackpressureLevelOpen TelemetryBackpressureLevel = "OPEN"
)

// TelemetryBackpressureSignal is the Go-side mirror of the
// `TelemetryBackpressureSignal` payload published to `telemetry.backpressure.v1`
// by redpanda-ingestion-pipeline's BackpressureSignalPublisher. Field names
// and JSON tags MUST stay in sync with that TypeScript interface -- see the
// note atop topic-definitions.ts about this pipeline's contracts being
// documentation-as-code rather than protobuf-generated today.
type TelemetryBackpressureSignal struct {
	SignalID                string                     `json:"signalId"`
	SourceService           string                     `json:"sourceService"`
	CircuitState            TelemetryBackpressureLevel `json:"circuitState"`
	QueuedMessageCount      int                        `json:"queuedMessageCount"`
	ConsecutiveFailureCount int                        `json:"consecutiveFailureCount"`
	// RecommendedThrottleMs is advisory: how long the publisher expects its
	// own circuit to stay OPEN (its openStateCooldownMs at signal time), so
	// a consumer can choose to self-clear its throttle after roughly this
	// long even if it missed the follow-up CLOSED signal.
	RecommendedThrottleMs int64  `json:"recommendedThrottleMs"`
	OccurredAtUnixNano    string `json:"occurredAtUnixNano"`
}

// PipelineDispatchThrottlePort is the interface the scheduler's dispatch
// loop (cmd/scheduler/main.go's worker wiring, or a future dedicated
// dispatch-loop type) consults immediately before starting a new pipeline
// run or dispatching a new node, so backpressure actually slows down
// dispatch instead of only ever being visible on the telemetry-lake side.
// Kept as an interface -- rather than a concrete consumer -- so workflow/
// activity code can depend on it without a hard dependency on however the
// signal is actually transported (Redpanda consumer group today; could be a
// Redis pub/sub relay or a Temporal Signal in the future without callers
// changing).
type PipelineDispatchThrottlePort interface {
	// ShouldThrottleDispatch reports whether new dispatch should currently
	// be paused because the telemetry lake most recently reported itself
	// under backpressure, along with the level that triggered the decision
	// (useful for logging/metrics at the call site).
	ShouldThrottleDispatch(ctx context.Context) (throttle bool, level TelemetryBackpressureLevel)
}

// TelemetryBackpressureThrottle is the production
// PipelineDispatchThrottlePort implementation. A Redpanda consumer for
// `telemetry.backpressure.v1` (wired in cmd/scheduler/main.go, not included
// in this starter) calls OnSignalReceived for every TelemetryBackpressureSignal
// it consumes; ShouldThrottleDispatch reads the latest state that call
// recorded. Recording is also self-expiring via RecommendedThrottleMs so a
// missed/dropped CLOSED signal (e.g. the scheduler pod restarted and its
// consumer group re-balanced mid-window) cannot wedge dispatch throttled
// forever.
type TelemetryBackpressureThrottle struct {
	mu            sync.RWMutex
	currentLevel  TelemetryBackpressureLevel
	throttleUntil time.Time
	now           func() time.Time
}

// NewTelemetryBackpressureThrottle constructs a throttle that starts CLOSED
// (i.e. dispatch is not throttled) until the first backpressure signal
// arrives.
func NewTelemetryBackpressureThrottle() *TelemetryBackpressureThrottle {
	return &TelemetryBackpressureThrottle{
		currentLevel: TelemetryBackpressureLevelClosed,
		now:          time.Now,
	}
}

// OnSignalReceived updates the throttle's view of telemetry-lake health from
// a freshly consumed TelemetryBackpressureSignal. Called from the
// scheduler's telemetry.backpressure.v1 consumer loop (see this package's
// README for the wiring this stubs out).
func (t *TelemetryBackpressureThrottle) OnSignalReceived(signal TelemetryBackpressureSignal) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.currentLevel = signal.CircuitState
	if signal.CircuitState == TelemetryBackpressureLevelClosed {
		t.throttleUntil = time.Time{}
		return
	}

	throttleDuration := time.Duration(signal.RecommendedThrottleMs) * time.Millisecond
	t.throttleUntil = t.now().Add(throttleDuration)
}

// ShouldThrottleDispatch implements PipelineDispatchThrottlePort. Dispatch
// is throttled whenever the most recently observed level is not CLOSED and
// the advisory throttle window (RecommendedThrottleMs from the signal that
// set it) has not yet elapsed. Letting the window lapse even without an
// explicit CLOSED follow-up signal means a dropped/missed message on
// `telemetry.backpressure.v1` degrades to "eventually resumes dispatch"
// rather than "stuck throttled until a human intervenes".
func (t *TelemetryBackpressureThrottle) ShouldThrottleDispatch(_ context.Context) (bool, TelemetryBackpressureLevel) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.currentLevel == TelemetryBackpressureLevelClosed {
		return false, t.currentLevel
	}
	if t.throttleUntil.IsZero() || t.now().After(t.throttleUntil) {
		return false, t.currentLevel
	}
	return true, t.currentLevel
}
