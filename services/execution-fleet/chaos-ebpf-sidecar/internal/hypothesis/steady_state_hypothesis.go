// Package hypothesis implements steady-state-hypothesis-guarded chaos experiments: verifying a
// measurable "the system looks healthy" statement before injecting a fault, continuing to poll
// it while the fault is active, and automatically aborting and rolling back the fault the
// moment that statement stops holding.
//
// This is the auto-abort safety layer referenced in
// docs/architecture-decision-records/0007-hypothesis-guarded-chaos-and-global-kill-switch.md,
// and it is deliberately independent of internal/safety's GlobalChaosCircuitBreaker: a
// hypothesis violation means *this specific experiment's own target* looks unhealthy, whereas
// the global circuit breaker means an operator decided the whole sidecar should stop for a
// reason unrelated to any one experiment's telemetry. Both can fire independently, and either
// one aborting an experiment is a safety mechanism working correctly, not a bug.
package hypothesis

import (
	"context"
	"fmt"
)

// ComparisonOperator names how a SteadyStateHypothesis compares an observed metric value
// against its Threshold. Kept as a small closed set (rather than an arbitrary expression
// language) so a hypothesis can be described declaratively — e.g. sourced from a DAG node's
// dispatch-payload config — without embedding a scripting engine in this package.
type ComparisonOperator string

const (
	// ComparisonLessThan holds when the observed value is strictly less than Threshold, e.g.
	// "checkout-service p99 stays under 300ms".
	ComparisonLessThan ComparisonOperator = "<"
	// ComparisonLessThanOrEqual holds when the observed value is <= Threshold.
	ComparisonLessThanOrEqual ComparisonOperator = "<="
	// ComparisonGreaterThan holds when the observed value is strictly greater than Threshold,
	// e.g. "checkout-service success rate stays above 99.9%".
	ComparisonGreaterThan ComparisonOperator = ">"
	// ComparisonGreaterThanOrEqual holds when the observed value is >= Threshold.
	ComparisonGreaterThanOrEqual ComparisonOperator = ">="
)

func (op ComparisonOperator) validate() error {
	switch op {
	case ComparisonLessThan, ComparisonLessThanOrEqual, ComparisonGreaterThan, ComparisonGreaterThanOrEqual:
		return nil
	default:
		return fmt.Errorf("comparison operator: unknown operator %q", op)
	}
}

func (op ComparisonOperator) holds(observed, threshold float64) bool {
	switch op {
	case ComparisonLessThan:
		return observed < threshold
	case ComparisonLessThanOrEqual:
		return observed <= threshold
	case ComparisonGreaterThan:
		return observed > threshold
	case ComparisonGreaterThanOrEqual:
		return observed >= threshold
	default:
		// Unreachable if Validate() was called first; fail closed (hypothesis does not hold)
		// rather than silently treating an unrecognized operator as "true", matching this
		// package's overall bias toward aborting when it cannot be sure it's safe to continue.
		return false
	}
}

// SteadyStateHypothesis is a single, measurable "the system is healthy" statement backed by one
// telemetry query — e.g. Name: "checkout-service p99 stays under 300ms",
// MetricQuery: `p99_latency_ms{service="checkout-service"}`, Comparison: ComparisonLessThan,
// Threshold: 300. HypothesisGuardedExperimentRunner evaluates this both before injecting a
// fault (steady state must already hold) and repeatedly during injection (steady state must
// keep holding), matching the "steady-state hypothesis" vocabulary from chaos-engineering
// practice (verify normal behavior, perturb, verify it still holds).
type SteadyStateHypothesis struct {
	// Name is a human-readable label for logs, telemetry, and experiment reports.
	Name string
	// MetricQuery is opaque to this package — it is passed as-is to a MetricQueryPort
	// implementation, which is the only thing that needs to understand its syntax (e.g. a
	// telemetry-query-api query string identifying a service + percentile + window).
	MetricQuery string
	// Threshold is the value Comparison checks the observed metric against.
	Threshold float64
	// Comparison names how the observed value must relate to Threshold for the hypothesis to
	// hold.
	Comparison ComparisonOperator
}

// Validate checks that h describes a well-formed, evaluable hypothesis.
func (h SteadyStateHypothesis) Validate() error {
	if h.Name == "" {
		return fmt.Errorf("steady state hypothesis: Name is required")
	}
	if h.MetricQuery == "" {
		return fmt.Errorf("steady state hypothesis %q: MetricQuery is required", h.Name)
	}
	if err := h.Comparison.validate(); err != nil {
		return fmt.Errorf("steady state hypothesis %q: %w", h.Name, err)
	}
	return nil
}

// Holds reports whether observedValue satisfies this hypothesis's Comparison against Threshold.
func (h SteadyStateHypothesis) Holds(observedValue float64) bool {
	return h.Comparison.holds(observedValue, h.Threshold)
}

// MetricQueryPort is the seam between this package's hypothesis evaluation and the actual
// telemetry backend a MetricQuery is evaluated against. Decoupling via this interface means
// HypothesisGuardedExperimentRunner never depends on telemetry-query-api's HTTP contract
// directly — see HTTPTelemetryQueryAPIMetricQueryPort in
// hypothesis_guarded_experiment_runner.go for the stub adapter and the intended real wiring.
type MetricQueryPort interface {
	// QueryMetric evaluates metricQuery against current telemetry and returns its scalar value.
	QueryMetric(ctx context.Context, metricQuery string) (float64, error)
}
