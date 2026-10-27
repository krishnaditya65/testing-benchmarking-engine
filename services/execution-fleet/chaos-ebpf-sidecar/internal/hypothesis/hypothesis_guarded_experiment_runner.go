// hypothesis_guarded_experiment_runner.go wraps any chaos.* fault-injection primitive so it is
// only ever run under a verified steady-state hypothesis: verify before injecting, poll during
// injection, auto-abort-and-rollback the instant the hypothesis is violated.
package hypothesis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/chaos"
)

// FaultInjection is the minimal shape HypothesisGuardedExperimentRunner needs from any chaos
// primitive: a way to start the fault and a way to undo it. Every existing injector in
// internal/chaos has a different method shape (Apply/Revert, BeginSkew/EndSkew,
// KillMatching[WithStrategy]/not-revertible) because each fault has different semantics; the
// adapter types below translate each one into this common interface so the runner stays
// decoupled from any specific injector's API shape — "wraps any of the existing chaos
// injectors" per this package's purpose.
type FaultInjection interface {
	// Inject begins the fault. Called at most once per RunGuarded call, after the
	// pre-injection steady-state check has already passed.
	Inject(ctx context.Context) error
	// Rollback undoes the fault. Must be safe to call even if the fault was never fully
	// applied, and safe to call more than once (idempotent) — both the auto-abort path and
	// normal experiment completion call it.
	Rollback(ctx context.Context) error
	// Description is a short human-readable summary of what this fault injection does, used in
	// error messages and logs.
	Description() string
}

// NetworkDegradationFaultInjection adapts chaos.NetworkDegradationInjector.Apply/Revert to the
// FaultInjection interface.
type NetworkDegradationFaultInjection struct {
	Injector *chaos.NetworkDegradationInjector
	Target   chaos.NetworkTarget
	Profile  chaos.DegradationProfile
}

func (f NetworkDegradationFaultInjection) Inject(ctx context.Context) error {
	return f.Injector.Apply(ctx, f.Target, f.Profile)
}

func (f NetworkDegradationFaultInjection) Rollback(ctx context.Context) error {
	return f.Injector.Revert(ctx, f.Target)
}

func (f NetworkDegradationFaultInjection) Description() string {
	return fmt.Sprintf("network degradation on %s/%s/%s", f.Target.NodeName, f.Target.PodUID, f.Target.NetworkInterfaceName)
}

// ClockSkewFaultInjection adapts chaos.NodeClockSkewInjector.BeginSkew/EndSkew to the
// FaultInjection interface.
type ClockSkewFaultInjection struct {
	Injector *chaos.NodeClockSkewInjector
	Target   chaos.NetworkTarget
	Spec     chaos.SkewSpec
}

func (f ClockSkewFaultInjection) Inject(ctx context.Context) error {
	return f.Injector.BeginSkew(ctx, f.Target, f.Spec)
}

func (f ClockSkewFaultInjection) Rollback(ctx context.Context) error {
	return f.Injector.EndSkew(ctx, f.Target)
}

func (f ClockSkewFaultInjection) Description() string {
	return fmt.Sprintf("clock skew on %s/%s (target offset %s)", f.Target.NodeName, f.Target.PodUID, f.Spec.OffsetDuration)
}

// PodKillFaultInjection adapts chaos.BlastRadiusControlledPodKiller.KillMatchingWithStrategy to
// the FaultInjection interface. Rollback is intentionally a documented no-op: a killed pod
// cannot be un-killed (see chaos-ebpf-sidecar/README.md's "Graceful revert on shutdown"
// section, which draws the same distinction for the shutdown path). Guarding pod kills with a
// steady-state hypothesis is still valuable even though there is nothing to roll back: the
// pre-injection check can refuse to start the kill at all if the system is already unhealthy,
// and RunGuarded's during-injection poll still gives the caller an immediate, labeled abort
// signal if the hypothesis breaks — the actionable response at that point is "stop killing more
// pods", not "un-kill the ones already gone".
type PodKillFaultInjection struct {
	Killer        *chaos.BlastRadiusControlledPodKiller
	AllCandidates []chaos.PodRef
	Strategy      chaos.PodSelectionStrategy
}

func (f PodKillFaultInjection) Inject(ctx context.Context) error {
	return f.Killer.KillMatchingWithStrategy(ctx, f.AllCandidates, f.Strategy)
}

func (f PodKillFaultInjection) Rollback(ctx context.Context) error {
	// See type doc comment: pod kills are not undoable. Nothing to do.
	return nil
}

func (f PodKillFaultInjection) Description() string {
	return fmt.Sprintf("blast-radius-controlled pod kill across %d candidate pod(s)", len(f.AllCandidates))
}

// GuardedExperimentResult reports how a HypothesisGuardedExperimentRunner.RunGuarded call ended.
type GuardedExperimentResult struct {
	// Aborted is true whenever RunGuarded stopped the fault early — a hypothesis violation, an
	// unevaluable hypothesis, or caller-context cancellation. It is also true for the
	// "experiment ran its full duration cleanly" case is false; see AbortReason.
	Aborted bool
	// AbortReason is a human-readable explanation, set whenever Aborted is true.
	AbortReason string
	// ViolatingObservedValue is the metric value that violated the hypothesis, set only when
	// AbortReason describes a hypothesis violation (not a context-cancellation or query-error
	// abort).
	ViolatingObservedValue float64
	StartedAt              time.Time
	EndedAt                time.Time
}

// HypothesisGuardedExperimentRunner wraps a FaultInjection with steady-state verification:
// before injecting, during injection (polled every pollInterval), and always reverts the fault
// on the way out — whether that is because the hypothesis was violated (auto-abort) or because
// the experiment ran its full requested duration cleanly. Owning the full inject-poll-rollback
// lifecycle here (rather than leaving rollback to the caller) is what makes "auto-abort" an
// actual safety guarantee instead of a signal the caller could ignore.
type HypothesisGuardedExperimentRunner struct {
	metrics      MetricQueryPort
	pollInterval time.Duration
}

// NewHypothesisGuardedExperimentRunner constructs a runner. pollInterval must be positive — a
// runner that never polls during injection cannot auto-abort, which defeats this type's entire
// purpose.
func NewHypothesisGuardedExperimentRunner(metrics MetricQueryPort, pollInterval time.Duration) (*HypothesisGuardedExperimentRunner, error) {
	if metrics == nil {
		return nil, fmt.Errorf("new hypothesis guarded experiment runner: MetricQueryPort must not be nil")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("new hypothesis guarded experiment runner: pollInterval must be positive, got %s", pollInterval)
	}
	return &HypothesisGuardedExperimentRunner{metrics: metrics, pollInterval: pollInterval}, nil
}

// RunGuarded verifies hyp holds, injects injection, polls hyp every pollInterval for up to
// experimentDuration, and always calls injection.Rollback before returning — either because the
// hypothesis was violated (auto-abort) or because experimentDuration elapsed cleanly. It
// returns a non-nil error only for setup failures (steady state already violated before
// injection, or injection itself failing to start); a hypothesis violation *during* injection is
// reported via the returned GuardedExperimentResult, not an error, since auto-abort-and-rollback
// succeeding is this type doing its job correctly, not a failure of the call.
func (r *HypothesisGuardedExperimentRunner) RunGuarded(
	ctx context.Context,
	hyp SteadyStateHypothesis,
	injection FaultInjection,
	experimentDuration time.Duration,
) (*GuardedExperimentResult, error) {
	if err := hyp.Validate(); err != nil {
		return nil, fmt.Errorf("run guarded experiment: %w", err)
	}
	if experimentDuration <= 0 {
		return nil, fmt.Errorf("run guarded experiment %q: experimentDuration must be positive, got %s", hyp.Name, experimentDuration)
	}

	startedAt := time.Now()

	// (a) Verify steady-state holds BEFORE injecting.
	preValue, err := r.metrics.QueryMetric(ctx, hyp.MetricQuery)
	if err != nil {
		return nil, fmt.Errorf("run guarded experiment %q: failed to evaluate pre-injection steady state: %w", hyp.Name, err)
	}
	if !hyp.Holds(preValue) {
		return nil, fmt.Errorf(
			"run guarded experiment %q: refusing to inject %s; steady-state hypothesis is already violated before injection (observed %v, want %s %v)",
			hyp.Name, injection.Description(), preValue, hyp.Comparison, hyp.Threshold,
		)
	}

	if err := injection.Inject(ctx); err != nil {
		return nil, fmt.Errorf("run guarded experiment %q: failed to start %s: %w", hyp.Name, injection.Description(), err)
	}

	// (b) Poll DURING injection; (c) auto-abort-and-rollback the moment the hypothesis is
	// violated.
	result := &GuardedExperimentResult{StartedAt: startedAt}
	deadline := startedAt.Add(experimentDuration)
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			result.Aborted = true
			result.AbortReason = fmt.Sprintf("caller context canceled: %v", ctx.Err())
			r.rollbackBestEffort(injection)
			result.EndedAt = time.Now()
			return result, nil

		case now := <-ticker.C:
			observed, err := r.metrics.QueryMetric(ctx, hyp.MetricQuery)
			if err != nil {
				// Fail-safe: an unevaluable hypothesis during an active fault is treated the
				// same as a violated one — this package would rather abort a healthy experiment
				// than continue running a fault it can no longer verify is safe.
				result.Aborted = true
				result.AbortReason = fmt.Sprintf("could not evaluate steady-state hypothesis during injection: %v", err)
				r.rollbackBestEffort(injection)
				result.EndedAt = time.Now()
				return result, nil
			}
			if !hyp.Holds(observed) {
				result.Aborted = true
				result.AbortReason = "steady-state hypothesis violated during injection"
				result.ViolatingObservedValue = observed
				r.rollbackBestEffort(injection)
				result.EndedAt = time.Now()
				return result, nil
			}
			if !now.Before(deadline) {
				// Experiment ran its full requested duration without a violation. Still rolls
				// back — RunGuarded owns the full lifecycle end to end, so a caller never has to
				// remember to separately revert a successfully-completed guarded experiment.
				r.rollbackBestEffort(injection)
				result.EndedAt = time.Now()
				return result, nil
			}
		}
	}
}

// rollbackBestEffort calls injection.Rollback with a fresh, short-lived context detached from
// the caller's ctx, so an already-canceled/timed-out caller context can never prevent us from
// cleaning up a fault we just started — leaving a fault applied because the caller's context
// expired would defeat the entire point of this type. Errors are swallowed here because there
// is no caller left in this call stack to hand them to; a real implementation should route a
// rollback failure into OTLP chaos-event telemetry, mirroring the NOTE in
// NetworkDegradationInjector.autoRevertAfter.
func (r *HypothesisGuardedExperimentRunner) rollbackBestEffort(injection FaultInjection) {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = injection.Rollback(rollbackCtx)
}

// HTTPTelemetryQueryAPIMetricQueryPort is a stub MetricQueryPort adapter that queries
// telemetry-query-api over HTTP, decoupling HypothesisGuardedExperimentRunner from that
// service's transport entirely (it only ever talks to the MetricQueryPort interface).
//
// NOTE: telemetry-query-api's read surface (see its README's "ClickHouseQueryRepository"
// section) is percentile-latency and error-rate rollups keyed by (service, window, dagNodeId?)
// — not a single opaque query-string endpoint. This adapter models MetricQuery as a URL query
// string appended to a generic `/hypothesis-metric` path so this package's MetricQueryPort
// contract stays stable regardless of which underlying telemetry-query-api route eventually
// backs it; wiring MetricQuery to a real percentile/error-rate endpoint shape (e.g.
// `service=checkout-service&percentile=p99&windowMinutes=5`) is real-backend work left for when
// this adapter is actually deployed against a running telemetry-query-api, matching every other
// "seam documented, real wiring deferred" boundary in this service.
type HTTPTelemetryQueryAPIMetricQueryPort struct {
	// BaseURL is telemetry-query-api's base HTTP address, e.g. "http://telemetry-query-api:3000".
	BaseURL string
	// HTTPClient is the client used to issue requests. A nil HTTPClient defaults to
	// http.DefaultClient at construction time via NewHTTPTelemetryQueryAPIMetricQueryPort.
	HTTPClient *http.Client
}

// NewHTTPTelemetryQueryAPIMetricQueryPort constructs the adapter. Passing a nil httpClient
// selects http.DefaultClient.
func NewHTTPTelemetryQueryAPIMetricQueryPort(baseURL string, httpClient *http.Client) (*HTTPTelemetryQueryAPIMetricQueryPort, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("new http telemetry query api metric query port: baseURL is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HTTPTelemetryQueryAPIMetricQueryPort{BaseURL: baseURL, HTTPClient: httpClient}, nil
}

// hypothesisMetricResponse is the expected JSON shape of the (not-yet-real) telemetry-query-api
// endpoint this adapter calls: a single scalar value for the requested metric query.
type hypothesisMetricResponse struct {
	Value float64 `json:"value"`
}

// QueryMetric implements MetricQueryPort by issuing a GET request against BaseURL with
// metricQuery passed through as-is in the `query` URL parameter. See the type doc comment for
// why the exact endpoint shape is left as a documented seam rather than pinned to a specific
// telemetry-query-api route today.
func (p *HTTPTelemetryQueryAPIMetricQueryPort) QueryMetric(ctx context.Context, metricQuery string) (float64, error) {
	requestURL := fmt.Sprintf("%s/hypothesis-metric?query=%s", p.BaseURL, url.QueryEscape(metricQuery))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return 0, fmt.Errorf("query metric %q: build request: %w", metricQuery, err)
	}

	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("query metric %q: request failed: %w", metricQuery, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("query metric %q: telemetry-query-api returned status %d: %s", metricQuery, resp.StatusCode, string(body))
	}

	var decoded hypothesisMetricResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return 0, fmt.Errorf("query metric %q: decode response: %w", metricQuery, err)
	}
	return decoded.Value, nil
}
