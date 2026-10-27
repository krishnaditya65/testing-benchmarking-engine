package workflow

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

// LiveTelemetryQueryPort is the outbound port to the sibling
// telemetry-query-api service (telemetry-lake layer) used only by
// LiveTelemetryThresholdWatcher to poll for a running pipeline's current
// p99 latency during its ramp-up phase. Modeled as an interface (rather than
// an http.Client field) so the watcher's threshold-crossing logic is unit
// testable against a fake without a live telemetry-query-api instance.
type LiveTelemetryQueryPort interface {
	// FetchCurrentP99LatencyMs returns the p99 latency, in milliseconds,
	// observed so far for runID's active load-test node(s). Errors are
	// treated as "no data yet" by the watcher (see pollOnce) rather than
	// fatal, since telemetry can lag a few seconds behind dispatch during
	// ramp-up.
	FetchCurrentP99LatencyMs(ctx context.Context, runID string) (float64, error)
}

// LiveTelemetryThresholdWatcherConfig configures one watcher instance for
// one in-flight pipeline run.
type LiveTelemetryThresholdWatcherConfig struct {
	// RunID is the Temporal RunID of the DagPipelineWorkflow execution being
	// watched (see workflow.GetInfo(ctx).WorkflowExecution.RunID in
	// dag_pipeline_workflow.go) — also the run identifier telemetry-query-api
	// indexes ingested telemetry by.
	RunID string
	// WorkflowID is the Temporal WorkflowID, required alongside RunID to
	// target client.SignalWorkflow.
	WorkflowID string
	// P99LatencyThresholdMs is the ramp-up-phase p99 latency ceiling; a
	// sustained breach emits a DagMutationSignal. This is deliberately a
	// simple static threshold (rather than an anomaly-detection model) —
	// the SLA thresholds pipeline authors already configure via
	// pipeline-api-gateway's quality-gate module are the natural source for
	// this value in production.
	P99LatencyThresholdMs float64
	// PollInterval controls how often telemetry-query-api is queried.
	PollInterval time.Duration
	// ConsecutiveBreachesRequired guards against emitting a mutation signal
	// off a single noisy sample; the threshold must be crossed this many
	// polls in a row before a signal is emitted.
	ConsecutiveBreachesRequired int
}

// LiveTelemetryThresholdWatcher polls telemetry-query-api for a running
// pipeline's live p99 latency during ramp-up and, once it crosses
// P99LatencyThresholdMs for ConsecutiveBreachesRequired consecutive polls,
// signals the running DagPipelineWorkflow with a DagMutationSignal so the
// workflow can react (inject a diagnostic trace-capture node, skip a
// now-pointless branch, or abort outright) without waiting for the whole DAG
// to finish first. This is the mechanism that makes "dynamic DAG mutation
// mid-run" actually dynamic — it is the producer side of
// DagMutationSignalChannelName; DagMutationSignalHandler in
// dag_mutation_signal_handler.go is the consumer side inside the workflow.
//
// LiveTelemetryThresholdWatcher runs outside the workflow (in the scheduler
// process, alongside the Temporal worker — see cmd/scheduler/main.go), not
// as workflow code itself, precisely because it needs to make a real,
// non-deterministic network call to telemetry-query-api on a wall-clock
// timer; that would violate Temporal's workflow-determinism constraint if
// done directly inside DagPipelineWorkflow.
type LiveTelemetryThresholdWatcher struct {
	telemetryClient LiveTelemetryQueryPort
	temporalClient  client.Client
	cfg             LiveTelemetryThresholdWatcherConfig
}

// NewLiveTelemetryThresholdWatcher constructs a watcher for one run, wiring
// in the telemetry query port and the Temporal client used to deliver the
// resulting mutation signal.
func NewLiveTelemetryThresholdWatcher(
	telemetryClient LiveTelemetryQueryPort,
	temporalClient client.Client,
	cfg LiveTelemetryThresholdWatcherConfig,
) *LiveTelemetryThresholdWatcher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.ConsecutiveBreachesRequired <= 0 {
		cfg.ConsecutiveBreachesRequired = 3
	}
	return &LiveTelemetryThresholdWatcher{
		telemetryClient: telemetryClient,
		temporalClient:  temporalClient,
		cfg:             cfg,
	}
}

// Watch blocks, polling telemetry-query-api every cfg.PollInterval, until
// ctx is cancelled (typically because the caller learned the run has
// terminated) or a mutation signal has been successfully delivered. Callers
// should run this in its own goroutine — see cmd/scheduler/main.go for how
// it would be started alongside a triggered run in production, gated behind
// a per-pipeline "enable live mutation" opt-in so this polling loop is not
// spun up for every run unconditionally.
//
// NOTE: this starter implementation stops after delivering the first
// mutation signal (abort/skip/inject are all one-shot reactions to a single
// breach episode). A production version would likely keep watching after an
// INJECT_DIAGNOSTIC_NODE signal (since the run continues) but stop after
// ABORT_RUN/SKIP_REMAINING_BRANCH have resolved the breach it was watching
// for, and would deduplicate against the diagnostic node ID it already
// injected so repeated breaches don't loop forever.
func (w *LiveTelemetryThresholdWatcher) Watch(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	consecutiveBreaches := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			breached, observedP99, err := w.pollOnce(ctx)
			if err != nil {
				log.Printf("live_telemetry_threshold_watcher: run %s: poll error (will retry): %v", w.cfg.RunID, err)
				continue
			}

			if !breached {
				consecutiveBreaches = 0
				continue
			}

			consecutiveBreaches++
			log.Printf(
				"live_telemetry_threshold_watcher: run %s: p99=%.1fms breached threshold=%.1fms (%d/%d consecutive)",
				w.cfg.RunID, observedP99, w.cfg.P99LatencyThresholdMs, consecutiveBreaches, w.cfg.ConsecutiveBreachesRequired,
			)

			if consecutiveBreaches < w.cfg.ConsecutiveBreachesRequired {
				continue
			}

			if err := w.emitDiagnosticCaptureSignal(ctx, observedP99); err != nil {
				return fmt.Errorf("live_telemetry_threshold_watcher: run %s: emit mutation signal: %w", w.cfg.RunID, err)
			}
			return nil
		}
	}
}

// pollOnce fetches the current p99 and reports whether it breaches the
// configured threshold.
func (w *LiveTelemetryThresholdWatcher) pollOnce(ctx context.Context) (breached bool, observedP99Ms float64, err error) {
	observedP99Ms, err = w.telemetryClient.FetchCurrentP99LatencyMs(ctx, w.cfg.RunID)
	if err != nil {
		return false, 0, err
	}
	return observedP99Ms > w.cfg.P99LatencyThresholdMs, observedP99Ms, nil
}

// emitDiagnosticCaptureSignal delivers an INJECT_DIAGNOSTIC_NODE
// DagMutationSignal to the watched workflow, requesting a trace-capture node
// be spliced in immediately so the anomaly's root cause has a chance of
// being captured while it is still happening rather than only inferred
// after the fact from the flakiness bisector.
func (w *LiveTelemetryThresholdWatcher) emitDiagnosticCaptureSignal(ctx context.Context, observedP99Ms float64) error {
	signal := DagMutationSignal{
		MutationType: DagMutationTypeInjectDiagnosticNode,
		Reason: fmt.Sprintf(
			"p99 latency %.1fms breached ramp-up threshold %.1fms for %d consecutive polls",
			observedP99Ms, w.cfg.P99LatencyThresholdMs, w.cfg.ConsecutiveBreachesRequired,
		),
		IssuedAtUnixMilli: time.Now().UnixMilli(),
		InjectDiagnosticNode: &InjectDiagnosticNodeDetail{
			AfterNodeID:                rampUpAnchorNodeID(w.cfg.RunID),
			DiagnosticNodeID:           fmt.Sprintf("diagnostic-capture-%d", time.Now().UnixMilli()),
			TraceCaptureTargetSelector: fmt.Sprintf("run=%s", w.cfg.RunID),
		},
	}

	return w.temporalClient.SignalWorkflow(ctx, w.cfg.WorkflowID, w.cfg.RunID, DagMutationSignalChannelName, signal)
}

// rampUpAnchorNodeID is a placeholder for resolving which currently-running
// LOAD_STEP node the diagnostic capture should be wired after.
//
// NOTE: in production this would be resolved from the same run-status
// projection pipeline-api-gateway's PipelineRunEntity.nodeStatuses maintains
// (i.e. "which node is currently RUNNING for this run right now"), passed
// into the watcher at construction time or looked up via a small port to
// pipeline-run.service.ts, rather than hardcoded here.
func rampUpAnchorNodeID(runID string) string {
	return fmt.Sprintf("ramp-up-%s", runID)
}
