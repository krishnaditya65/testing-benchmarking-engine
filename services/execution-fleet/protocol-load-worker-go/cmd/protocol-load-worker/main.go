// Command protocol-load-worker is the entrypoint for one execution-fleet
// worker process. It wires together a protocolclient.ProtocolClient, a
// loadgen.ConcurrentLoadGenerator, and a pipeline.MultiProtocolPipelineExecutor,
// then blocks running the configured load phase until it completes or the
// process receives SIGTERM/SIGINT.
//
// NOTE: this starter drives one hard-coded example load phase from flags
// so the wiring can be exercised end to end. A production build replaces
// runConfiguredLoadPhase's request construction with a real
// dag.node.dispatch.v1 consumer (see ARCHITECTURE.md's communication
// contracts): the worker would subscribe to that Redpanda topic, decode a
// DAG dispatch payload per shared-libraries/protocol-contracts, and build
// the TrafficProfile/PipelineStep values below from it instead of flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/capture"
	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/extraction"
	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/loadgen"
	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/pipeline"
	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/protocolclient"
)

// workerConfig holds every value this process needs, sourced from flags
// with environment-variable fallbacks so the same binary works under a
// Kubernetes Deployment (env vars) or a developer's shell (flags).
type workerConfig struct {
	targetURL         string
	protocol          string
	startConcurrency  int
	peakConcurrency   int
	rampDuration      time.Duration
	phaseDuration     time.Duration
	requestTimeout    time.Duration
	otelCollectorAddr string
	runID             string

	// captureMode enables LiveTrafficCaptureRecorder as a passive sidecar
	// around whatever ProtocolClient newProtocolClient built: every request
	// this worker sends still executes identically, but the
	// request/response pairs are also recorded (headers redacted via
	// capture.SensitiveHeaderRedactor) and written to captureOutputPath as
	// a capture.ReplayableTrafficFixture once the load phase completes.
	captureMode       bool
	captureOutputPath string

	// loadStrategy selects which loadgen.LoadGenerationStrategy drives the
	// phase: "goroutine-per-vu" (loadgen.ConcurrentLoadGenerator, one
	// physical connection per virtual user) or "http2-multiplexed"
	// (loadgen.Http2MultiplexedRequestScheduler, many virtual users sharing
	// a capped pool of HTTP/2 connections per BrowserLikeMultiplexingPolicy).
	loadStrategy string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := parseConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("protocol-load-worker starting",
		"target", cfg.targetURL,
		"protocol", cfg.protocol,
		"peakConcurrency", cfg.peakConcurrency,
		"phaseDuration", cfg.phaseDuration.String(),
		"runID", cfg.runID,
	)

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("protocol-load-worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("protocol-load-worker completed cleanly")
}

// parseConfig reads flags (with env-var defaults) into a workerConfig.
// Flags take precedence over environment variables so a Kubernetes
// manifest can set env defaults while still allowing local overrides.
func parseConfig() workerConfig {
	var cfg workerConfig

	flag.StringVar(&cfg.targetURL, "target-url", envOrDefault("TARGET_URL", "http://localhost:8080/health"), "URL/authority the load phase calls")
	flag.StringVar(&cfg.protocol, "protocol", envOrDefault("PROTOCOL", "rest"), "protocol to speak: rest|graphql|grpc-stream|websocket")
	flag.IntVar(&cfg.startConcurrency, "start-concurrency", 1, "concurrency at the start of the ramp")
	flag.IntVar(&cfg.peakConcurrency, "peak-concurrency", 50, "concurrency the ramp climbs to")
	flag.DurationVar(&cfg.rampDuration, "ramp-duration", 10*time.Second, "time to climb from start to peak concurrency")
	flag.DurationVar(&cfg.phaseDuration, "phase-duration", 30*time.Second, "total duration of the load phase")
	flag.DurationVar(&cfg.requestTimeout, "request-timeout", 10*time.Second, "per-call timeout")
	flag.StringVar(&cfg.otelCollectorAddr, "otel-collector-addr", envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector.telemetry-lake.svc:4317"), "OTLP gRPC endpoint telemetry is exported to")
	flag.StringVar(&cfg.runID, "run-id", envOrDefault("RUN_ID", "local-dev-run"), "test run identifier, used as the checkpoint key")
	flag.BoolVar(&cfg.captureMode, "capture-mode", envOrDefault("CAPTURE_MODE", "") == "true", "run as a passive traffic-capture sidecar: record request/response pairs (headers redacted) alongside normal execution and write a ReplayableTrafficFixture to -capture-output-path")
	flag.StringVar(&cfg.captureOutputPath, "capture-output-path", envOrDefault("CAPTURE_OUTPUT_PATH", "capture-fixture.json"), "file path a captured ReplayableTrafficFixture is written to when -capture-mode is set")
	flag.StringVar(&cfg.loadStrategy, "load-strategy", envOrDefault("LOAD_STRATEGY", "goroutine-per-vu"), "loadgen.LoadGenerationStrategy to drive the phase with: goroutine-per-vu|http2-multiplexed")
	flag.Parse()

	return cfg
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// run builds the protocol client, load generator, and pipeline executor
// described by cfg and executes one load phase to completion (or until
// ctx is cancelled by a shutdown signal).
//
// NOTE: OTLP export is not implemented in this starter — a production
// build would construct an otel.TracerProvider/MeterProvider pointed at
// cfg.otelCollectorAddr (via shared-libraries/observability-kit, per
// ARCHITECTURE.md) here, and each loadgen.CallResult drained below would
// become a span/metric point instead of a log line.
func run(ctx context.Context, cfg workerConfig, logger *slog.Logger) error {
	client, err := newProtocolClient(cfg.protocol)
	if err != nil {
		return fmt.Errorf("main: selecting protocol client: %w", err)
	}
	defer client.Close()

	// --capture-mode wraps client in a passive recorder rather than
	// replacing it: everything downstream (the pipeline probe, the load
	// generator) keeps talking to the protocolclient.ProtocolClient
	// interface exactly as before, unaware capture is happening, per
	// LiveTrafficCaptureRecorder's decorator contract.
	var recorder *capture.LiveTrafficCaptureRecorder
	if cfg.captureMode {
		recorder = capture.NewLiveTrafficCaptureRecorder(client, cfg.runID)
		client = recorder
		logger.Info("capture-mode enabled; recording redacted request/response pairs",
			"captureOutputPath", cfg.captureOutputPath)
	}

	// A minimal single-step pipeline demonstrates the executor wiring;
	// a real dispatch payload would describe however many chained steps
	// the DAG test node needs (e.g. REST JWT fetch -> gRPC stream).
	varCtx := extraction.NewVariableContext()
	varCtx.Set("targetURL", cfg.targetURL)

	executor := pipeline.NewMultiProtocolPipelineExecutor([]pipeline.PipelineStep{
		{
			Name:   "probe-target",
			Client: client,
			BuildRequest: func(vc *extraction.VariableContext) protocolclient.Request {
				url, _ := vc.GetString("targetURL")
				return protocolclient.Request{
					TargetURL: url,
					Method:    "GET",
					Timeout:   cfg.requestTimeout,
				}
			},
		},
	})

	probeResult := executor.Execute(ctx)
	if probeResult.Err != nil {
		logger.Warn("initial pipeline probe failed; continuing to load phase anyway", "error", probeResult.Err)
	}

	results := make(chan loadgen.CallResult, 256)
	requestFactory := func(ctx context.Context) protocolclient.Request {
		return protocolclient.Request{
			TargetURL: cfg.targetURL,
			Method:    "GET",
			Timeout:   cfg.requestTimeout,
		}
	}

	strategy, err := newLoadGenerationStrategy(cfg, client, requestFactory, results)
	if err != nil {
		return fmt.Errorf("main: selecting load generation strategy: %w", err)
	}
	logger.Info("load generation strategy selected", "strategy", strategy.StrategyName())

	profile := loadgen.LinearRampProfile{
		StartConcurrency: cfg.startConcurrency,
		EndConcurrency:   cfg.peakConcurrency,
		RampDuration:     cfg.rampDuration,
		TotalDuration:    cfg.phaseDuration,
	}

	// Drain results concurrently so the generator's workers never block
	// on a full channel; this is where OTLP export would hook in.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		var observed int64
		for result := range results {
			observed++
			if result.Response.Err != nil {
				logger.Debug("call failed", "worker", result.WorkerID, "error", result.Response.Err)
			}
		}
		logger.Info("results drain finished", "callsObserved", observed)
	}()

	metrics, err := strategy.Run(ctx, profile)
	close(results)
	<-drainDone

	if err != nil {
		return fmt.Errorf("main: load phase %q: %w", profile.Name(), err)
	}

	logger.Info("load phase complete",
		"phase", profile.Name(),
		"totalCalls", metrics.TotalCalls,
		"successfulCalls", metrics.SuccessfulCalls,
		"failedCalls", metrics.FailedCalls,
		"averageLatency", metrics.AverageLatency().String(),
		"maxLatency", metrics.MaxLatency.String(),
	)

	if recorder != nil {
		if err := recorder.WriteJSONFile(cfg.captureOutputPath); err != nil {
			return fmt.Errorf("main: writing captured traffic fixture: %w", err)
		}
		logger.Info("capture-mode fixture written", "path", cfg.captureOutputPath)
	}

	return nil
}

// newLoadGenerationStrategy builds the loadgen.LoadGenerationStrategy named
// by cfg.loadStrategy. Both branches share the same RequestFactory,
// CheckpointStore, and Results channel so switching strategies never changes
// anything about how a load phase is observed or checkpointed — only how
// physical connections are managed underneath it.
//
// "http2-multiplexed" intentionally ignores client: Http2MultiplexedRequestScheduler
// dials its own pooled *http.Client transports per ConnectionReusePolicy
// (see http2_multiplexed_request_scheduler.go) rather than reusing whatever
// protocolclient.ProtocolClient newProtocolClient built, since multiplexing
// topology needs direct control over the transport's connection limits that
// the ProtocolClient interface doesn't expose.
func newLoadGenerationStrategy(cfg workerConfig, client protocolclient.ProtocolClient, requestFactory loadgen.RequestFactory, results chan<- loadgen.CallResult) (loadgen.LoadGenerationStrategy, error) {
	switch cfg.loadStrategy {
	case "", "goroutine-per-vu":
		return &loadgen.ConcurrentLoadGenerator{
			Client:          client,
			RunID:           cfg.runID,
			CheckpointStore: loadgen.NoopCheckpointStore{}, // NOTE: swap for a real Redis-backed store in production.
			Results:         results,
			RequestFactory:  requestFactory,
		}, nil
	case "http2-multiplexed":
		return &loadgen.Http2MultiplexedRequestScheduler{
			RequestFactory:  requestFactory,
			Policy:          loadgen.BrowserLikeMultiplexingPolicy{},
			RunID:           cfg.runID,
			CheckpointStore: loadgen.NoopCheckpointStore{}, // NOTE: swap for a real Redis-backed store in production.
			RequestTimeout:  cfg.requestTimeout,
			Results:         results,
		}, nil
	default:
		return nil, fmt.Errorf("unknown load strategy %q (want goroutine-per-vu|http2-multiplexed)", cfg.loadStrategy)
	}
}

// newProtocolClient builds the ProtocolClient implementation named by
// protocol. Returns an error for unrecognized names rather than silently
// defaulting, since a load phase running against the wrong protocol is a
// correctness bug, not a warning.
func newProtocolClient(protocol string) (protocolclient.ProtocolClient, error) {
	switch protocol {
	case "rest":
		return protocolclient.NewRestHttpClient(), nil
	case "graphql":
		return protocolclient.NewGraphQlClient(), nil
	case "grpc-stream":
		return protocolclient.NewGrpcStreamClient(true), nil
	case "websocket":
		return protocolclient.NewWebSocketClient(), nil
	default:
		return nil, fmt.Errorf("unknown protocol %q (want rest|graphql|grpc-stream|websocket)", protocol)
	}
}
