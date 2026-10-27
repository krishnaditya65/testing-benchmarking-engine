// Command traffic-shadow-replay-worker is the entrypoint for one
// execution-fleet worker that replays previously-captured production
// traffic against a target environment at a configurable multiplier, and
// diffs the replayed responses against their original baselines. It
// implements the "Production Traffic Shadowing" and "Log-to-Test
// Generation" feature-manifest bullets — see this service's README.md for
// the full mapping.
//
// Like its sibling protocol-load-worker-go, this worker is stateless
// between runs and takes its traffic source (a fixture file or a gateway
// access log) and target from flags/env for local development; a production
// build wires the same dag.node.dispatch.v1 consumption
// protocol-load-worker-go's main.go documents, decoding a shadow-replay-shaped
// dispatch payload instead of a load-phase one.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-testing-benchmarking-tool/traffic-shadow-replay-worker/internal/shadowing"
)

// workerConfig holds every value this process needs, sourced from flags
// with environment-variable fallbacks — mirrors protocol-load-worker-go's
// cmd/protocol-load-worker/main.go workerConfig convention.
type workerConfig struct {
	// trafficSource selects where replayed traffic comes from:
	// "fixture" (read a capture.ReplayableTrafficFixture-shaped JSON file,
	// the default) or "alb-access-log" (ingest an ALB access log via
	// shadowing.ALBAccessLogIngester).
	trafficSource string
	inputPath     string

	targetBaseURL          string
	multiplier             int
	maxConcurrentReplays   int
	requestTimeout         time.Duration
	latencyRegressionRatio float64

	// findingsOutputPath, if non-empty, receives every DiffFinding with at
	// least one flagged issue as newline-delimited JSON, for a CI job or a
	// human to review after the run.
	findingsOutputPath string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := parseConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("traffic-shadow-replay-worker starting",
		"trafficSource", cfg.trafficSource,
		"inputPath", cfg.inputPath,
		"targetBaseURL", cfg.targetBaseURL,
		"multiplier", cfg.multiplier,
	)

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("traffic-shadow-replay-worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("traffic-shadow-replay-worker completed cleanly")
}

func parseConfig() workerConfig {
	var cfg workerConfig

	flag.StringVar(&cfg.trafficSource, "traffic-source", envOrDefault("TRAFFIC_SOURCE", "fixture"), "where replayed traffic comes from: fixture|alb-access-log")
	flag.StringVar(&cfg.inputPath, "input-path", envOrDefault("INPUT_PATH", "capture-fixture.json"), "path to a ReplayableTrafficFixture JSON file (traffic-source=fixture) or a gateway access log file (traffic-source=alb-access-log)")
	flag.StringVar(&cfg.targetBaseURL, "target-base-url", envOrDefault("TARGET_BASE_URL", "http://localhost:8080"), "scheme://host:port every captured exchange is replayed against")
	flag.IntVar(&cfg.multiplier, "multiplier", 2, "how many concurrent replays each captured exchange gets (2x/10x/100x traffic shadowing)")
	flag.IntVar(&cfg.maxConcurrentReplays, "max-concurrent-replays", 100, "hard cap on in-flight replay requests across the whole run, independent of multiplier")
	flag.DurationVar(&cfg.requestTimeout, "request-timeout", 10*time.Second, "per-replay timeout")
	flag.Float64Var(&cfg.latencyRegressionRatio, "latency-regression-ratio", 1.5, "actual/baseline latency ratio at or above which a replay is flagged as a latency regression")
	flag.StringVar(&cfg.findingsOutputPath, "findings-output-path", envOrDefault("FINDINGS_OUTPUT_PATH", "shadow-replay-findings.ndjson"), "path newline-delimited DiffFinding records (schema drift / latency regressions) are written to")
	flag.Parse()

	return cfg
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// run loads the configured traffic source into a ReplayableTrafficFixture,
// replays it against cfg.targetBaseURL via TrafficMultiplierReplayer, diffs
// every completed replay against its baseline via ResponseShapeDiffer, and
// writes flagged findings to cfg.findingsOutputPath.
func run(ctx context.Context, cfg workerConfig, logger *slog.Logger) error {
	fixture, err := loadFixture(ctx, cfg)
	if err != nil {
		return fmt.Errorf("main: loading traffic source: %w", err)
	}
	logger.Info("traffic source loaded", "exchanges", len(fixture.Exchanges), "fixtureName", fixture.FixtureName)

	results := make(chan shadowing.ReplayResult, 256)
	replayer := &shadowing.TrafficMultiplierReplayer{
		Fixture:              fixture,
		TargetBaseURL:        cfg.targetBaseURL,
		Multiplier:           cfg.multiplier,
		MaxConcurrentReplays: cfg.maxConcurrentReplays,
		RequestTimeout:       cfg.requestTimeout,
		Results:              results,
	}
	differ := shadowing.ResponseShapeDiffer{LatencyRegressionThreshold: cfg.latencyRegressionRatio}

	findingsFile, err := os.Create(cfg.findingsOutputPath)
	if err != nil {
		return fmt.Errorf("main: creating findings output file %s: %w", cfg.findingsOutputPath, err)
	}
	defer findingsFile.Close()

	drainDone := make(chan struct{})
	var flaggedCount int
	go func() {
		defer close(drainDone)
		for result := range results {
			finding := differ.Diff(result.SourceExchange, result)
			if result.Err != nil {
				logger.Debug("replay failed", "path", result.SourceExchange.Path, "pass", result.ReplayPass, "error", result.Err)
				continue
			}
			if !finding.HasFindings() {
				continue
			}
			flaggedCount++
			if writeErr := writeFindingLine(findingsFile, finding); writeErr != nil {
				logger.Warn("failed writing finding to findings output", "error", writeErr)
			}
		}
	}()

	summary, err := replayer.Replay(ctx)
	close(results)
	<-drainDone

	if err != nil {
		return fmt.Errorf("main: replaying traffic: %w", err)
	}

	logger.Info("shadow replay complete",
		"exchangesReplayed", summary.ExchangesReplayed,
		"totalReplays", summary.TotalReplays,
		"failedReplays", summary.FailedReplays,
		"averageLatency", summary.AverageLatency().String(),
		"flaggedFindings", flaggedCount,
		"findingsOutputPath", cfg.findingsOutputPath,
	)
	return nil
}

// loadFixture reads cfg.inputPath through whichever traffic source
// cfg.trafficSource names.
func loadFixture(ctx context.Context, cfg workerConfig) (shadowing.ReplayableTrafficFixture, error) {
	switch cfg.trafficSource {
	case "", "fixture":
		return shadowing.ReadReplayableTrafficFixtureFile(cfg.inputPath)
	case "alb-access-log":
		file, err := os.Open(cfg.inputPath)
		if err != nil {
			return shadowing.ReplayableTrafficFixture{}, fmt.Errorf("opening ALB access log %s: %w", cfg.inputPath, err)
		}
		defer file.Close()
		return shadowing.ALBAccessLogIngester{}.IngestFixture(ctx, file)
	default:
		return shadowing.ReplayableTrafficFixture{}, fmt.Errorf("unknown traffic source %q (want fixture|alb-access-log)", cfg.trafficSource)
	}
}

// writeFindingLine appends one DiffFinding to the findings output file as a
// single line of JSON (newline-delimited JSON, so the file can be tailed/
// streamed and grown across a long-running replay without holding every
// finding in memory).
func writeFindingLine(file *os.File, finding shadowing.DiffFinding) error {
	encoded, err := json.Marshal(finding)
	if err != nil {
		return fmt.Errorf("marshaling finding for %s: %w", finding.ExchangePath, err)
	}
	_, err = file.Write(append(encoded, '\n'))
	return err
}
