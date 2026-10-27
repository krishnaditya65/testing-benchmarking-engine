package shadowing

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ReplayResult is the outcome of replaying one CapturedExchange once against
// TrafficMultiplierReplayer.TargetBaseURL. ResponseShapeDiffer
// (response_shape_differ.go) consumes these paired back against the
// exchange's originally-captured response to flag drift/regression.
type ReplayResult struct {
	// SourceExchange is the fixture exchange this replay was driven from.
	SourceExchange CapturedExchange

	// ReplayPass identifies which of Multiplier concurrent replays of
	// SourceExchange this result is (0-indexed), so a caller can tell
	// "replay 47 of the 2nd exchange at 10x" apart from "replay 3."
	ReplayPass int

	// StatusCode, Headers, Body, and Latency are what the target environment
	// actually returned for this replay — the "actual" side of the
	// baseline-vs-actual comparison ResponseShapeDiffer performs.
	StatusCode int
	Headers    map[string]string
	Body       []byte
	Latency    time.Duration

	// Err is non-nil when the replay request itself failed (dial error,
	// timeout) rather than merely returning an unexpected response — a
	// distinct failure mode from a shape/latency diff.
	Err error
}

// ReplaySummary aggregates every ReplayResult from one TrafficMultiplierReplayer.Replay
// call.
type ReplaySummary struct {
	ExchangesReplayed int
	TotalReplays      int // ExchangesReplayed * Multiplier, minus any skipped on ctx cancellation
	FailedReplays     int
	TotalLatency      time.Duration
}

func (s ReplaySummary) AverageLatency() time.Duration {
	if s.TotalReplays == 0 {
		return 0
	}
	return time.Duration(int64(s.TotalLatency) / int64(s.TotalReplays))
}

// TrafficMultiplierReplayer implements this service's "Production Traffic
// Shadowing" feature-manifest bullet: it takes a ReplayableTrafficFixture —
// real captured production traffic — and replays every exchange in it
// against TargetBaseURL at a configurable Multiplier (2x, 10x, 100x
// concurrent replays per exchange), so a target environment (a staging
// deploy, a canary, a capacity-planning target) can be shadow-tested against
// realistic traffic *shape* at N times its original volume, not just
// synthetic load.
//
// This is deliberately not the same thing as loadgen.ConcurrentLoadGenerator
// in protocol-load-worker-go: that package drives a declarative
// TrafficProfile concurrency curve against one RequestFactory. This type
// replays a fixed, already-recorded sequence of *distinct* requests (real
// paths, real payload shapes, real header combinations) — the traffic shape
// itself is the point, not just the concurrency curve.
type TrafficMultiplierReplayer struct {
	// Fixture is the captured traffic to replay. Every exchange's Path is
	// combined with TargetBaseURL to build the actual replay URL; the
	// exchange's original CapturedFromHost is not dialed directly (that
	// would defeat the point of shadowing a *different* target
	// environment).
	Fixture ReplayableTrafficFixture

	// TargetBaseURL is the scheme://host:port every exchange is replayed
	// against, e.g. "https://staging.internal.example.com". Required.
	TargetBaseURL string

	// Multiplier is how many concurrent replays each exchange in Fixture
	// gets — the "2x/10x/100x" from the feature-manifest bullet. Values
	// <= 0 are treated as 1 (replay each exchange exactly once).
	Multiplier int

	// MaxConcurrentReplays bounds how many replay requests are in flight at
	// once across the *entire* run, independent of Multiplier — e.g.
	// replaying a 500-exchange fixture at 100x is 50,000 logical replays,
	// which must not all fire simultaneously regardless of how "realistic"
	// that would be. Defaults to 100 if <= 0.
	MaxConcurrentReplays int

	// RequestTimeout bounds each individual replay call. Defaults to 30s.
	RequestTimeout time.Duration

	// HTTPClient issues the replay requests. A zero value uses
	// http.DefaultClient's transport defaults; callers replaying at high
	// Multiplier against one host should supply a client whose transport
	// raises MaxIdleConnsPerHost, mirroring protocol-load-worker-go's
	// RestHttpClient.
	HTTPClient *http.Client

	// Results, if non-nil, receives every ReplayResult as it completes. The
	// replayer does not close this channel; callers should drain it
	// concurrently with Replay to avoid backpressuring replay goroutines.
	Results chan<- ReplayResult
}

// Replay issues every exchange in r.Fixture against r.TargetBaseURL,
// r.Multiplier times each, bounded to r.MaxConcurrentReplays requests in
// flight at once, and returns an aggregated ReplaySummary. It respects ctx
// cancellation: in-flight replays are allowed to finish, but no new ones
// start once ctx is done.
func (r *TrafficMultiplierReplayer) Replay(ctx context.Context) (ReplaySummary, error) {
	if r.TargetBaseURL == "" {
		return ReplaySummary{}, fmt.Errorf("shadowing: TrafficMultiplierReplayer requires a non-empty TargetBaseURL")
	}
	multiplier := r.Multiplier
	if multiplier <= 0 {
		multiplier = 1
	}
	maxConcurrent := r.MaxConcurrentReplays
	if maxConcurrent <= 0 {
		maxConcurrent = 100
	}
	requestTimeout := r.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	var mu sync.Mutex
	summary := ReplaySummary{ExchangesReplayed: len(r.Fixture.Exchanges)}

	for _, exchange := range r.Fixture.Exchanges {
		exchange := exchange
		for pass := 0; pass < multiplier; pass++ {
			pass := pass

			select {
			case <-ctx.Done():
				wg.Wait()
				return summary, nil
			case semaphore <- struct{}{}:
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-semaphore }()

				result := r.replayOne(ctx, exchange, pass, requestTimeout, httpClient)

				mu.Lock()
				summary.TotalReplays++
				summary.TotalLatency += result.Latency
				if result.Err != nil {
					summary.FailedReplays++
				}
				mu.Unlock()

				if r.Results != nil {
					select {
					case r.Results <- result:
					case <-ctx.Done():
					}
				}
			}()
		}
	}

	wg.Wait()
	return summary, nil
}

// replayOne issues a single replay of exchange (one of its Multiplier
// passes) against r.TargetBaseURL.
func (r *TrafficMultiplierReplayer) replayOne(ctx context.Context, exchange CapturedExchange, pass int, timeout time.Duration, httpClient *http.Client) ReplayResult {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	targetURL := strings.TrimRight(r.TargetBaseURL, "/") + exchange.Path
	method := exchange.Method
	if method == "" {
		method = http.MethodGet
	}

	httpReq, err := http.NewRequestWithContext(callCtx, method, targetURL, bytes.NewReader(exchange.RequestBody))
	if err != nil {
		return ReplayResult{
			SourceExchange: exchange,
			ReplayPass:     pass,
			Err:            fmt.Errorf("shadowing: building replay request for %s: %w", targetURL, err),
		}
	}
	for key, value := range exchange.RequestHeaders {
		// Skip the redacted placeholder outright rather than replaying a
		// literal "[REDACTED]" auth header — a shadow-replay target
		// legitimately needs its own valid credentials injected by the
		// caller (e.g. via a wrapping RoundTripper), not the sanitized
		// value a captured fixture retained.
		if value == redactedValuePlaceholder {
			continue
		}
		httpReq.Header.Set(key, value)
	}

	dispatchedAt := time.Now()
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return ReplayResult{
			SourceExchange: exchange,
			ReplayPass:     pass,
			Latency:        time.Since(dispatchedAt),
			Err:            fmt.Errorf("shadowing: replaying %s %s: %w", method, targetURL, err),
		}
	}
	defer httpResp.Body.Close()

	bodyBytes, readErr := io.ReadAll(httpResp.Body)
	latency := time.Since(dispatchedAt)
	if readErr != nil {
		return ReplayResult{
			SourceExchange: exchange,
			ReplayPass:     pass,
			StatusCode:     httpResp.StatusCode,
			Latency:        latency,
			Err:            fmt.Errorf("shadowing: reading replay response body from %s: %w", targetURL, readErr),
		}
	}

	headers := make(map[string]string, len(httpResp.Header))
	for key := range httpResp.Header {
		headers[key] = httpResp.Header.Get(key)
	}

	return ReplayResult{
		SourceExchange: exchange,
		ReplayPass:     pass,
		StatusCode:     httpResp.StatusCode,
		Headers:        headers,
		Body:           bodyBytes,
		Latency:        latency,
	}
}

// redactedValuePlaceholder mirrors protocol-load-worker-go's
// internal/capture.redactedValuePlaceholder constant. Duplicated for the
// same cross-module reason documented on CapturedExchange in
// replayable_traffic_fixture.go — if these two services gain a shared Go
// module, this constant moves there too.
const redactedValuePlaceholder = "[REDACTED]"
