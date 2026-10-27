package capture

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/protocolclient"
)

// LiveTrafficCaptureRecorder wraps a real protocolclient.ProtocolClient in
// passive sidecar mode: it delegates every call unchanged to Inner, so a
// load phase or functional test run behaves identically whether or not
// capture is enabled, and observes the resulting request/response pair on
// the way past. This makes capture mode a decorator around the same
// ProtocolClient interface everything else in this service already codes
// against (cmd/protocol-load-worker/main.go's --capture-mode flag wires this
// in front of whichever client newProtocolClient built, in place of using
// that client directly) — nothing downstream needs to know capture is
// happening.
//
// LiveTrafficCaptureRecorder itself satisfies protocolclient.ProtocolClient,
// so it can be dropped in anywhere a ProtocolClient is expected
// (ConcurrentLoadGenerator.Client, Http2MultiplexedRequestScheduler via its
// own request path, pipeline.MultiProtocolPipelineExecutor's steps).
type LiveTrafficCaptureRecorder struct {
	// Inner is the real client every call is delegated to. Required.
	Inner protocolclient.ProtocolClient

	// SourceRunID tags the resulting fixture with the run that produced it.
	SourceRunID string

	// FixtureName is a human-readable label for the eventual fixture (see
	// ReplayableTrafficFixture.FixtureName). Defaults to
	// "capture-<SourceRunID>" if empty at Fixture()-build time.
	FixtureName string

	redactor SensitiveHeaderRedactor

	mu               sync.Mutex
	startedAt        time.Time
	exchanges        []CapturedExchange
	redactedNames    map[string]struct{}
	capturedFromHost string // set from the first observed request's authority; guarded by mu
}

// NewLiveTrafficCaptureRecorder wraps inner for passive capture. Panics if
// inner is nil, matching this service's existing convention of failing fast
// on programmer error rather than returning an error type for something
// only a bad wiring call site could produce (compare
// ConcurrentLoadGenerator.Run's runtime nil check, which the caller *can*
// hit from bad config and therefore returns an error instead).
func NewLiveTrafficCaptureRecorder(inner protocolclient.ProtocolClient, sourceRunID string) *LiveTrafficCaptureRecorder {
	if inner == nil {
		panic("capture: NewLiveTrafficCaptureRecorder requires a non-nil inner ProtocolClient")
	}
	return &LiveTrafficCaptureRecorder{
		Inner:       inner,
		SourceRunID: sourceRunID,
		startedAt:   time.Now(),
	}
}

// Execute delegates to Inner.Execute unchanged, then records the
// request/response pair via Observe before returning Inner's result
// untouched. A capture failure (there isn't really a failure mode here
// beyond an allocation) must never surface as a load-phase error, since
// capture is strictly observational.
func (r *LiveTrafficCaptureRecorder) Execute(ctx context.Context, req protocolclient.Request) (protocolclient.Response, error) {
	dispatchedAt := time.Now()
	resp, err := r.Inner.Execute(ctx, req)
	r.Observe(req, resp, dispatchedAt)
	return resp, err
}

// Protocol delegates to Inner.
func (r *LiveTrafficCaptureRecorder) Protocol() string { return r.Inner.Protocol() }

// Close delegates to Inner. It does not flush the fixture — callers own
// that explicitly via Fixture()/WriteJSONFile so a worker can keep capturing
// across multiple pipeline steps or load phases before deciding it's done.
func (r *LiveTrafficCaptureRecorder) Close() error { return r.Inner.Close() }

// Observe records one already-completed request/response pair. Exported
// (rather than a private recording step inside Execute only) so
// cmd/protocol-load-worker can also feed it exchanges observed outside the
// ProtocolClient.Execute path if a future capture source needs that (e.g.
// an ingress-side tap), without needing a second recorder type.
//
// Headers are redacted via SensitiveHeaderRedactor before anything is
// retained in memory — capture-mode never holds an unredacted header value
// past this call, so a crash dump or heap profile taken mid-run can't leak
// one either.
func (r *LiveTrafficCaptureRecorder) Observe(req protocolclient.Request, resp protocolclient.Response, dispatchedAt time.Time) {
	redactedReqHeaders, reqRedactedNames := r.redactor.Redact(req.Headers)
	redactedRespHeaders, respRedactedNames := r.redactor.Redact(resp.Headers)

	path, host := splitTargetURL(req.TargetURL)

	exchange := CapturedExchange{
		Protocol:        resp.Metrics.Protocol,
		Method:          req.Method,
		Path:            path,
		RequestHeaders:  redactedReqHeaders,
		ResponseHeaders: redactedRespHeaders,
		RequestBody:     req.Body,
		ResponseBody:    resp.Body,
		StatusCode:      resp.StatusCode,
		CapturedAt:      dispatchedAt,
		Latency:         resp.Metrics.Latency,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.exchanges = append(r.exchanges, exchange)
	if r.redactedNames == nil {
		r.redactedNames = make(map[string]struct{})
	}
	for _, name := range reqRedactedNames {
		r.redactedNames[name] = struct{}{}
	}
	for _, name := range respRedactedNames {
		r.redactedNames[name] = struct{}{}
	}
	// capturedFromHost is set lazily from the first observed request; every
	// exchange after that is assumed to target the same authority, matching
	// this recorder's one-client-per-target usage in main.go. Stored on the
	// recorder (rather than derived from exchanges[0] at Fixture()-build
	// time) so it survives even if a future streaming capture mode trims
	// old exchanges out of the buffer.
	if host != "" && r.capturedFromHost == "" {
		r.capturedFromHost = host
	}
}

func (r *LiveTrafficCaptureRecorder) fixtureName() string {
	if r.FixtureName != "" {
		return r.FixtureName
	}
	return fmt.Sprintf("capture-%s", r.SourceRunID)
}

// Fixture snapshots every exchange observed so far into a
// ReplayableTrafficFixture. Safe to call mid-capture (e.g. periodically, to
// checkpoint partial progress) — it does not clear the recorder's buffer.
func (r *LiveTrafficCaptureRecorder) Fixture() ReplayableTrafficFixture {
	r.mu.Lock()
	defer r.mu.Unlock()

	exchanges := make([]CapturedExchange, len(r.exchanges))
	copy(exchanges, r.exchanges)

	names := make([]string, 0, len(r.redactedNames))
	for name := range r.redactedNames {
		names = append(names, name)
	}

	return ReplayableTrafficFixture{
		FixtureName:         r.fixtureName(),
		SourceRunID:         r.SourceRunID,
		CapturedFromHost:    r.capturedFromHost,
		StartedAt:           r.startedAt,
		FinishedAt:          time.Now(),
		Exchanges:           exchanges,
		RedactedHeaderNames: names,
	}
}

// WriteJSONFile snapshots the current fixture (via Fixture) and writes it to
// path. Convenience wrapper so cmd/protocol-load-worker's --capture-mode
// wiring doesn't need to call Fixture() itself.
func (r *LiveTrafficCaptureRecorder) WriteJSONFile(path string) error {
	return r.Fixture().WriteJSONFile(path)
}

// splitTargetURL splits a full request URL into its path+query (what
// CapturedExchange.Path stores) and its scheme://host:port authority (what
// ReplayableTrafficFixture.CapturedFromHost stores). Falls back to returning
// the raw targetURL as the path with an empty host if it doesn't parse as a
// URL (e.g. a bare gRPC "host:port" authority), so capture never drops an
// exchange just because it couldn't split it cleanly.
func splitTargetURL(targetURL string) (path, host string) {
	parsed, err := url.Parse(targetURL)
	if err != nil || parsed.Host == "" {
		return targetURL, ""
	}
	pathAndQuery := parsed.Path
	if parsed.RawQuery != "" {
		pathAndQuery += "?" + parsed.RawQuery
	}
	if pathAndQuery == "" {
		pathAndQuery = "/"
	}
	return pathAndQuery, parsed.Scheme + "://" + parsed.Host
}

var _ protocolclient.ProtocolClient = (*LiveTrafficCaptureRecorder)(nil)
