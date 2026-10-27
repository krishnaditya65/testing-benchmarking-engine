// Package capture implements the "record real traffic, replay it later"
// half of load testing: a passive sidecar mode that observes request/response
// pairs already flowing through this worker's protocolclient implementations
// during a normal load phase or functional test run, and serializes them
// into a fixture another run (or another service — see
// services/execution-fleet/traffic-shadow-replay-worker/) can replay.
//
// This is deliberately *not* a network tap or proxy: LiveTrafficCaptureRecorder
// only ever sees what this process itself already sent/received via a
// protocolclient.ProtocolClient, so capture-mode adds zero extra network
// exposure beyond what the load phase was doing anyway.
package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// CapturedExchange is one recorded request/response pair, timed and shaped
// close to protocolclient.Request/protocolclient.Response but flattened and
// JSON-friendly (fixtures are meant to be read by humans and by
// traffic-shadow-replay-worker, a separate Go module that cannot import this
// package's types directly — see that service's
// internal/shadowing/replayable_traffic_fixture.go for the mirrored shape).
type CapturedExchange struct {
	// Protocol names which ProtocolClient implementation produced this
	// exchange ("rest", "graphql", "grpc-stream", "websocket"), matching
	// protocolclient.Response.Metrics.Protocol.
	Protocol string `json:"protocol"`

	// Method is the HTTP verb (REST/GraphQL) or empty for protocols
	// without one.
	Method string `json:"method,omitempty"`

	// Path is the request's path+query with scheme/host stripped, so a
	// replay run can point the same fixture at a different environment's
	// host without editing every exchange. The full TargetURL is not
	// retained for that reason.
	Path string `json:"path"`

	// RequestHeaders and ResponseHeaders have already been through
	// SensitiveHeaderRedactor by the time they reach a persisted fixture —
	// see LiveTrafficCaptureRecorder.Observe, which is the only writer of
	// CapturedExchange values in this package.
	RequestHeaders  map[string]string `json:"requestHeaders,omitempty"`
	ResponseHeaders map[string]string `json:"responseHeaders,omitempty"`

	// RequestBody and ResponseBody are the raw payload bytes. Bodies are
	// captured as-is (redaction only applies to headers — see the NOTE on
	// SensitiveHeaderRedactor for why body-level PII scrubbing is out of
	// scope here).
	RequestBody  []byte `json:"requestBody,omitempty"`
	ResponseBody []byte `json:"responseBody,omitempty"`

	// StatusCode is the response status (HTTP status for REST/GraphQL,
	// gRPC status cast to int, etc.), mirroring protocolclient.Response.
	StatusCode int `json:"statusCode"`

	// CapturedAt is when the request was dispatched.
	CapturedAt time.Time `json:"capturedAt"`

	// Latency is the observed round-trip time, preserved so a replay run's
	// response_shape_differ (in traffic-shadow-replay-worker) has a
	// same-environment baseline to compare against.
	Latency time.Duration `json:"latency"`
}

// ReplayableTrafficFixture is a self-contained, ordered recording of
// CapturedExchanges plus enough metadata to make a later replay run
// reproducible and attributable back to where it came from.
type ReplayableTrafficFixture struct {
	// FixtureName identifies this recording for humans (e.g.
	// "checkout-flow-prod-2026-08-11"). Not required to be unique on disk;
	// callers choosing a file name should incorporate SourceRunID instead.
	FixtureName string `json:"fixtureName"`

	// SourceRunID is the RunID of the load phase or test run this fixture
	// was captured from (see loadgen.ConcurrentLoadGenerator.RunID /
	// Http2MultiplexedRequestScheduler.RunID), for traceability back to the
	// DAG test run that produced it.
	SourceRunID string `json:"sourceRunID"`

	// CapturedFromHost is the scheme://host:port every Exchange.Path was
	// stripped from, recorded once here so a replay run knows what
	// environment the traffic shape originated from even though individual
	// exchanges are host-agnostic.
	CapturedFromHost string `json:"capturedFromHost"`

	// StartedAt and FinishedAt bound the capture window.
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`

	// Exchanges is the ordered list of recorded request/response pairs, in
	// the order they were observed.
	Exchanges []CapturedExchange `json:"exchanges"`

	// RedactedHeaderNames records which header names SensitiveHeaderRedactor
	// stripped from at least one exchange in this fixture, so a reader can
	// tell at a glance that redaction ran (and what it removed) without
	// diffing every exchange's headers.
	RedactedHeaderNames []string `json:"redactedHeaderNames,omitempty"`
}

// WriteJSONFile serializes the fixture as indented JSON to path, creating or
// truncating the file. Indented output is deliberate: fixtures are meant to
// be reviewed in a PR diff before a team trusts them as a shadow-replay
// traffic source, not just machine-read.
func (f ReplayableTrafficFixture) WriteJSONFile(path string) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("capture: marshaling fixture %q: %w", f.FixtureName, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("capture: writing fixture %q to %s: %w", f.FixtureName, path, err)
	}
	return nil
}

// ReadReplayableTrafficFixtureFile reads back a fixture written by
// WriteJSONFile. Exists alongside the writer so this package is a complete
// round-trip reference for the JSON shape traffic-shadow-replay-worker's
// mirrored type must match.
func ReadReplayableTrafficFixtureFile(path string) (ReplayableTrafficFixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ReplayableTrafficFixture{}, fmt.Errorf("capture: reading fixture file %s: %w", path, err)
	}
	var fixture ReplayableTrafficFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return ReplayableTrafficFixture{}, fmt.Errorf("capture: unmarshaling fixture file %s: %w", path, err)
	}
	return fixture, nil
}
