// Package shadowing implements this service's two feature-manifest bullets
// — "Production Traffic Shadowing" and "Log-to-Test Generation" — neither
// of which had an owning service before this one. See this service's
// README.md for the full feature-manifest mapping.
package shadowing

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// CapturedExchange and ReplayableTrafficFixture below are a deliberate,
// field-for-field mirror of protocol-load-worker-go's
// internal/capture/replayable_traffic_fixture.go.
//
// NOTE — why this type is duplicated instead of imported: protocol-load-worker-go
// and traffic-shadow-replay-worker are separate Go modules (separate go.mod
// files, per ARCHITECTURE.md's execution-fleet-is-independently-deployable
// split), and neither depends on the other — per ARCHITECTURE.md, workers
// only ever talk to each other indirectly, through the message bus / shared
// fixture files, never via direct Go imports across service boundaries. If
// these two services ever need to interoperate by importing each other's
// Go types directly (rather than by one writing a fixture file the other
// reads, which is the only coupling today), this shape should move to a new
// package under services/shared-libraries/ — analogous to how
// dag-domain-model and protocol-contracts already hold the cross-service
// TypeScript/Protobuf shapes — and both workers should import it from
// there instead of maintaining two hand-synced copies. Until that need is
// real, duplicating a ~100-line JSON-shaped type across two modules is
// cheaper than introducing a shared Go module for one type.
type CapturedExchange struct {
	Protocol        string            `json:"protocol"`
	Method          string            `json:"method,omitempty"`
	Path            string            `json:"path"`
	RequestHeaders  map[string]string `json:"requestHeaders,omitempty"`
	ResponseHeaders map[string]string `json:"responseHeaders,omitempty"`
	RequestBody     []byte            `json:"requestBody,omitempty"`
	ResponseBody    []byte            `json:"responseBody,omitempty"`
	StatusCode      int               `json:"statusCode"`
	CapturedAt      time.Time         `json:"capturedAt"`
	Latency         time.Duration     `json:"latency"`
}

// ReplayableTrafficFixture mirrors protocol-load-worker-go's type of the
// same name — see the NOTE above CapturedExchange for why. TrafficMultiplierReplayer
// (traffic_multiplier_replayer.go) is the reader of this shape;
// protocol-load-worker-go's internal/capture package is the writer.
type ReplayableTrafficFixture struct {
	FixtureName         string             `json:"fixtureName"`
	SourceRunID         string             `json:"sourceRunID"`
	CapturedFromHost    string             `json:"capturedFromHost"`
	StartedAt           time.Time          `json:"startedAt"`
	FinishedAt          time.Time          `json:"finishedAt"`
	Exchanges           []CapturedExchange `json:"exchanges"`
	RedactedHeaderNames []string           `json:"redactedHeaderNames,omitempty"`
}

// ReadReplayableTrafficFixtureFile reads a fixture written by
// protocol-load-worker-go's capture.LiveTrafficCaptureRecorder.WriteJSONFile
// (or any other producer that writes this same JSON shape — including
// SanitizedGatewayLogIngester's adapter, once it converts gateway log lines
// into fixture exchanges).
func ReadReplayableTrafficFixtureFile(path string) (ReplayableTrafficFixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ReplayableTrafficFixture{}, fmt.Errorf("shadowing: reading fixture file %s: %w", path, err)
	}
	var fixture ReplayableTrafficFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return ReplayableTrafficFixture{}, fmt.Errorf("shadowing: unmarshaling fixture file %s: %w", path, err)
	}
	return fixture, nil
}
