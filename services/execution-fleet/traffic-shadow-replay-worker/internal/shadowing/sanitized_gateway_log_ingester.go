package shadowing

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SanitizedGatewayLogIngester is the alternative "Log-to-Test Generation"
// traffic source this service supports alongside TrafficMultiplierReplayer's
// primary path (a captured ReplayableTrafficFixture from
// protocol-load-worker-go's --capture-mode): instead of running a passive
// capture sidecar in front of the target, teams that already ship API
// gateway access logs (ALB, Envoy, nginx, etc.) can turn *those* into an
// equivalent ReplayableTrafficFixture and get the exact same replay/diff
// pipeline for free.
//
// "Sanitized" in the name is load-bearing: unlike protocol-load-worker-go's
// capture path, which redacts sensitive headers itself
// (internal/capture/sensitive_header_redactor.go), this ingester assumes the
// log source has *already* had PII/secrets scrubbed upstream (most API
// gateways' access logs don't even capture full header sets or bodies by
// default, so there's usually much less to redact than a full request/
// response capture — but whatever the log does contain is trusted as-is
// here). An ingester adapter that reads a raw, unsanitized gateway log
// stream and needs its own redaction pass should wrap its Read call with
// protocol-load-worker-go's SensitiveHeaderRedactor logic (duplicated here
// per the same cross-module NOTE as CapturedExchange) rather than skip
// redaction — this interface does not enforce that itself.
type SanitizedGatewayLogIngester interface {
	// IngestFixture parses gateway log lines from source and returns an
	// equivalent ReplayableTrafficFixture, ready for
	// TrafficMultiplierReplayer to replay exactly as if it had come from a
	// captured fixture file.
	IngestFixture(ctx context.Context, source io.Reader) (ReplayableTrafficFixture, error)
}

// ALBAccessLogIngester is a sketch adapter implementing
// SanitizedGatewayLogIngester for AWS Application Load Balancer access log
// lines (the space-delimited, partially-quoted format ALB writes to S3).
// Reference format (fields in order):
//
//	type time elb client:port target:port request_processing_time
//	target_processing_time response_processing_time elb_status_code
//	target_status_code received_bytes sent_bytes "request" "user_agent"
//	ssl_cipher ssl_protocol target_group_arn "trace_id" "domain_name"
//	"chosen_cert_arn" matched_rule_priority request_creation_time
//	"actions_executed" "redirect_url" "error_reason" "target:port_list"
//	"target_status_code_list" "classification" "classification_reason"
//
// NOTE — this is a sketch, not a spec-complete ALB log parser: it handles
// the common case (double-quoted fields with no embedded/escaped quotes,
// which is what ALB actually emits) via splitRespectingQuotes below, and
// only extracts the fields TrafficMultiplierReplayer/ResponseShapeDiffer
// need (timestamp, request line, status code, timing). A production
// implementation should also: validate the `type` field (http/https/h2/
// grpcs/ws/wss) and branch on it, surface parse errors per-line instead of
// aborting the whole ingest on the first malformed line, and add a sibling
// EnvoyAccessLogIngester for Envoy's JSON or default text access-log format
// (structurally simpler than ALB's since Envoy's default JSON format is
// already field-delimited) — left as follow-up since the point of this file
// is establishing the SanitizedGatewayLogIngester seam, not exhaustively
// implementing every gateway's log dialect.
type ALBAccessLogIngester struct {
	// FixtureName labels the resulting ReplayableTrafficFixture. Defaults
	// to "alb-access-log-ingest" if empty.
	FixtureName string
}

// IngestFixture reads newline-delimited ALB access log lines from source and
// converts each into a CapturedExchange.
func (a ALBAccessLogIngester) IngestFixture(ctx context.Context, source io.Reader) (ReplayableTrafficFixture, error) {
	fixtureName := a.FixtureName
	if fixtureName == "" {
		fixtureName = "alb-access-log-ingest"
	}

	fixture := ReplayableTrafficFixture{
		FixtureName: fixtureName,
		StartedAt:   time.Now(),
	}

	scanner := bufio.NewScanner(source)
	// ALB log lines (especially the quoted user-agent field) can exceed
	// bufio.Scanner's 64KiB default token limit; raise it generously.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNumber := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return fixture, ctx.Err()
		default:
		}
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		exchange, err := parseALBAccessLogLine(line)
		if err != nil {
			return fixture, fmt.Errorf("shadowing: parsing ALB access log line %d: %w", lineNumber, err)
		}
		fixture.Exchanges = append(fixture.Exchanges, exchange)
	}
	if err := scanner.Err(); err != nil {
		return fixture, fmt.Errorf("shadowing: reading ALB access log: %w", err)
	}

	fixture.FinishedAt = time.Now()
	return fixture, nil
}

// parseALBAccessLogLine converts one ALB access log line into a
// CapturedExchange, extracting only the fields this service's replay/diff
// pipeline consumes.
func parseALBAccessLogLine(line string) (CapturedExchange, error) {
	fields := splitRespectingQuotes(line)
	// Minimum field count to reach the elb_status_code (index 8) and the
	// quoted "request" field (index 11) this parser actually uses.
	const minFields = 12
	if len(fields) < minFields {
		return CapturedExchange{}, fmt.Errorf("expected at least %d space-delimited fields, got %d", minFields, len(fields))
	}

	capturedAt, err := time.Parse(time.RFC3339Nano, fields[1])
	if err != nil {
		// ALB's timestamp is always RFC3339Nano in practice; fall back to
		// "now" rather than failing the whole line over an unparseable
		// clock value, since the request-shape fields matter far more to
		// replay fidelity than the exact original timestamp.
		capturedAt = time.Now()
	}

	requestProcessing := parseALBTimingSeconds(fields[5])
	targetProcessing := parseALBTimingSeconds(fields[6])
	responseProcessing := parseALBTimingSeconds(fields[7])
	latency := requestProcessing + targetProcessing + responseProcessing

	statusCode, _ := strconv.Atoi(fields[8])

	method, path := parseALBRequestLine(fields[11])

	return CapturedExchange{
		Protocol:   "rest",
		Method:     method,
		Path:       path,
		StatusCode: statusCode,
		CapturedAt: capturedAt,
		Latency:    latency,
	}, nil
}

// parseALBTimingSeconds parses one of ALB's *_processing_time fields, which
// are fractional seconds (e.g. "0.001") or "-1" when not applicable (e.g.
// target_processing_time on a fixed-response action). "-1" and unparseable
// values both contribute zero to the summed Latency.
func parseALBTimingSeconds(field string) time.Duration {
	seconds, err := strconv.ParseFloat(field, 64)
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// parseALBRequestLine splits ALB's quoted "request" field — e.g.
// `GET https://example.com:443/checkout?step=2 HTTP/1.1` — into an HTTP
// method and a path+query with scheme/host stripped, matching
// CapturedExchange.Path's contract in replayable_traffic_fixture.go (host-
// agnostic, so TrafficMultiplierReplayer can point it at any TargetBaseURL).
func parseALBRequestLine(requestLine string) (method, path string) {
	parts := strings.Fields(requestLine)
	if len(parts) < 2 {
		return http.MethodGet, "/"
	}
	method = parts[0]
	rawURL := parts[1]

	// Strip scheme://host, keeping only path+query — the "://" search
	// avoids a full net/url.Parse just for this one substitution, and
	// tolerates the occasional malformed URL ALB has been known to log
	// without failing the whole line.
	if idx := strings.Index(rawURL, "://"); idx >= 0 {
		rest := rawURL[idx+3:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			path = rest[slash:]
		} else {
			path = "/"
		}
	} else {
		path = rawURL
	}
	if path == "" {
		path = "/"
	}
	return method, path
}

// splitRespectingQuotes splits an ALB access log line on spaces, treating a
// double-quoted run (e.g. `"GET /path HTTP/1.1"`) as a single field even
// though it contains spaces. Does not handle backslash-escaped quotes
// within a quoted field — ALB does not emit those in practice, but a
// hardened parser for other gateways' formats should.
func splitRespectingQuotes(line string) []string {
	var fields []string
	var current strings.Builder
	inQuotes := false

	for _, r := range line {
		switch {
		case r == '"':
			inQuotes = !inQuotes
		case r == ' ' && !inQuotes:
			if current.Len() > 0 {
				fields = append(fields, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		fields = append(fields, current.String())
	}
	return fields
}

var _ SanitizedGatewayLogIngester = ALBAccessLogIngester{}
