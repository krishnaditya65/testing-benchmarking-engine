// Package protocolclient defines the universal protocol-client boundary used
// by the load generator and pipeline executor to talk to whatever system a
// DAG test node targets (REST, GraphQL, gRPC, WebSocket). Every concrete
// client in this package is an I/O boundary: it is the only place in the
// service that opens a socket or dials a peer.
package protocolclient

import (
	"context"
	"time"
)

// Request is the protocol-agnostic description of a single call a
// ConcurrentLoadGenerator worker goroutine issues against a target. Fields
// that don't apply to a given protocol are simply left zero-valued; each
// client implementation interprets the subset it understands.
type Request struct {
	// TargetURL is the fully-qualified endpoint: an http(s):// URL for
	// REST/GraphQL, a ws(s):// URL for WebSocket, or a host:port authority
	// for gRPC.
	TargetURL string

	// Method is the HTTP verb for REST requests. Ignored by other
	// protocols.
	Method string

	// Headers are protocol-level metadata: HTTP headers for REST/GraphQL,
	// or gRPC metadata pairs. WebSocket clients may use these as the
	// initial handshake headers.
	Headers map[string]string

	// Body is the raw request payload for REST calls, or the WebSocket
	// message to send. GraphQL requests build their own body from Query
	// and Variables instead.
	Body []byte

	// GraphQLQuery and GraphQLVariables are used only by GraphQlClient.
	GraphQLQuery     string
	GraphQLVariables map[string]any

	// StreamMessages are the ordered payloads a GrpcStreamClient or
	// WebSocketClient sends after the connection/stream is established.
	// A REST/GraphQL client ignores this field.
	StreamMessages [][]byte

	// Timeout bounds the whole call, including connection setup.
	Timeout time.Duration
}

// Response is the protocol-agnostic outcome of executing a Request. Callers
// (the pipeline executor, the load generator's metrics aggregator) only
// depend on this shape, never on protocol-specific response types.
type Response struct {
	// StatusCode is the HTTP status for REST/GraphQL, the gRPC status
	// code cast to int for gRPC, or 101/1000 style codes for WebSocket
	// upgrade/close outcomes.
	StatusCode int

	// Headers carries response headers/trailers when the protocol has
	// them.
	Headers map[string]string

	// Body is the raw response payload. For streaming protocols this is
	// the last message received, while StreamMessages holds the full
	// sequence.
	Body []byte

	// StreamMessages holds every message received on a stream, in
	// arrival order.
	StreamMessages [][]byte

	// Metrics captures the timing/size data OTLP telemetry export cares
	// about.
	Metrics Metrics

	// Err is non-nil when the call failed at the protocol level (dial
	// failure, non-2xx status treated as failure by the caller, stream
	// reset, etc). Execute still returns Err as the function's error
	// return; it is duplicated here so a Response can be queued onto a
	// results channel without losing the error.
	Err error
}

// Metrics captures per-call timing and size data. The load generator
// aggregates these across every call in a run, and the pipeline executor
// forwards them to the OTel exporter as span/metric attributes.
//
// NOTE: a production implementation would also record TLS handshake time,
// DNS resolution time, and TTFB separately (net/http/httptrace.ClientTrace
// exposes hooks for all of these); this starter keeps a single wall-clock
// Latency for simplicity.
type Metrics struct {
	// DispatchedAt is when the client began executing the request.
	DispatchedAt time.Time

	// Latency is the total wall-clock time from dispatch to the final
	// byte of the response (or stream close).
	Latency time.Duration

	// RequestBytes and ResponseBytes size the payloads for throughput
	// accounting.
	RequestBytes  int
	ResponseBytes int

	// Protocol names which ProtocolClient implementation produced this
	// Metrics value ("rest", "graphql", "grpc-stream", "websocket").
	Protocol string
}

// ProtocolClient is the single interface every concrete protocol
// implementation satisfies. The load generator and pipeline executor code
// against this interface only — they never import net/http, grpc, or
// gorilla/websocket directly, which keeps protocol-specific dial/transport
// logic isolated to the files in this package.
type ProtocolClient interface {
	// Execute issues one Request and blocks until a Response (or error)
	// is available. Implementations must respect ctx cancellation and
	// Request.Timeout, whichever is shorter.
	Execute(ctx context.Context, req Request) (Response, error)

	// Protocol returns a short, stable name identifying the protocol
	// this client speaks (e.g. "rest", "graphql", "grpc-stream",
	// "websocket"). Used as a metric/span attribute.
	Protocol() string

	// Close releases any pooled connections/transports held by the
	// client. Safe to call multiple times.
	Close() error
}
