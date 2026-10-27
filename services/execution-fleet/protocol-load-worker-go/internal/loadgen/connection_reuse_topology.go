// Package loadgen: this file defines *how many physical connections* a load
// phase is allowed to open, independent of how many logical requests it
// needs to issue concurrently.
//
// ConcurrentLoadGenerator's default behavior is one physical socket per
// virtual-user goroutine (a "goroutine-per-VU" topology): concurrency 5,000
// means 5,000 open connections. That is exactly wrong for load-testing
// anything sitting behind a CDN, an L7 load balancer, or an HTTP/2-aware
// origin, because it does not resemble how real clients behave. A real
// browser tab or mobile app multiplexes dozens to hundreds of logical
// requests over a small, capped number of physical HTTP/2 connections per
// origin (the browser opens a new connection only when the existing ones'
// stream capacity is exhausted, and stops well short of one-socket-per-
// request). A load generator that ignores this creates two false signals:
// it can trip a CDN/LB's per-IP connection-count abuse heuristics that real
// traffic never would, and it under-tests the thing that actually matters
// at scale — how the origin behaves under many multiplexed streams sharing
// few connections (HOL blocking within a stream, SETTINGS_MAX_CONCURRENT_STREAMS
// exhaustion, connection-level flow control).
//
// ConnectionReusePolicy is the seam between "how many logical requests does
// the TrafficProfile want in flight" (unchanged) and "how many physical
// connections should carry them" (new). Http2MultiplexedRequestScheduler
// (http2_multiplexed_request_scheduler.go) is the LoadGenerationStrategy
// that honors a ConnectionReusePolicy instead of assuming 1:1.
package loadgen

// ConnectionReusePolicy answers the two questions a connection-aware
// scheduler needs before it dials anything:
//
//  1. How many logical requests may share one physical connection at once
//     (bounded by both the policy and, in practice, the peer's own
//     SETTINGS_MAX_CONCURRENT_STREAMS)?
//  2. How many physical connections may be open to a single host at once,
//     regardless of how many logical requests are queued for it?
//
// Implementations must be pure/stateless value types — the scheduler reads
// these limits repeatedly while placing requests, never mutates the policy,
// and may share one policy value across many goroutines concurrently.
type ConnectionReusePolicy interface {
	// LogicalStreamsPerConnection caps how many concurrently in-flight
	// logical requests the scheduler will multiplex onto one physical
	// HTTP/2 (or HTTP/3/QUIC) connection before it either queues the next
	// request or opens another connection (subject to
	// MaxConnectionsPerHost).
	LogicalStreamsPerConnection() int

	// MaxConnectionsPerHost caps the number of physical connections the
	// scheduler will ever hold open to a single target host at once. Once
	// this many connections are open and all are at
	// LogicalStreamsPerConnection capacity, additional logical requests
	// queue for a free stream slot rather than opening connection N+1.
	MaxConnectionsPerHost() int

	// Name identifies the topology for logging/telemetry attribution,
	// mirroring how TrafficProfile.Name identifies a concurrency shape.
	Name() string
}

// Defaults chosen to model a real HTTP/2-capable browser tab against a
// single origin, not an arbitrary round number:
//   - defaultStreamsPerConnection (100) matches the SETTINGS_MAX_CONCURRENT_STREAMS
//     value most production HTTP/2 servers (nginx, envoy, most CDNs)
//     advertise by default, so a load phase at this setting exercises the
//     same stream-capacity ceiling a real client would actually hit.
//   - defaultConnectionsPerHost (6) matches the historical per-origin
//     connection cap browsers enforced under HTTP/1.1 and that most still
//     apply defensively even over HTTP/2, where in principle one
//     connection could carry everything.
const (
	defaultStreamsPerConnection = 100
	defaultConnectionsPerHost   = 6
)

// BrowserLikeMultiplexingPolicy is the ConnectionReusePolicy a load phase
// should select whenever the point of the test is to reproduce how real
// user traffic hits the target — which is the common case. It caps both the
// number of physical connections per host and the number of logical
// requests multiplexed onto each one, so N virtual users collapse onto a
// realistic handful of sockets instead of N sockets.
type BrowserLikeMultiplexingPolicy struct {
	// StreamsPerConnection overrides defaultStreamsPerConnection when > 0.
	StreamsPerConnection int

	// ConnectionsPerHost overrides defaultConnectionsPerHost when > 0.
	ConnectionsPerHost int
}

func (p BrowserLikeMultiplexingPolicy) LogicalStreamsPerConnection() int {
	if p.StreamsPerConnection > 0 {
		return p.StreamsPerConnection
	}
	return defaultStreamsPerConnection
}

func (p BrowserLikeMultiplexingPolicy) MaxConnectionsPerHost() int {
	if p.ConnectionsPerHost > 0 {
		return p.ConnectionsPerHost
	}
	return defaultConnectionsPerHost
}

func (p BrowserLikeMultiplexingPolicy) Name() string { return "browser-like-multiplexing" }

// OneSocketPerVirtualUserPolicy is the naive topology every goroutine-per-VU
// load generator (including this package's own ConcurrentLoadGenerator)
// implicitly uses: every logical request gets its own physical connection,
// and there is no cap on connections per host beyond the concurrency level
// itself. It exists here as an explicit, nameable ConnectionReusePolicy —
// not because Http2MultiplexedRequestScheduler should normally be run this
// way (doing so defeats the point of using it over ConcurrentLoadGenerator),
// but so a load-test plan can deliberately A/B "realistic multiplexed load"
// against "naive one-socket-per-VU load" against the same target using the
// same scheduler code path, to measure how much the multiplexing topology
// itself changes the target's observed behavior.
type OneSocketPerVirtualUserPolicy struct{}

func (OneSocketPerVirtualUserPolicy) LogicalStreamsPerConnection() int { return 1 }

// MaxConnectionsPerHost returns 0, which Http2MultiplexedRequestScheduler
// treats as "unbounded" — matching ConcurrentLoadGenerator's behavior of
// letting the worker/concurrency count alone determine connection count.
func (OneSocketPerVirtualUserPolicy) MaxConnectionsPerHost() int { return 0 }

func (OneSocketPerVirtualUserPolicy) Name() string { return "one-socket-per-virtual-user" }
