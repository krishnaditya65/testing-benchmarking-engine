package loadgen

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/protocolclient"
)

// LoadGenerationStrategy is the selectable-strategy seam requested for load
// generation: any type that can drive one TrafficProfile phase to
// completion and report RunMetrics satisfies it. ConcurrentLoadGenerator
// (concurrent_load_generator.go, goroutine-per-virtual-user, one physical
// connection per worker) and Http2MultiplexedRequestScheduler (this file,
// many logical requests sharing a capped pool of physical connections) are
// the two implementations shipped in this package; cmd/protocol-load-worker
// selects between them per load phase (e.g. via a `--load-strategy` flag)
// rather than one replacing the other, because goroutine-per-VU is still the
// right choice for protocols/targets where connection multiplexing doesn't
// apply (WebSocket, gRPC streams, or deliberately testing raw
// connection-exhaustion behavior).
type LoadGenerationStrategy interface {
	// Run executes one load phase described by profile and returns its
	// aggregated RunMetrics, following the same checkpoint-before-return
	// contract ConcurrentLoadGenerator.Run documents.
	Run(ctx context.Context, profile TrafficProfile) (RunMetrics, error)

	// StrategyName identifies which strategy produced a given RunMetrics,
	// for logging/telemetry attribution alongside TrafficProfile.Name.
	StrategyName() string
}

// StrategyName on ConcurrentLoadGenerator makes it satisfy
// LoadGenerationStrategy alongside Http2MultiplexedRequestScheduler, so
// callers can hold a single LoadGenerationStrategy value and not care which
// concrete topology is behind it.
func (g *ConcurrentLoadGenerator) StrategyName() string { return "goroutine-per-virtual-user" }

// Http2MultiplexedRequestScheduler is the LoadGenerationStrategy that models
// real multiplexing clients: it holds open a small, ConnectionReusePolicy-
// bounded pool of physical HTTP/2 connections per target host and schedules
// every logical request from RequestFactory onto whichever pooled
// connection currently has spare stream capacity, opening a new physical
// connection only when every pooled connection for that host is already at
// LogicalStreamsPerConnection capacity and MaxConnectionsPerHost allows one
// more.
//
// Scope: this scheduler speaks REST/GraphQL-shaped HTTP requests only (it
// builds and issues net/http requests directly, the same protocolclient.Request
// shape ConcurrentLoadGenerator's RequestFactory produces) — it does not
// wrap protocolclient.ProtocolClient, because connection-multiplexing
// topology is an HTTP/2-specific concept that doesn't have a meaningful
// analog for gRPC streams or long-lived WebSocket connections (those
// already are one logical "request" per connection by construction). A load
// phase that needs multiplexing-aware REST/GraphQL load selects this
// strategy; a phase driving gRPC/WebSocket stays on ConcurrentLoadGenerator.
//
// NOTE: HTTP/3 (QUIC) multiplexing is conceptually identical (many streams,
// few QUIC connections, per-connection stream caps) but requires a QUIC
// transport (e.g. quic-go/http3.RoundTripper) this starter does not vendor.
// The pooling/scheduling logic below is transport-agnostic — swapping
// newPooledTransport's http.Transport for an HTTP/3 RoundTripper is the only
// change an HTTP/3 build needs; see the NOTE on newPooledTransport.
type Http2MultiplexedRequestScheduler struct {
	RequestFactory  RequestFactory
	Policy          ConnectionReusePolicy
	CheckpointStore CheckpointStore
	RunID           string

	// SampleInterval controls how often the scheduler re-samples the
	// TrafficProfile and reconciles the live count of in-flight logical
	// requests against it, exactly like ConcurrentLoadGenerator.SampleInterval.
	// Defaults to 500ms if zero.
	SampleInterval time.Duration

	// RequestTimeout bounds each logical request when protocolclient.Request.Timeout
	// is left zero. Defaults to 30s if zero, matching RestHttpClient's default.
	RequestTimeout time.Duration

	// InsecureSkipVerify disables TLS certificate verification on pooled
	// connections. Exists only for load-testing targets with self-signed
	// certs in non-production environments; defaults to false.
	InsecureSkipVerify bool

	// Results, if non-nil, receives every CallResult as it completes, same
	// contract as ConcurrentLoadGenerator.Results.
	Results chan<- CallResult

	poolsMu sync.Mutex
	pools   map[string]*hostConnectionPool // keyed by request authority (scheme://host:port)
}

// hostConnectionPool is the bounded set of physical connections
// Http2MultiplexedRequestScheduler holds open to one target host, each
// wrapped in a streamSlot that limits how many logical requests are
// multiplexed onto it at once.
type hostConnectionPool struct {
	policy ConnectionReusePolicy

	mu    sync.Mutex
	slots []*streamSlot
}

// streamSlot is one physical connection's share of capacity: an *http.Client
// whose Transport is pinned to at most one physical connection
// (MaxConnsPerHost: 1) so every request routed through this slot genuinely
// multiplexes onto the same socket instead of net/http silently opening a
// sibling connection, plus a semaphore bounding concurrent in-flight
// requests to Policy.LogicalStreamsPerConnection.
type streamSlot struct {
	client    *http.Client
	inFlight  chan struct{} // buffered to LogicalStreamsPerConnection; acquire before Do, release after
	streamCap int
}

func (s *streamSlot) load() int { return len(s.inFlight) }

// newPooledTransport builds the *http.Client behind one streamSlot.
//
// NOTE: ForceAttemptHTTP2 plus MaxConnsPerHost: 1 is what makes this an
// *HTTP/2*-multiplexed slot: net/http negotiates h2 via TLS ALPN against any
// target that supports it and then satisfies every concurrent request
// through this client against the same physical connection (true stream
// multiplexing) rather than opening siblings, because MaxConnsPerHost caps
// the transport at exactly one connection for this host. Cleartext h2c
// targets need golang.org/x/net/http2's h2c.Transport wrapped around this
// instead — swap it in here without changing streamSlot's shape. An HTTP/3
// build swaps the whole *http.Transport for a QUIC RoundTripper, since
// HTTP/3 doesn't dial via net.Dial/TLS ALPN at all.
func newPooledTransport(insecureSkipVerify bool) *http.Client {
	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxConnsPerHost:     1,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: insecureSkipVerify}, //nolint:gosec // opt-in via InsecureSkipVerify for test targets only
	}
	return &http.Client{Transport: transport}
}

// acquireSlot returns the least-loaded existing streamSlot for host if it
// has spare stream capacity, opens a new physical connection (streamSlot) if
// MaxConnectionsPerHost allows one more, or otherwise blocks on the
// least-loaded existing slot's capacity — i.e. it queues the logical request
// for a free stream rather than ever exceeding the policy's caps.
func (p *hostConnectionPool) acquireSlot(ctx context.Context, insecureSkipVerify bool) (*streamSlot, error) {
	p.mu.Lock()
	streamCap := p.policy.LogicalStreamsPerConnection()
	if streamCap <= 0 {
		streamCap = 1
	}
	maxConns := p.policy.MaxConnectionsPerHost()

	best := leastLoadedSlot(p.slots)
	if best == nil || (best.load() >= best.streamCap && (maxConns <= 0 || len(p.slots) < maxConns)) {
		best = &streamSlot{
			client:    newPooledTransport(insecureSkipVerify),
			inFlight:  make(chan struct{}, streamCap),
			streamCap: streamCap,
		}
		p.slots = append(p.slots, best)
	}
	p.mu.Unlock()

	select {
	case best.inFlight <- struct{}{}:
		return best, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func leastLoadedSlot(slots []*streamSlot) *streamSlot {
	var best *streamSlot
	for _, s := range slots {
		if best == nil || s.load() < best.load() {
			best = s
		}
	}
	return best
}

// releaseSlot frees the stream capacity acquireSlot reserved.
func releaseSlot(slot *streamSlot) { <-slot.inFlight }

// poolFor returns (creating if necessary) the hostConnectionPool for the
// authority parsed out of targetURL.
func (sched *Http2MultiplexedRequestScheduler) poolFor(targetURL string) (*hostConnectionPool, error) {
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("loadgen: parsing target URL %q for connection pooling: %w", targetURL, err)
	}
	authority := parsed.Scheme + "://" + parsed.Host

	sched.poolsMu.Lock()
	defer sched.poolsMu.Unlock()
	if sched.pools == nil {
		sched.pools = make(map[string]*hostConnectionPool)
	}
	pool, ok := sched.pools[authority]
	if !ok {
		pool = &hostConnectionPool{policy: sched.Policy}
		sched.pools[authority] = pool
	}
	return pool, nil
}

// Run drives profile exactly like ConcurrentLoadGenerator.Run — same
// SampleInterval reconciliation loop, same checkpoint-before-return
// contract — but every logical request goroutine issues its call through
// acquireSlot/releaseSlot instead of owning a dedicated connection, so the
// physical connection count converges on Policy's caps rather than on
// profile's target concurrency.
func (sched *Http2MultiplexedRequestScheduler) Run(ctx context.Context, profile TrafficProfile) (RunMetrics, error) {
	if sched.RequestFactory == nil {
		return RunMetrics{}, fmt.Errorf("loadgen: Http2MultiplexedRequestScheduler requires a non-nil RequestFactory")
	}
	policy := sched.Policy
	if policy == nil {
		policy = BrowserLikeMultiplexingPolicy{}
	}
	sched.Policy = policy

	sampleInterval := sched.SampleInterval
	if sampleInterval <= 0 {
		sampleInterval = 500 * time.Millisecond
	}
	requestTimeout := sched.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	checkpointStore := sched.CheckpointStore
	if checkpointStore == nil {
		checkpointStore = NoopCheckpointStore{}
	}

	phaseCtx, cancelPhase := context.WithTimeout(ctx, profile.Duration())
	defer cancelPhase()

	metrics := &runMetricsAccumulator{}
	var activeIssuers int64
	var nextIssuerID int64
	issuerStop := make(map[int64]chan struct{})
	var issuersMu sync.Mutex
	var issuersWG sync.WaitGroup

	spawnIssuer := func() {
		id := atomic.AddInt64(&nextIssuerID, 1)
		stop := make(chan struct{})
		issuersMu.Lock()
		issuerStop[id] = stop
		issuersMu.Unlock()
		atomic.AddInt64(&activeIssuers, 1)

		issuersWG.Add(1)
		go func() {
			defer issuersWG.Done()
			defer atomic.AddInt64(&activeIssuers, -1)
			sched.runIssuer(phaseCtx, int(id), stop, requestTimeout, metrics)
		}()
	}

	stopOneIssuer := func() {
		issuersMu.Lock()
		defer issuersMu.Unlock()
		for id, stop := range issuerStop {
			close(stop)
			delete(issuerStop, id)
			return
		}
	}

	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	startedAt := time.Now()
	lastConcurrency := 0

reconcileLoop:
	for {
		elapsed := time.Since(startedAt)
		target := profile.TargetConcurrency(elapsed)
		if target < 0 {
			target = 0
		}

		current := int(atomic.LoadInt64(&activeIssuers))
		for current < target {
			spawnIssuer()
			current++
		}
		for current > target {
			stopOneIssuer()
			current--
		}
		lastConcurrency = target

		select {
		case <-phaseCtx.Done():
			break reconcileLoop
		case <-ticker.C:
			continue
		}
	}

	issuersMu.Lock()
	for _, stop := range issuerStop {
		close(stop)
	}
	issuerStop = map[int64]chan struct{}{}
	issuersMu.Unlock()
	issuersWG.Wait()

	result := metrics.snapshot()

	checkpoint := PhaseCheckpoint{
		PhaseName:       profile.Name(),
		CompletedAt:     time.Now(),
		CallsCompleted:  result.TotalCalls,
		CallsSucceeded:  result.SuccessfulCalls,
		LastConcurrency: lastConcurrency,
	}
	if err := checkpointStore.SaveCheckpoint(ctx, sched.RunID, checkpoint); err != nil {
		return result, fmt.Errorf("loadgen: checkpointing phase %q before transition: %w", profile.Name(), err)
	}

	return result, nil
}

// StrategyName identifies this LoadGenerationStrategy for logging/telemetry.
func (sched *Http2MultiplexedRequestScheduler) StrategyName() string {
	policy := sched.Policy
	if policy == nil {
		policy = BrowserLikeMultiplexingPolicy{}
	}
	return "http2-multiplexed:" + policy.Name()
}

// runIssuer is the body of one long-lived "logical request issuer"
// goroutine. Unlike ConcurrentLoadGenerator's worker, it does not own a
// connection — every loop iteration acquires a stream slot from the
// appropriate hostConnectionPool, issues the request, and releases the slot,
// so the physical connection it happens to land on can vary call to call as
// the pool's load balances across slots.
func (sched *Http2MultiplexedRequestScheduler) runIssuer(ctx context.Context, id int, stop chan struct{}, requestTimeout time.Duration, metrics *runMetricsAccumulator) {
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}

		req := sched.RequestFactory(ctx)
		resp := sched.executeMultiplexed(ctx, req, requestTimeout)
		metrics.record(resp)

		if sched.Results != nil {
			select {
			case sched.Results <- CallResult{Response: resp, WorkerID: id, Timestamp: time.Now()}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// executeMultiplexed issues one protocolclient.Request over a pooled,
// policy-bounded HTTP/2 connection and returns a protocol-agnostic Response,
// matching the shape ConcurrentLoadGenerator's protocolclient.ProtocolClient
// implementations return so both strategies' CallResults are interchangeable
// on the same Results channel.
func (sched *Http2MultiplexedRequestScheduler) executeMultiplexed(ctx context.Context, req protocolclient.Request, defaultTimeout time.Duration) protocolclient.Response {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pool, err := sched.poolFor(req.TargetURL)
	if err != nil {
		return protocolclient.Response{Err: err, Metrics: protocolclient.Metrics{Protocol: "http2-multiplexed"}}
	}

	slot, err := pool.acquireSlot(callCtx, sched.InsecureSkipVerify)
	if err != nil {
		return protocolclient.Response{
			Err:     fmt.Errorf("loadgen: acquiring pooled connection stream for %s: %w", req.TargetURL, err),
			Metrics: protocolclient.Metrics{Protocol: "http2-multiplexed"},
		}
	}
	defer releaseSlot(slot)

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	httpReq, err := http.NewRequestWithContext(callCtx, method, req.TargetURL, bytes.NewReader(req.Body))
	if err != nil {
		return protocolclient.Response{
			Err:     fmt.Errorf("loadgen: building multiplexed request for %s: %w", req.TargetURL, err),
			Metrics: protocolclient.Metrics{Protocol: "http2-multiplexed"},
		}
	}
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	dispatchedAt := time.Now()
	httpResp, err := slot.client.Do(httpReq)
	if err != nil {
		return protocolclient.Response{
			Err: fmt.Errorf("loadgen: multiplexed call to %s: %w", req.TargetURL, err),
			Metrics: protocolclient.Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      time.Since(dispatchedAt),
				RequestBytes: len(req.Body),
				Protocol:     "http2-multiplexed",
			},
		}
	}
	defer httpResp.Body.Close()

	bodyBytes, readErr := io.ReadAll(httpResp.Body)
	latency := time.Since(dispatchedAt)
	if readErr != nil {
		return protocolclient.Response{
			StatusCode: httpResp.StatusCode,
			Err:        fmt.Errorf("loadgen: reading multiplexed response body from %s: %w", req.TargetURL, readErr),
			Metrics: protocolclient.Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      latency,
				RequestBytes: len(req.Body),
				Protocol:     "http2-multiplexed",
			},
		}
	}

	headers := make(map[string]string, len(httpResp.Header))
	for key := range httpResp.Header {
		headers[key] = httpResp.Header.Get(key)
	}

	return protocolclient.Response{
		StatusCode: httpResp.StatusCode,
		Headers:    headers,
		Body:       bodyBytes,
		Metrics: protocolclient.Metrics{
			DispatchedAt:  dispatchedAt,
			Latency:       latency,
			RequestBytes:  len(req.Body),
			ResponseBytes: len(bodyBytes),
			Protocol:      "http2-multiplexed",
		},
	}
}
