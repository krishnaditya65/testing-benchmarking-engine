package protocolclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RestHttpClient issues plain HTTP(S) request/response calls using
// net/http. It is the ProtocolClient implementation behind every REST DAG
// node.
type RestHttpClient struct {
	httpClient *http.Client
}

// NewRestHttpClient builds a RestHttpClient with a transport tuned for
// high-fan-out load generation: a large idle-connection pool so thousands
// of goroutines can reuse keep-alive connections instead of paying a new
// TCP/TLS handshake per request.
//
// NOTE: a production implementation would also set DialContext with a
// custom net.Dialer (control over local port range/keepalive), configure
// TLSClientConfig for mTLS between the worker and target when the DAG node
// specifies a client certificate, and wire httptrace.ClientTrace to split
// Latency into DNS/connect/TLS/TTFB phases for richer OTLP spans.
func NewRestHttpClient() *RestHttpClient {
	transport := &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 4096,
		MaxConnsPerHost:     0, // unbounded: concurrency is governed by ConcurrentLoadGenerator instead
		IdleConnTimeout:     90 * time.Second,
	}
	return &RestHttpClient{
		httpClient: &http.Client{Transport: transport},
	}
}

// Execute performs a single HTTP request built from req and returns a
// protocol-agnostic Response with timing captured around the round trip.
func (c *RestHttpClient) Execute(ctx context.Context, req Request) (Response, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, req.TargetURL, bytes.NewReader(req.Body))
	if err != nil {
		return Response{}, fmt.Errorf("protocolclient: building REST request for %s: %w", req.TargetURL, err)
	}
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	dispatchedAt := time.Now()
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return Response{
			Err: fmt.Errorf("protocolclient: REST call to %s: %w", req.TargetURL, err),
			Metrics: Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      time.Since(dispatchedAt),
				RequestBytes: len(req.Body),
				Protocol:     c.Protocol(),
			},
		}, err
	}
	defer httpResp.Body.Close()

	bodyBytes, readErr := io.ReadAll(httpResp.Body)
	latency := time.Since(dispatchedAt)
	if readErr != nil {
		wrapped := fmt.Errorf("protocolclient: reading REST response body from %s: %w", req.TargetURL, readErr)
		return Response{
			StatusCode: httpResp.StatusCode,
			Err:        wrapped,
			Metrics: Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      latency,
				RequestBytes: len(req.Body),
				Protocol:     c.Protocol(),
			},
		}, wrapped
	}

	headers := make(map[string]string, len(httpResp.Header))
	for key := range httpResp.Header {
		headers[key] = httpResp.Header.Get(key)
	}

	return Response{
		StatusCode: httpResp.StatusCode,
		Headers:    headers,
		Body:       bodyBytes,
		Metrics: Metrics{
			DispatchedAt:  dispatchedAt,
			Latency:       latency,
			RequestBytes:  len(req.Body),
			ResponseBytes: len(bodyBytes),
			Protocol:      c.Protocol(),
		},
	}, nil
}

// Protocol identifies this client as "rest".
func (c *RestHttpClient) Protocol() string { return "rest" }

// Close releases idle connections held by the underlying transport.
func (c *RestHttpClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
