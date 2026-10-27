// Package targeting implements dependency-aware fault targeting: instead of choosing which pod
// to target for a chaos action purely by static label selector, it consults a real
// upstream/downstream service dependency graph (derived from trace data) so an experiment can
// guarantee it is exercising an actual failover path rather than an arbitrary label match.
//
// service_dependency_graph_client.go defines the graph model plus the port/adapter pair that
// fetches it. The graph is built from trace-derived edges: telemetry-query-api's
// cross-service-trace-correlation read (its README: "getCrossServiceTraceCorrelation(traceId) —
// reads raw_telemetry_events for every span sharing a trace id, across services, ordered by
// start time") returns, for one trace, every span across every service that participated in it.
// Because each span records its parent span, a parent span's service calling a child span's
// (different) service is direct evidence of a real "service A depends on service B" edge — this
// package aggregates that evidence across a sample of trace IDs into a ServiceDependencyGraph.
package targeting

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

// ServiceDependencyEdge is one observed "UpstreamService calls DownstreamService" relationship,
// derived from parent/child spans that cross a service boundary within a correlated trace.
type ServiceDependencyEdge struct {
	UpstreamService   string
	DownstreamService string
	// ObservedCallCount is how many sampled trace IDs contained at least one span pair
	// exhibiting this edge. Not a precise call-volume metric — it is evidence strength for "does
	// this edge exist at all", which is all dependency-aware targeting needs.
	ObservedCallCount int
}

// ServiceDependencyGraph is the aggregate result of correlating a sample of traces: every
// distinct cross-service caller/callee edge observed, plus which trace IDs contributed to it
// (useful for a human auditing why the graph believes an edge exists).
type ServiceDependencyGraph struct {
	Edges             []ServiceDependencyEdge
	BuiltFromTraceIDs []string
}

// DirectDependents returns the distinct services that directly call service (i.e. service is
// their downstream dependency), in sorted order for deterministic output.
func (g ServiceDependencyGraph) DirectDependents(service string) []string {
	seen := make(map[string]struct{})
	for _, edge := range g.Edges {
		if edge.DownstreamService == service {
			seen[edge.UpstreamService] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// DirectDependencies returns the distinct services that service directly calls (i.e. service's
// downstream dependencies), in sorted order.
func (g ServiceDependencyGraph) DirectDependencies(service string) []string {
	seen := make(map[string]struct{})
	for _, edge := range g.Edges {
		if edge.UpstreamService == service {
			seen[edge.DownstreamService] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// ServicesWithAtLeastNDirectDependents returns every service in the graph that has at least
// minDependents distinct direct dependents (callers) — the set of services safe to target under
// DependencyAwarePodSelectionStrategy's "only target services with real upstream callers"
// eligibility rule. Sorted for deterministic output.
func (g ServiceDependencyGraph) ServicesWithAtLeastNDirectDependents(minDependents int) []string {
	downstreamServices := make(map[string]struct{})
	for _, edge := range g.Edges {
		downstreamServices[edge.DownstreamService] = struct{}{}
	}

	eligible := make(map[string]struct{})
	for service := range downstreamServices {
		if len(g.DirectDependents(service)) >= minDependents {
			eligible[service] = struct{}{}
		}
	}
	return sortedKeys(eligible)
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ServiceDependencyGraphClient is the port between this package's dependency-aware targeting
// and the actual telemetry backend a dependency graph is derived from.
type ServiceDependencyGraphClient interface {
	// FetchDependencyGraph correlates seedTraceIDs (recent trace IDs sampled for the service(s)
	// a chaos experiment cares about) into a ServiceDependencyGraph. Implementations should
	// treat an empty/not-found trace as evidence contributing nothing (not an error) — a chaos
	// experiment's dependency graph is inherently a sample, not a complete picture.
	FetchDependencyGraph(ctx context.Context, seedTraceIDs []string) (ServiceDependencyGraph, error)
}

// traceSpan is the assumed shape of one element in telemetry-query-api's
// getCrossServiceTraceCorrelation(traceId) response — a raw_telemetry_events row, per that
// repository's README ("reads raw_telemetry_events for every span sharing a trace id, across
// services, ordered by start time").
//
// NOTE: telemetry-query-api's controller does not expose a documented field-level JSON contract
// in this pass (its README describes the query, not the HTTP response schema) — the field names
// below are this adapter's best-effort assumption of the natural raw_telemetry_events shape
// (trace/span/parent-span identifiers plus service name and start time). Reconciling this
// struct against the real response shape once telemetry-query-api's controller ships a
// documented DTO is real-backend work left for when this adapter is actually deployed, matching
// every other "seam documented, real wiring deferred" boundary in this service (see e.g.
// chaos.KernelNetworkFaultApplier's NOTE comment).
type traceSpan struct {
	TraceID      string    `json:"traceId"`
	SpanID       string    `json:"spanId"`
	ParentSpanID string    `json:"parentSpanId"`
	ServiceName  string    `json:"serviceName"`
	StartTime    time.Time `json:"startTime"`
}

// HTTPServiceDependencyGraphClient is a stub HTTP-backed ServiceDependencyGraphClient adapter
// that calls telemetry-query-api's cross-service-trace-correlation endpoint once per seed trace
// ID and aggregates the resulting parent/child, cross-service span pairs into dependency edges.
type HTTPServiceDependencyGraphClient struct {
	// BaseURL is telemetry-query-api's base HTTP address, e.g. "http://telemetry-query-api:3000".
	BaseURL string
	// HTTPClient is the client used to issue requests. A nil HTTPClient defaults to
	// http.DefaultClient at construction time via NewHTTPServiceDependencyGraphClient.
	HTTPClient *http.Client
}

// NewHTTPServiceDependencyGraphClient constructs the adapter. Passing a nil httpClient selects
// http.DefaultClient.
func NewHTTPServiceDependencyGraphClient(baseURL string, httpClient *http.Client) (*HTTPServiceDependencyGraphClient, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("new http service dependency graph client: baseURL is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HTTPServiceDependencyGraphClient{BaseURL: baseURL, HTTPClient: httpClient}, nil
}

// FetchDependencyGraph implements ServiceDependencyGraphClient. For each seed trace ID it GETs
// telemetry-query-api's trace-correlation route, decodes the span list, and derives one edge
// per (parent span, child span) pair whose ServiceName differs — a same-service parent/child
// pair is an internal call, not a cross-service dependency, and is skipped.
func (c *HTTPServiceDependencyGraphClient) FetchDependencyGraph(ctx context.Context, seedTraceIDs []string) (ServiceDependencyGraph, error) {
	if len(seedTraceIDs) == 0 {
		return ServiceDependencyGraph{}, fmt.Errorf("fetch dependency graph: seedTraceIDs must not be empty")
	}

	edgeCounts := make(map[ServiceDependencyEdge]int)
	contributingTraceIDs := make([]string, 0, len(seedTraceIDs))

	for _, traceID := range seedTraceIDs {
		spans, err := c.fetchTraceSpans(ctx, traceID)
		if err != nil {
			return ServiceDependencyGraph{}, fmt.Errorf("fetch dependency graph: trace %s: %w", traceID, err)
		}
		if len(spans) == 0 {
			continue
		}

		spanByID := make(map[string]traceSpan, len(spans))
		for _, span := range spans {
			spanByID[span.SpanID] = span
		}

		observedEdgeInThisTrace := false
		for _, span := range spans {
			if span.ParentSpanID == "" {
				continue
			}
			parent, ok := spanByID[span.ParentSpanID]
			if !ok || parent.ServiceName == "" || span.ServiceName == "" || parent.ServiceName == span.ServiceName {
				continue
			}
			edge := ServiceDependencyEdge{UpstreamService: parent.ServiceName, DownstreamService: span.ServiceName}
			edgeCounts[edge]++
			observedEdgeInThisTrace = true
		}
		if observedEdgeInThisTrace {
			contributingTraceIDs = append(contributingTraceIDs, traceID)
		}
	}

	edges := make([]ServiceDependencyEdge, 0, len(edgeCounts))
	for edge, count := range edgeCounts {
		edges = append(edges, ServiceDependencyEdge{
			UpstreamService:   edge.UpstreamService,
			DownstreamService: edge.DownstreamService,
			ObservedCallCount: count,
		})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].UpstreamService != edges[j].UpstreamService {
			return edges[i].UpstreamService < edges[j].UpstreamService
		}
		return edges[i].DownstreamService < edges[j].DownstreamService
	})

	return ServiceDependencyGraph{Edges: edges, BuiltFromTraceIDs: contributingTraceIDs}, nil
}

func (c *HTTPServiceDependencyGraphClient) fetchTraceSpans(ctx context.Context, traceID string) ([]traceSpan, error) {
	requestURL := fmt.Sprintf("%s/trace-correlation/%s", c.BaseURL, traceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// A trace ID with no correlated spans is expected/benign sampling noise, not an error.
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("telemetry-query-api returned status %d: %s", resp.StatusCode, string(body))
	}

	var spans []traceSpan
	if err := json.NewDecoder(resp.Body).Decode(&spans); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return spans, nil
}
