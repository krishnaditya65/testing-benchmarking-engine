package protocolclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// graphQLEnvelope is the standard {query, variables} POST body GraphQL
// servers expect.
type graphQLEnvelope struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// graphQLErrorPayload mirrors the "errors" array the GraphQL spec defines,
// used only to detect application-level failures that still return HTTP
// 200.
type graphQLErrorPayload struct {
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// GraphQlClient executes GraphQL operations over HTTP POST. It reuses the
// same net/http transport pattern as RestHttpClient but always marshals
// req.GraphQLQuery/req.GraphQLVariables into the request body rather than
// using req.Body directly.
type GraphQlClient struct {
	httpClient *http.Client
}

// NewGraphQlClient builds a GraphQlClient with a pooled HTTP transport.
//
// NOTE: a production implementation would also support persisted queries
// (sending a query hash instead of full query text) and GraphQL-over-WS for
// subscription DAG nodes; this starter only covers request/response
// operations (queries and mutations).
func NewGraphQlClient() *GraphQlClient {
	transport := &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 4096,
		IdleConnTimeout:     90 * time.Second,
	}
	return &GraphQlClient{httpClient: &http.Client{Transport: transport}}
}

// Execute POSTs req.GraphQLQuery and req.GraphQLVariables as a JSON
// envelope to req.TargetURL and returns the raw response body. Callers
// (typically the pipeline executor's JSONPath extractor) are responsible
// for interpreting the GraphQL "data"/"errors" shape.
func (c *GraphQlClient) Execute(ctx context.Context, req Request) (Response, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	envelope := graphQLEnvelope{Query: req.GraphQLQuery, Variables: req.GraphQLVariables}
	bodyBytes, err := json.Marshal(envelope)
	if err != nil {
		return Response{}, fmt.Errorf("protocolclient: marshaling GraphQL envelope for %s: %w", req.TargetURL, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.TargetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return Response{}, fmt.Errorf("protocolclient: building GraphQL request for %s: %w", req.TargetURL, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	dispatchedAt := time.Now()
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return Response{
			Err: fmt.Errorf("protocolclient: GraphQL call to %s: %w", req.TargetURL, err),
			Metrics: Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      time.Since(dispatchedAt),
				RequestBytes: len(bodyBytes),
				Protocol:     c.Protocol(),
			},
		}, err
	}
	defer httpResp.Body.Close()

	respBytes, readErr := io.ReadAll(httpResp.Body)
	latency := time.Since(dispatchedAt)
	if readErr != nil {
		wrapped := fmt.Errorf("protocolclient: reading GraphQL response body from %s: %w", req.TargetURL, readErr)
		return Response{StatusCode: httpResp.StatusCode, Err: wrapped}, wrapped
	}

	// GraphQL can return HTTP 200 with an "errors" array on the body;
	// surface that as Response.Err so callers don't have to re-parse.
	var errPayload graphQLErrorPayload
	var callErr error
	if json.Unmarshal(respBytes, &errPayload) == nil && len(errPayload.Errors) > 0 {
		callErr = fmt.Errorf("protocolclient: GraphQL %s returned %d application error(s), first: %s",
			req.TargetURL, len(errPayload.Errors), errPayload.Errors[0].Message)
	}

	headers := make(map[string]string, len(httpResp.Header))
	for key := range httpResp.Header {
		headers[key] = httpResp.Header.Get(key)
	}

	return Response{
		StatusCode: httpResp.StatusCode,
		Headers:    headers,
		Body:       respBytes,
		Err:        callErr,
		Metrics: Metrics{
			DispatchedAt:  dispatchedAt,
			Latency:       latency,
			RequestBytes:  len(bodyBytes),
			ResponseBytes: len(respBytes),
			Protocol:      c.Protocol(),
		},
	}, callErr
}

// Protocol identifies this client as "graphql".
func (c *GraphQlClient) Protocol() string { return "graphql" }

// Close releases idle connections held by the underlying transport.
func (c *GraphQlClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
