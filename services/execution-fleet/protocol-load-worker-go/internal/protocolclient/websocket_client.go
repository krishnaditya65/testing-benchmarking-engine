package protocolclient

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketClient dials a WebSocket endpoint, sends every message in the
// request (Request.Body first if set, then Request.StreamMessages in
// order), and collects everything the server sends back until the
// connection closes or the request context expires.
type WebSocketClient struct {
	dialer *websocket.Dialer
}

// NewWebSocketClient builds a WebSocketClient with handshake and
// keep-alive timeouts suited to load generation, where thousands of
// concurrent handshakes must not each block on the library default.
//
// NOTE: a production implementation would also expose
// dialer.TLSClientConfig for wss:// targets that need a custom CA/mTLS
// client cert, and would set EnableCompression based on the DAG node's
// per-message-deflate requirement.
func NewWebSocketClient() *WebSocketClient {
	return &WebSocketClient{
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
	}
}

// Execute dials req.TargetURL (a ws:// or wss:// URL), performs the
// send/receive loop described on WebSocketClient, and closes the
// connection cleanly before returning.
func (c *WebSocketClient) Execute(ctx context.Context, req Request) (Response, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	handshakeHeaders := make(http.Header, len(req.Headers))
	for key, value := range req.Headers {
		handshakeHeaders.Set(key, value)
	}

	dispatchedAt := time.Now()
	conn, httpResp, err := c.dialer.DialContext(ctx, req.TargetURL, handshakeHeaders)
	if err != nil {
		status := 0
		if httpResp != nil {
			status = httpResp.StatusCode
		}
		wrapped := fmt.Errorf("protocolclient: WebSocket dial to %s: %w", req.TargetURL, err)
		return Response{
			StatusCode: status,
			Err:        wrapped,
			Metrics: Metrics{
				DispatchedAt: dispatchedAt,
				Latency:      time.Since(dispatchedAt),
				Protocol:     c.Protocol(),
			},
		}, wrapped
	}
	defer conn.Close()

	// Close the connection the moment ctx is done even if a Read is
	// blocked, since gorilla/websocket's Conn has no context-aware read.
	go func() {
		<-ctx.Done()
		_ = conn.SetReadDeadline(time.Now())
	}()

	outbound := req.StreamMessages
	if len(req.Body) > 0 {
		outbound = append([][]byte{req.Body}, outbound...)
	}

	requestBytes := 0
	for _, msg := range outbound {
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			wrapped := fmt.Errorf("protocolclient: WebSocket send to %s: %w", req.TargetURL, err)
			return Response{Err: wrapped}, wrapped
		}
		requestBytes += len(msg)
	}

	var received [][]byte
	responseBytes := 0
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Normal close (server done, or our own ctx-triggered
			// deadline) ends the loop rather than failing the call.
			break
		}
		received = append(received, message)
		responseBytes += len(message)
	}

	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(2*time.Second))

	latency := time.Since(dispatchedAt)
	var lastMessage []byte
	if len(received) > 0 {
		lastMessage = received[len(received)-1]
	}

	status := 0
	if httpResp != nil {
		status = httpResp.StatusCode
	}

	return Response{
		StatusCode:     status,
		Body:           lastMessage,
		StreamMessages: received,
		Metrics: Metrics{
			DispatchedAt:  dispatchedAt,
			Latency:       latency,
			RequestBytes:  requestBytes,
			ResponseBytes: responseBytes,
			Protocol:      c.Protocol(),
		},
	}, nil
}

// Protocol identifies this client as "websocket".
func (c *WebSocketClient) Protocol() string { return "websocket" }

// Close is a no-op: WebSocketClient holds no long-lived resources of its
// own between calls (each Execute owns and closes its own connection).
func (c *WebSocketClient) Close() error { return nil }
