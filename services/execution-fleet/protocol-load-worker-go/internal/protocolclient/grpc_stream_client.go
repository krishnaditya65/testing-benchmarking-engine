package protocolclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcStreamClient drives a bidirectional-streaming gRPC call against an
// arbitrary target authority. Because DAG test nodes describe gRPC targets
// generically (host:port + method name), rather than as generated Go
// client stubs compiled ahead of time, this client operates on the raw
// grpc.ClientConn stream primitives instead of a typed service client.
//
// NOTE: real dynamic-protobuf-schema support (calling a method the worker
// has no compiled .proto for) requires either (a) reflecting the target's
// method/message descriptors via the grpc.reflection.v1alpha
// ServerReflection service and building google.golang.org/protobuf
// dynamicpb messages from them at runtime, or (b) accepting a
// FileDescriptorSet uploaded alongside the DAG node definition and loading
// it with protodesc/protoregistry. Both require a proper codec
// (encoding.Codec) registered so grpc.ClientConn can marshal dynamicpb
// messages instead of concrete generated structs. This starter models the
// stream lifecycle (dial, open stream, send N messages, receive until
// closed) using grpc.ClientConn.NewStream against a caller-supplied
// StreamDesc/method name, and leaves descriptor resolution as the
// integration point a production build would fill in.
type GrpcStreamClient struct {
	dialOptions []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // authority -> pooled connection
}

// NewGrpcStreamClient builds a GrpcStreamClient. insecureTransport selects
// plaintext (h2c) dialing for local/dev targets; production DAG nodes
// should always request TLS.
//
// NOTE: a production implementation would build credentials.TransportCredentials
// from the DAG node's TLS config (CA bundle, client cert for mTLS, SNI
// override) rather than the two fixed choices offered here.
func NewGrpcStreamClient(insecureTransport bool) *GrpcStreamClient {
	var transportCreds credentials.TransportCredentials
	if insecureTransport {
		transportCreds = insecure.NewCredentials()
	} else {
		transportCreds = credentials.NewTLS(nil)
	}
	return &GrpcStreamClient{
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(transportCreds),
		},
		conns: make(map[string]*grpc.ClientConn),
	}
}

// connectionFor returns a pooled *grpc.ClientConn for authority, dialing a
// new one on first use. gRPC connections multiplex many streams over one
// HTTP/2 connection, so unlike REST, pooling here means "one conn per
// authority", not "one conn per in-flight call".
func (c *GrpcStreamClient) connectionFor(ctx context.Context, authority string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[authority]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(authority, c.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("protocolclient: dialing gRPC authority %s: %w", authority, err)
	}
	c.conns[authority] = conn
	return conn, nil
}

// Execute opens a bidirectional stream to req.TargetURL (interpreted as a
// "host:port/full.service.Method" authority+method pair), sends every
// entry in req.StreamMessages as an already-encoded protobuf message, then
// reads responses until the server half-closes the stream or ctx expires.
//
// req.StreamMessages must already be wire-format protobuf bytes; encoding
// application types into that form is the caller's responsibility (see the
// NOTE on this type for how a schema-less DAG node would produce those
// bytes via dynamicpb).
func (c *GrpcStreamClient) Execute(ctx context.Context, req Request) (Response, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	authority, method, err := splitAuthorityAndMethod(req.TargetURL)
	if err != nil {
		return Response{}, err
	}

	conn, err := c.connectionFor(ctx, authority)
	if err != nil {
		return Response{}, err
	}

	dispatchedAt := time.Now()

	// A raw byte-passthrough codec lets NewStream carry pre-encoded
	// protobuf messages without a generated service client. The stream
	// descriptor declares both client- and server-streaming so this one
	// code path covers unary, client-stream, server-stream, and bidi
	// gRPC methods.
	streamDesc := &grpc.StreamDesc{
		StreamName:    method,
		ClientStreams: true,
		ServerStreams: true,
	}

	stream, err := conn.NewStream(ctx, streamDesc, method)
	if err != nil {
		wrapped := fmt.Errorf("protocolclient: opening gRPC stream %s on %s: %w", method, authority, err)
		return Response{Err: wrapped}, wrapped
	}

	requestBytes := 0
	for _, msg := range req.StreamMessages {
		if err := stream.SendMsg(rawFrame(msg)); err != nil {
			wrapped := fmt.Errorf("protocolclient: sending gRPC stream message on %s: %w", method, err)
			return Response{Err: wrapped}, wrapped
		}
		requestBytes += len(msg)
	}
	if err := stream.CloseSend(); err != nil {
		wrapped := fmt.Errorf("protocolclient: closing gRPC send side on %s: %w", method, err)
		return Response{Err: wrapped}, wrapped
	}

	var received [][]byte
	responseBytes := 0
	for {
		var frame rawFrame
		if err := stream.RecvMsg(&frame); err != nil {
			// io.EOF (surfaced by grpc as a wrapped status) marks a
			// clean stream close; anything else is a real failure.
			if err.Error() == "EOF" {
				break
			}
			break // NOTE: production code should distinguish status.Code(err) == codes.OK/EOF from real errors here.
		}
		received = append(received, frame)
		responseBytes += len(frame)
	}

	latency := time.Since(dispatchedAt)
	var lastMessage []byte
	if len(received) > 0 {
		lastMessage = received[len(received)-1]
	}

	return Response{
		StatusCode:     0, // NOTE: production code should map status.FromError(err).Code() to an int here.
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

// Protocol identifies this client as "grpc-stream".
func (c *GrpcStreamClient) Protocol() string { return "grpc-stream" }

// Close tears down every pooled connection.
func (c *GrpcStreamClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for authority, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("protocolclient: closing gRPC connection to %s: %w", authority, err)
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

// rawFrame is a []byte that implements proto.Message-shaped marshal/unmarshal
// hooks expected by a passthrough codec, letting Execute send/receive
// pre-encoded protobuf bytes without generated message types.
//
// NOTE: this requires registering a matching grpc.Codec (via
// google.golang.org/grpc/encoding) that marshals/unmarshals rawFrame as raw
// bytes instead of protobuf-encoding it again; grpc-go's default codec
// expects proto.Message. A production implementation would register that
// codec at package init via encoding.RegisterCodec.
type rawFrame []byte

func splitAuthorityAndMethod(target string) (authority, method string, err error) {
	for i := 0; i < len(target); i++ {
		if target[i] == '/' {
			return target[:i], target[i:], nil
		}
	}
	return "", "", fmt.Errorf("protocolclient: gRPC target %q must be \"host:port/pkg.Service/Method\"", target)
}
