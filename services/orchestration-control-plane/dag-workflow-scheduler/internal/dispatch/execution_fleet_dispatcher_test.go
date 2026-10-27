package dispatch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// countingFakeRedpandaWriter is a redpandaMessageWriter test double that
// records how many times WriteMessages was actually invoked, so tests can
// assert on whether a real produce reached "the broker" without needing one.
type countingFakeRedpandaWriter struct {
	mu               sync.Mutex
	writeCallCount   int
	lastWrittenMsgs  []kafka.Message
	writeMessagesErr error
}

func (w *countingFakeRedpandaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeCallCount++
	w.lastWrittenMsgs = msgs
	return w.writeMessagesErr
}

func (w *countingFakeRedpandaWriter) Close() error { return nil }

func newDispatcherUnderTestWithFakeWriter() (*RedpandaExecutionFleetDispatcher, *countingFakeRedpandaWriter) {
	fakeWriter := &countingFakeRedpandaWriter{}
	dispatcher := &RedpandaExecutionFleetDispatcher{
		writer:     fakeWriter,
		dedupStore: NewInMemoryDispatchDeduplicationStore(),
		dedupTTL:   DefaultDispatchIdempotencyTTL,
	}
	return dispatcher, fakeWriter
}

// TestDispatchNode_DuplicateIdempotencyKeySuppressedFromReachingBroker proves
// the core idempotent-dispatch guarantee this package adds: calling
// DispatchNode twice with a message describing the same
// (RunID, Node.ID, AttemptGeneration) -- exactly what happens when a
// scheduler crash causes Temporal to re-invoke the same in-flight activity
// attempt -- must only ever reach the underlying Redpanda writer once. A
// 50k-VU load-test node dispatch must not be produced (and therefore run by
// the Execution Fleet) twice for what is logically one dispatch.
func TestDispatchNode_DuplicateIdempotencyKeySuppressedFromReachingBroker(t *testing.T) {
	dispatcher, fakeWriter := newDispatcherUnderTestWithFakeWriter()

	msg := NodeDispatchMessage{
		RunID: "run-7f3c9e",
		Node: domain.PipelineNode{
			ID:   "node-load-step-50k-vu",
			Type: domain.PipelineNodeTypeLoadStep,
		},
		DispatchedAtUnixMillis: time.Now().UnixMilli(),
		AttemptGeneration:      1,
	}

	if err := dispatcher.DispatchNode(context.Background(), msg); err != nil {
		t.Fatalf("first DispatchNode call: unexpected error: %v", err)
	}
	if err := dispatcher.DispatchNode(context.Background(), msg); err != nil {
		t.Fatalf("duplicate DispatchNode call: unexpected error (duplicates must be suppressed, not errored): %v", err)
	}
	// A third call, identical in every field, must still be suppressed --
	// this isn't a one-shot "only the second call is deduplicated" fluke.
	if err := dispatcher.DispatchNode(context.Background(), msg); err != nil {
		t.Fatalf("second duplicate DispatchNode call: unexpected error: %v", err)
	}

	fakeWriter.mu.Lock()
	gotWriteCallCount := fakeWriter.writeCallCount
	fakeWriter.mu.Unlock()

	if gotWriteCallCount != 1 {
		t.Fatalf("expected exactly 1 underlying WriteMessages call after 3 DispatchNode calls with the same idempotency key, got %d", gotWriteCallCount)
	}
}

// TestDispatchNode_NewAttemptGenerationIsNotSuppressed proves the dedup
// guard is scoped to a specific attempt, not the (RunID, Node.ID) pair alone
// -- a genuinely new attempt (e.g. the previous attempt failed outright
// rather than the dispatcher crashing mid-flight) must still be allowed to
// dispatch, otherwise a transient-failure retry would leave the node
// permanently stuck.
func TestDispatchNode_NewAttemptGenerationIsNotSuppressed(t *testing.T) {
	dispatcher, fakeWriter := newDispatcherUnderTestWithFakeWriter()

	baseMsg := NodeDispatchMessage{
		RunID: "run-attempt-scope",
		Node: domain.PipelineNode{
			ID:   "node-assertion",
			Type: domain.PipelineNodeTypeAssertion,
		},
		DispatchedAtUnixMillis: time.Now().UnixMilli(),
	}

	firstAttempt := baseMsg
	firstAttempt.AttemptGeneration = 1
	secondAttempt := baseMsg
	secondAttempt.AttemptGeneration = 2

	if err := dispatcher.DispatchNode(context.Background(), firstAttempt); err != nil {
		t.Fatalf("attempt 1 DispatchNode call: unexpected error: %v", err)
	}
	if err := dispatcher.DispatchNode(context.Background(), secondAttempt); err != nil {
		t.Fatalf("attempt 2 DispatchNode call: unexpected error: %v", err)
	}

	fakeWriter.mu.Lock()
	gotWriteCallCount := fakeWriter.writeCallCount
	fakeWriter.mu.Unlock()

	if gotWriteCallCount != 2 {
		t.Fatalf("expected 2 underlying WriteMessages calls for 2 distinct attempt generations, got %d", gotWriteCallCount)
	}
}

// TestDispatchNode_PropagatesUnderlyingWriteFailure ensures the dedup layer
// doesn't swallow real produce errors -- only exact-duplicate calls should
// short-circuit before reaching the writer.
func TestDispatchNode_PropagatesUnderlyingWriteFailure(t *testing.T) {
	dispatcher, fakeWriter := newDispatcherUnderTestWithFakeWriter()
	fakeWriter.writeMessagesErr = context.DeadlineExceeded

	msg := NodeDispatchMessage{
		RunID: "run-write-fails",
		Node: domain.PipelineNode{
			ID:   "node-1",
			Type: domain.PipelineNodeTypeSetup,
		},
		AttemptGeneration: 1,
	}

	if err := dispatcher.DispatchNode(context.Background(), msg); err == nil {
		t.Fatal("expected DispatchNode to propagate the underlying writer error, got nil")
	}
}
