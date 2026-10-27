// Package dispatch publishes DAG node work orders onto the message bus that
// the Execution Fleet layer (e.g. services/execution-fleet/protocol-load-worker-go)
// consumes. The control plane never talks to workers directly; it only ever
// produces onto Redpanda per ARCHITECTURE.md's "Control plane -> workers"
// contract.
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
)

// NodeDispatchTopic is the Redpanda (Kafka-API-compatible) topic that
// carries dispatch messages to the Execution Fleet, per ARCHITECTURE.md.
const NodeDispatchTopic = "dag.node.dispatch.v1"

// NodeDispatchMessage is the wire payload published to NodeDispatchTopic for
// a single PipelineNode dispatch.
//
// NOTE: this struct is a local stand-in. The long-term source of truth is
// shared-libraries/protocol-contracts/proto/dag_dispatch.proto, which does
// not exist in this repo yet. Once it lands, this struct (and its JSON
// marshaling below) should be replaced with generated protobuf types and a
// binary/protobuf producer payload instead of JSON.
type NodeDispatchMessage struct {
	RunID                  string              `json:"runId"`
	Node                   domain.PipelineNode `json:"node"`
	DispatchedAtUnixMillis int64               `json:"dispatchedAtUnixMillis"`
	// AttemptGeneration is the dispatching activity's attempt number (see
	// activity.GetInfo(ctx).Attempt in internal/workflow/activities.go). It
	// feeds DeriveDispatchIdempotencyKey so that re-invoking the *same*
	// attempt (e.g. after a scheduler crash mid-flight) is recognized as a
	// duplicate of an already-published message, while a genuinely new
	// attempt is still allowed to dispatch. Zero-valued for callers that
	// don't track attempts, which simply collapses every call for a given
	// (RunID, Node.ID) pair into attempt "0" -- still safe, just less
	// precise about which crash/retry generation produced the dispatch.
	AttemptGeneration int32 `json:"attemptGeneration"`
}

// ExecutionFleetDispatcherPort is the outbound port the DAG workflow's
// activities depend on to hand a node off to the Execution Fleet. Modeling
// it as an interface keeps internal/workflow free of any direct Kafka/Redpanda
// dependency and makes activity unit tests possible without a broker.
type ExecutionFleetDispatcherPort interface {
	// DispatchNode publishes msg onto the message bus for execution-fleet
	// workers to pick up. It returns once the broker has acknowledged the
	// write (or an error describing why it could not).
	DispatchNode(ctx context.Context, msg NodeDispatchMessage) error
}

// redpandaMessageWriter is the narrow slice of *kafka.Writer's method set
// RedpandaExecutionFleetDispatcher actually depends on. Depending on this
// interface instead of *kafka.Writer directly lets this package's tests
// (see execution_fleet_dispatcher_test.go) substitute a fake writer and
// assert on how many times a produce was actually attempted -- in
// particular, that a deduplicated DispatchNode call never reaches the writer
// at all -- without standing up a real Redpanda broker. *kafka.Writer
// satisfies this interface already; production code is unaffected.
type redpandaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// RedpandaExecutionFleetDispatcher is the production ExecutionFleetDispatcherPort
// implementation. It produces JSON-encoded NodeDispatchMessage values onto
// NodeDispatchTopic using kafka-go's Writer, which speaks the Kafka wire
// protocol that Redpanda implements.
//
// Before producing, it consults dedupStore (see idempotent_dispatch_deduplicator.go)
// keyed by DeriveDispatchIdempotencyKey(msg.RunID, msg.Node.ID,
// msg.AttemptGeneration) so that re-dispatching the same
// (pipelineRunId, nodeId, attemptGeneration) tuple -- e.g. because the
// scheduler process crashed and Temporal re-invoked the same still-in-flight
// activity attempt -- publishes onto NodeDispatchTopic at most once. This is
// what makes DispatchNode idempotent-effect rather than merely
// at-least-once: without it, a 50k-VU load-test node could be produced (and
// therefore picked up and run by the Execution Fleet) twice for what is
// logically a single dispatch.
type RedpandaExecutionFleetDispatcher struct {
	writer     redpandaMessageWriter
	dedupStore DispatchDeduplicationStore
	dedupTTL   time.Duration
}

// NewRedpandaExecutionFleetDispatcher constructs a dispatcher whose producer
// is pinned to NodeDispatchTopic against the given broker addresses (e.g.
// []string{"dag-workflow-redpanda:9092"}). Idempotent-dispatch dedup is
// backed by an InMemoryDispatchDeduplicationStore with
// DefaultDispatchIdempotencyTTL -- correct for a single scheduler replica.
// Production deployments running more than one dag-workflow-scheduler
// replica should construct via NewRedpandaExecutionFleetDispatcherWithDeduplication
// instead, passing a shared (e.g. Redis-backed) DispatchDeduplicationStore
// so two replicas racing to handle the same retried activity still
// deduplicate correctly -- see this package's README for the operational
// note.
func NewRedpandaExecutionFleetDispatcher(brokerAddrs []string) *RedpandaExecutionFleetDispatcher {
	return NewRedpandaExecutionFleetDispatcherWithDeduplication(
		brokerAddrs,
		NewInMemoryDispatchDeduplicationStore(),
		DefaultDispatchIdempotencyTTL,
	)
}

// NewRedpandaExecutionFleetDispatcherWithDeduplication is like
// NewRedpandaExecutionFleetDispatcher but takes explicit control of the
// DispatchDeduplicationStore and TTL used to suppress duplicate dispatches,
// so production wiring (cmd/scheduler/main.go) can supply a store shared
// across replicas instead of the package-default in-memory one.
func NewRedpandaExecutionFleetDispatcherWithDeduplication(
	brokerAddrs []string,
	dedupStore DispatchDeduplicationStore,
	dedupTTL time.Duration,
) *RedpandaExecutionFleetDispatcher {
	return &RedpandaExecutionFleetDispatcher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddrs...),
			Topic:                  NodeDispatchTopic,
			Balancer:               &kafka.Hash{}, // key on RunID so a run's nodes land consistently
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne,
		},
		dedupStore: dedupStore,
		dedupTTL:   dedupTTL,
	}
}

// DispatchNode implements ExecutionFleetDispatcherPort by JSON-marshaling
// msg and writing it to NodeDispatchTopic, keyed by RunID so all dispatches
// for one pipeline run are ordered relative to each other within a
// partition.
//
// Before producing, it checks dedupStore for msg's derived idempotency key.
// A duplicate (recordedNow == false) is treated as a successful no-op: the
// original call already published this exact (run, node, attempt) dispatch,
// so silently returning nil is the correct idempotent-effect behavior rather
// than an error.
func (d *RedpandaExecutionFleetDispatcher) DispatchNode(ctx context.Context, msg NodeDispatchMessage) error {
	idempotencyKey := DeriveDispatchIdempotencyKey(msg.RunID, msg.Node.ID, msg.AttemptGeneration)

	recordedNow, err := d.dedupStore.MarkDispatchedIfAbsent(ctx, idempotencyKey, d.dedupTTL)
	if err != nil {
		return fmt.Errorf("dispatch: check dispatch idempotency for node %q, run %q, attempt %d: %w", msg.Node.ID, msg.RunID, msg.AttemptGeneration, err)
	}
	if !recordedNow {
		log.Printf("dispatch: suppressing duplicate dispatch of node %q for run %q (attempt %d already dispatched, idempotency key %s)", msg.Node.ID, msg.RunID, msg.AttemptGeneration, idempotencyKey)
		return nil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dispatch: marshal node dispatch message for node %q: %w", msg.Node.ID, err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(msg.RunID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "node-id", Value: []byte(msg.Node.ID)},
			{Key: "node-type", Value: []byte(msg.Node.Type)},
		},
	}

	if err := d.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("dispatch: write node %q for run %q to topic %q: %w", msg.Node.ID, msg.RunID, NodeDispatchTopic, err)
	}
	return nil
}

// Close releases the underlying producer's network resources. Callers
// should invoke this during graceful shutdown of the scheduler process.
func (d *RedpandaExecutionFleetDispatcher) Close() error {
	if err := d.writer.Close(); err != nil {
		return fmt.Errorf("dispatch: close redpanda writer: %w", err)
	}
	return nil
}
