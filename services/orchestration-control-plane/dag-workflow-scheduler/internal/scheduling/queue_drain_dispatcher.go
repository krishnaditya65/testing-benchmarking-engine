// This file closes the gap ADR 0004's Consequences section documented and
// ARCHITECTURE.md's "Known scope boundaries" note repeated: WeightedFairShareQueue's
// fairness algorithm only matters if something actually drains it.
// QueueDrainDispatcher is that something -- see cmd/scheduler/main.go for the
// production wiring (a TenantRunQueue fed by the HTTP trigger handler, and a
// RunStarterPort backed by the Temporal client).
package scheduling

import (
	"context"
	"log"
	"time"
)

// RunHandle identifies a workflow execution that RunStarterPort.StartRun
// began. Opaque to QueueDrainDispatcher itself -- it's threaded straight
// through to the matching AwaitCompletion call, so a RunStarterPort
// implementation is free to put whatever it needs to look the run back up
// (e.g. a Temporal workflow ID + run ID pair) into it.
type RunHandle struct {
	WorkflowID string
	RunID      string
}

// RunStarterPort is what QueueDrainDispatcher hands each run off to once
// TenantRunQueue.DispatchNext has admitted it past fairness and concurrency
// checks. Splitting "start" from "await completion" -- rather than one
// blocking call -- lets StartRun return quickly, matching
// client.ExecuteWorkflow's own semantics of merely *starting* durable
// Temporal orchestration, while AwaitCompletion is what actually holds the
// tenant's concurrency slot open for the run's full lifetime. That
// distinction matters: MaxConcurrentRuns is meant to cap runs truly in
// flight, not just enqueue-to-schedule latency, so the slot DispatchNext
// reserved must not be freed (via MarkCompleted) until the run has actually
// terminated.
type RunStarterPort interface {
	// StartRun begins run's execution (e.g. Temporal client.ExecuteWorkflow)
	// and returns as soon as that has been durably accepted, without waiting
	// for the run itself to finish.
	StartRun(ctx context.Context, run QueuedPipelineRun) (RunHandle, error)

	// AwaitCompletion blocks until the run identified by handle has reached
	// a terminal state (PASSED, FAILED, or CANCELLED), or ctx is cancelled.
	AwaitCompletion(ctx context.Context, handle RunHandle) error
}

// DefaultPollInterval is used when QueueDrainDispatcherOptions.PollInterval
// is unset. Short enough that a newly enqueued run doesn't sit idle
// noticeably long waiting for the next DispatchNext sweep; long enough not
// to busy-spin a CPU core while the queue is empty, which is the common
// steady-state case.
const DefaultPollInterval = 25 * time.Millisecond

// QueueDrainDispatcherOptions configures QueueDrainDispatcher's polling
// behavior.
type QueueDrainDispatcherOptions struct {
	// PollInterval is how long the drain loop sleeps after a sweep finds no
	// eligible run to dispatch (every tenant is either empty or at its
	// MaxConcurrentRuns cap) before it checks again. Defaults to
	// DefaultPollInterval if <= 0.
	PollInterval time.Duration
}

// QueueDrainDispatcher continuously pulls the next eligible run off a
// TenantRunQueue and hands it to a RunStarterPort. It is the background
// dispatch loop half of the HTTP-submit -> priority-queue -> dispatch
// pipeline that cmd/scheduler/main.go wires together at startup.
type QueueDrainDispatcher struct {
	queue   TenantRunQueue
	starter RunStarterPort
	opts    QueueDrainDispatcherOptions
}

// NewQueueDrainDispatcher constructs a dispatcher draining queue via
// starter. It does not start the loop -- call Run for that, typically from
// its own goroutine.
func NewQueueDrainDispatcher(queue TenantRunQueue, starter RunStarterPort, opts QueueDrainDispatcherOptions) *QueueDrainDispatcher {
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	return &QueueDrainDispatcher{queue: queue, starter: starter, opts: opts}
}

// Run blocks, repeatedly draining queue until ctx is cancelled. Each
// dispatched run is started and awaited on its own goroutine so that one
// run's StartRun/AwaitCompletion latency never blocks the next
// DispatchNext sweep -- concurrency both across different tenants and within
// a single tenant up to its MaxConcurrentRuns cap is exactly what
// TenantRunQueue's own bookkeeping already permits, so this loop must not
// impose an artificial global bottleneck on top of it by, say, awaiting one
// run before starting the next.
func (d *QueueDrainDispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dispatchedAny := false
		for {
			run, ok := d.queue.DispatchNext()
			if !ok {
				break
			}
			dispatchedAny = true
			go d.runOne(ctx, run)
		}

		if !dispatchedAny {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

// runOne starts run and awaits its completion, always releasing the
// concurrency slot DispatchNext reserved for it via MarkCompleted --
// including when StartRun itself fails, since a run that never started can
// never "complete" some other way and must not permanently hold its
// tenant's concurrency slot hostage.
func (d *QueueDrainDispatcher) runOne(ctx context.Context, run QueuedPipelineRun) {
	defer d.queue.MarkCompleted(run.TenantID, run.RunID)

	handle, err := d.starter.StartRun(ctx, run)
	if err != nil {
		log.Printf("scheduling: failed to start run %q (tenant %q): %v", run.RunID, run.TenantID, err)
		return
	}

	if err := d.starter.AwaitCompletion(ctx, handle); err != nil {
		log.Printf("scheduling: run %q (tenant %q) ended with error: %v", run.RunID, run.TenantID, err)
	}
}
