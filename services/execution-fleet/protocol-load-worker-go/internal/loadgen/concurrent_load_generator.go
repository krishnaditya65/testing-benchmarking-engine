package loadgen

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/protocolclient"
)

// RequestFactory produces the next Request a worker goroutine should
// execute. It is called once per call attempt, so implementations that
// need per-call variation (unique IDs, values pulled from a
// pipeline.VariableContext) can close over that state safely — the
// generator never calls it concurrently for the same slot.
type RequestFactory func(ctx context.Context) protocolclient.Request

// CallResult pairs one executed call's Response with the goroutine slot
// index that ran it, for callers that want per-worker attribution.
type CallResult struct {
	Response  protocolclient.Response
	WorkerID  int
	Timestamp time.Time
}

// RunMetrics aggregates every CallResult observed during a Run.
type RunMetrics struct {
	TotalCalls      int64
	SuccessfulCalls int64
	FailedCalls     int64
	TotalLatency    time.Duration
	MaxLatency      time.Duration
	TotalRequestB   int64
	TotalResponseB  int64
}

// AverageLatency returns TotalLatency / TotalCalls, or 0 if no calls ran.
func (m RunMetrics) AverageLatency() time.Duration {
	if m.TotalCalls == 0 {
		return 0
	}
	return time.Duration(int64(m.TotalLatency) / m.TotalCalls)
}

// PhaseCheckpoint is the durable state saved before a load phase
// transition so a redispatched worker (after a crash or spot-instance
// preemption) can resume roughly where this one left off, per
// ARCHITECTURE.md's "checkpointed to Redis before each load-phase
// transition" durability design.
type PhaseCheckpoint struct {
	PhaseName       string
	CompletedAt     time.Time
	CallsCompleted  int64
	CallsSucceeded  int64
	LastConcurrency int
}

// CheckpointStore persists a PhaseCheckpoint between load phases. The
// control plane reads the last checkpoint back out when it redispatches a
// test run node to a fresh worker after a crash.
//
// NOTE: a production implementation lives outside this package (e.g.
// internal/checkpoint/redis_checkpoint_store.go) and would use a real
// Redis client (go-redis/redis) writing a small hash/JSON blob keyed by
// run ID + node ID, with a TTL slightly longer than the max phase
// duration. This starter ships only the interface and a no-op default so
// ConcurrentLoadGenerator has a real extension point without requiring a
// live Redis dependency to compile/run standalone.
type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, runID string, checkpoint PhaseCheckpoint) error
}

// NoopCheckpointStore discards checkpoints. It is the default so
// ConcurrentLoadGenerator is usable without wiring a real store, while
// still exercising the checkpoint call site.
type NoopCheckpointStore struct{}

func (NoopCheckpointStore) SaveCheckpoint(ctx context.Context, runID string, checkpoint PhaseCheckpoint) error {
	return nil
}

// ConcurrentLoadGenerator drives a protocolclient.ProtocolClient at a
// concurrency level that tracks a TrafficProfile over time, using a real
// worker-pool of goroutines that grows/shrinks to match the profile's
// target rather than fixing worker count up front.
type ConcurrentLoadGenerator struct {
	Client          protocolclient.ProtocolClient
	RequestFactory  RequestFactory
	CheckpointStore CheckpointStore
	RunID           string

	// SampleInterval controls how often the generator re-samples the
	// TrafficProfile and reconciles the live worker count against it.
	// Defaults to 500ms if zero.
	SampleInterval time.Duration

	// Results, if non-nil, receives every CallResult as it completes.
	// The generator does not close this channel (callers may share it
	// across phases); callers should drain it concurrently with Run to
	// avoid backpressuring workers.
	Results chan<- CallResult
}

// worker is one long-lived goroutine that repeatedly calls
// RequestFactory + Client.Execute until told to stop via its stop channel
// or ctx cancellation. Keeping workers long-lived (rather than
// spawning one goroutine per call) avoids goroutine-creation overhead
// dominating at high concurrency.
type worker struct {
	stop chan struct{}
	done chan struct{}
}

// Run executes a single load phase described by profile, ramping the
// worker pool up and down to track profile.TargetConcurrency(elapsed) on
// every SampleInterval tick, until profile.Duration() elapses or ctx is
// cancelled. It returns the aggregated RunMetrics for the phase and saves
// a PhaseCheckpoint via CheckpointStore before returning, so the caller
// can safely treat "Run returned" as "safe to transition to the next
// phase" per the checkpoint-before-transition durability contract.
func (g *ConcurrentLoadGenerator) Run(ctx context.Context, profile TrafficProfile) (RunMetrics, error) {
	if g.Client == nil {
		return RunMetrics{}, fmt.Errorf("loadgen: ConcurrentLoadGenerator requires a non-nil Client")
	}
	if g.RequestFactory == nil {
		return RunMetrics{}, fmt.Errorf("loadgen: ConcurrentLoadGenerator requires a non-nil RequestFactory")
	}
	sampleInterval := g.SampleInterval
	if sampleInterval <= 0 {
		sampleInterval = 500 * time.Millisecond
	}
	checkpointStore := g.CheckpointStore
	if checkpointStore == nil {
		checkpointStore = NoopCheckpointStore{}
	}

	phaseCtx, cancelPhase := context.WithTimeout(ctx, profile.Duration())
	defer cancelPhase()

	metrics := &runMetricsAccumulator{}
	var nextWorkerID int64
	workers := make(map[int64]*worker)
	var workersMu sync.Mutex

	spawnWorker := func() {
		id := atomic.AddInt64(&nextWorkerID, 1)
		w := &worker{stop: make(chan struct{}), done: make(chan struct{})}
		workersMu.Lock()
		workers[id] = w
		workersMu.Unlock()

		go g.runWorker(phaseCtx, int(id), w, metrics)
	}

	stopOneWorker := func() {
		workersMu.Lock()
		defer workersMu.Unlock()
		for id, w := range workers {
			close(w.stop)
			delete(workers, id)
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

		workersMu.Lock()
		current := len(workers)
		workersMu.Unlock()

		for current < target {
			spawnWorker()
			current++
		}
		for current > target {
			stopOneWorker()
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

	// Drain every worker before reporting final metrics so counts are
	// stable.
	workersMu.Lock()
	remaining := make([]*worker, 0, len(workers))
	for _, w := range workers {
		close(w.stop)
		remaining = append(remaining, w)
	}
	workersMu.Unlock()
	for _, w := range remaining {
		<-w.done
	}

	result := metrics.snapshot()

	checkpoint := PhaseCheckpoint{
		PhaseName:       profile.Name(),
		CompletedAt:     time.Now(),
		CallsCompleted:  result.TotalCalls,
		CallsSucceeded:  result.SuccessfulCalls,
		LastConcurrency: lastConcurrency,
	}
	if err := checkpointStore.SaveCheckpoint(ctx, g.RunID, checkpoint); err != nil {
		return result, fmt.Errorf("loadgen: checkpointing phase %q before transition: %w", profile.Name(), err)
	}

	return result, nil
}

// runWorker is the body of one long-lived worker goroutine: loop calling
// RequestFactory + Client.Execute until stop is closed or ctx is done,
// recording every result into metrics and forwarding it to g.Results if
// set.
func (g *ConcurrentLoadGenerator) runWorker(ctx context.Context, id int, w *worker, metrics *runMetricsAccumulator) {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		default:
		}

		req := g.RequestFactory(ctx)
		resp, err := g.Client.Execute(ctx, req)
		metrics.record(resp)

		if g.Results != nil {
			select {
			case g.Results <- CallResult{Response: resp, WorkerID: id, Timestamp: time.Now()}:
			case <-ctx.Done():
				return
			}
		}
		_ = err // error detail already carried on resp.Err / resp for aggregation
	}
}

// runMetricsAccumulator collects RunMetrics fields with atomic ops so many
// worker goroutines can record concurrently without a shared mutex on the
// hot path.
type runMetricsAccumulator struct {
	totalCalls      int64
	successfulCalls int64
	failedCalls     int64
	totalLatencyNs  int64
	maxLatencyNs    int64
	totalRequestB   int64
	totalResponseB  int64
}

func (a *runMetricsAccumulator) record(resp protocolclient.Response) {
	atomic.AddInt64(&a.totalCalls, 1)
	if resp.Err != nil {
		atomic.AddInt64(&a.failedCalls, 1)
	} else {
		atomic.AddInt64(&a.successfulCalls, 1)
	}
	atomic.AddInt64(&a.totalLatencyNs, int64(resp.Metrics.Latency))
	atomic.AddInt64(&a.totalRequestB, int64(resp.Metrics.RequestBytes))
	atomic.AddInt64(&a.totalResponseB, int64(resp.Metrics.ResponseBytes))

	for {
		current := atomic.LoadInt64(&a.maxLatencyNs)
		latency := int64(resp.Metrics.Latency)
		if latency <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&a.maxLatencyNs, current, latency) {
			break
		}
	}
}

func (a *runMetricsAccumulator) snapshot() RunMetrics {
	return RunMetrics{
		TotalCalls:      atomic.LoadInt64(&a.totalCalls),
		SuccessfulCalls: atomic.LoadInt64(&a.successfulCalls),
		FailedCalls:     atomic.LoadInt64(&a.failedCalls),
		TotalLatency:    time.Duration(atomic.LoadInt64(&a.totalLatencyNs)),
		MaxLatency:      time.Duration(atomic.LoadInt64(&a.maxLatencyNs)),
		TotalRequestB:   atomic.LoadInt64(&a.totalRequestB),
		TotalResponseB:  atomic.LoadInt64(&a.totalResponseB),
	}
}
