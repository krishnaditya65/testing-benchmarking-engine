package scheduling

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeRunStarter is a RunStarterPort test double. StartRun always succeeds
// and records the run; AwaitCompletion blocks on a per-run channel until the
// test explicitly releases it (releaseRun), unless autoRelease is set, in
// which case AwaitCompletion returns immediately -- useful for tests that
// only care about dispatch order/proportionality rather than concurrency
// timing.
type fakeRunStarter struct {
	autoRelease bool

	mu      sync.Mutex
	started []QueuedPipelineRun
	release map[string]chan struct{}
}

func newFakeRunStarter(autoRelease bool) *fakeRunStarter {
	return &fakeRunStarter{autoRelease: autoRelease, release: make(map[string]chan struct{})}
}

func (f *fakeRunStarter) StartRun(_ context.Context, run QueuedPipelineRun) (RunHandle, error) {
	f.mu.Lock()
	f.started = append(f.started, run)
	if !f.autoRelease {
		f.release[run.RunID] = make(chan struct{})
	}
	f.mu.Unlock()
	return RunHandle{WorkflowID: run.RunID, RunID: run.RunID}, nil
}

func (f *fakeRunStarter) AwaitCompletion(ctx context.Context, handle RunHandle) error {
	if f.autoRelease {
		return nil
	}
	f.mu.Lock()
	ch := f.release[handle.RunID]
	f.mu.Unlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseRun unblocks a prior AwaitCompletion call for runID. Safe to call
// even if StartRun for runID hasn't happened yet, or has already been
// released -- both are no-ops.
func (f *fakeRunStarter) releaseRun(runID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.release[runID]
	if !ok {
		return
	}
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
}

func (f *fakeRunStarter) startedCountForTenant(tenantID TenantID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, run := range f.started {
		if run.TenantID == tenantID {
			count++
		}
	}
	return count
}

func (f *fakeRunStarter) startedOrder() []QueuedPipelineRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]QueuedPipelineRun, len(f.started))
	copy(out, f.started)
	return out
}

// waitForCondition polls cond until it returns true or the deadline passes,
// failing the test if it never becomes true.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func TestQueueDrainDispatcher_RespectsPerTenantConcurrencyCap(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-capped", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 2})

	for i := 0; i < 5; i++ {
		mustEnqueue(t, q, "tenant-capped", fmt.Sprintf("run-%d", i))
	}

	starter := newFakeRunStarter(false)
	dispatcher := NewQueueDrainDispatcher(q, starter, QueueDrainDispatcherOptions{PollInterval: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx)

	// Exactly 2 of the 5 runs should ever be started+in-flight at once,
	// never more, however long we give the loop to try.
	waitForCondition(t, time.Second, func() bool { return len(starter.startedOrder()) == 2 })
	time.Sleep(50 * time.Millisecond) // generous window for an over-eager loop to misbehave in
	if got := len(starter.startedOrder()); got != 2 {
		t.Fatalf("expected exactly 2 runs started while at the concurrency cap, got %d", got)
	}
	if got := q.InFlightCount("tenant-capped"); got != 2 {
		t.Fatalf("expected InFlightCount=2 while capped, got %d", got)
	}

	// Keep releasing whatever's currently in flight until all 5 have been
	// started: each release frees exactly one concurrency slot, which lets
	// the dispatcher admit (at most) one more run at a time, so this drains
	// the whole backlog while still respecting the cap of 2 at any instant.
	waitForCondition(t, 2*time.Second, func() bool {
		for _, run := range starter.startedOrder() {
			starter.releaseRun(run.RunID)
		}
		return len(starter.startedOrder()) == 5
	})
	waitForCondition(t, time.Second, func() bool { return q.InFlightCount("tenant-capped") == 0 })

	if got := len(starter.startedOrder()); got != 5 {
		t.Fatalf("expected all 5 enqueued runs to eventually be started, got %d", got)
	}
}

func TestQueueDrainDispatcher_DispatchesProportionallyToWeight(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-heavy", TenantSchedulingPolicy{Weight: 3, MaxConcurrentRuns: 1000})
	q.SetTenantPolicy("tenant-light", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 1000})

	const runsPerTenant = 60
	for i := 0; i < runsPerTenant; i++ {
		mustEnqueue(t, q, "tenant-heavy", fmt.Sprintf("heavy-run-%d", i))
		mustEnqueue(t, q, "tenant-light", fmt.Sprintf("light-run-%d", i))
	}

	starter := newFakeRunStarter(true) // completes instantly so the loop drains everything quickly
	dispatcher := NewQueueDrainDispatcher(q, starter, QueueDrainDispatcherOptions{PollInterval: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx)

	waitForCondition(t, 2*time.Second, func() bool {
		return starter.startedCountForTenant("tenant-heavy") == runsPerTenant &&
			starter.startedCountForTenant("tenant-light") == runsPerTenant
	})

	// Every run submitted to the dispatcher must eventually have been
	// started via RunStarterPort -- nothing should be silently dropped.
	order := starter.startedOrder()
	if len(order) != 2*runsPerTenant {
		t.Fatalf("expected %d total dispatches, got %d", 2*runsPerTenant, len(order))
	}

	// Proportionality check: within the first N dispatches (before either
	// backlog can possibly have run dry), tenant-heavy (weight 3) should
	// have received noticeably more turns than tenant-light (weight 1) --
	// mirroring tenant_priority_run_queue_test.go's own proportionality
	// assertion on DispatchNext, now exercised through the dispatcher that
	// actually calls it in a loop rather than by calling DispatchNext
	// directly.
	const sampleWindow = 40
	if len(order) < sampleWindow {
		t.Fatalf("expected at least %d dispatches to sample from, got %d", sampleWindow, len(order))
	}
	heavyInWindow := 0
	for _, run := range order[:sampleWindow] {
		if run.TenantID == "tenant-heavy" {
			heavyInWindow++
		}
	}
	if heavyInWindow < 20 || heavyInWindow > 36 {
		t.Fatalf("expected tenant-heavy to receive noticeably more than tenant-light across the first %d dispatches, got heavy=%d (expected roughly 30)", sampleWindow, heavyInWindow)
	}
}

func TestQueueDrainDispatcher_StopsCleanlyOnContextCancellation(t *testing.T) {
	q := NewWeightedFairShareQueue()
	starter := newFakeRunStarter(true)
	dispatcher := NewQueueDrainDispatcher(q, starter, QueueDrainDispatcherOptions{PollInterval: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected Run to return promptly after context cancellation")
	}
}

func TestQueueDrainDispatcher_MarksCompletedEvenWhenStartRunFails(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-a", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 1})
	mustEnqueue(t, q, "tenant-a", "run-1")
	mustEnqueue(t, q, "tenant-a", "run-2")

	starter := &failingRunStarter{}
	dispatcher := NewQueueDrainDispatcher(q, starter, QueueDrainDispatcherOptions{PollInterval: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx)

	// Despite MaxConcurrentRuns=1, both runs must eventually be attempted:
	// a failed StartRun must still free the concurrency slot it reserved,
	// or run-2 would be stuck behind run-1 forever.
	waitForCondition(t, time.Second, func() bool { return starter.attempts() == 2 })
	waitForCondition(t, time.Second, func() bool { return q.InFlightCount("tenant-a") == 0 })
}

// failingRunStarter always fails StartRun, to verify QueueDrainDispatcher
// still releases the concurrency slot it reserved for the run.
type failingRunStarter struct {
	mu    sync.Mutex
	count int
}

func (f *failingRunStarter) StartRun(_ context.Context, _ QueuedPipelineRun) (RunHandle, error) {
	f.mu.Lock()
	f.count++
	f.mu.Unlock()
	return RunHandle{}, fmt.Errorf("simulated start failure")
}

func (f *failingRunStarter) AwaitCompletion(_ context.Context, _ RunHandle) error {
	return nil
}

func (f *failingRunStarter) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}
