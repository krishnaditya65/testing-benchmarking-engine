package scheduling

import (
	"fmt"
	"testing"
)

func TestEnqueue_RejectsMissingTenantOrRunID(t *testing.T) {
	q := NewWeightedFairShareQueue()

	if err := q.Enqueue(QueuedPipelineRun{RunID: "run-1"}); err == nil {
		t.Fatal("expected error enqueuing a run with empty TenantID, got nil")
	}
	if err := q.Enqueue(QueuedPipelineRun{TenantID: "tenant-a"}); err == nil {
		t.Fatal("expected error enqueuing a run with empty RunID, got nil")
	}
}

func TestDispatchNext_EmptyQueueReturnsNotOK(t *testing.T) {
	q := NewWeightedFairShareQueue()

	if _, ok := q.DispatchNext(); ok {
		t.Fatal("expected DispatchNext on an empty queue to return ok=false")
	}
}

func TestDispatchNext_SingleTenantIsFIFO(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-a", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 10})

	mustEnqueue(t, q, "tenant-a", "run-1")
	mustEnqueue(t, q, "tenant-a", "run-2")
	mustEnqueue(t, q, "tenant-a", "run-3")

	for _, want := range []string{"run-1", "run-2", "run-3"} {
		run, ok := q.DispatchNext()
		if !ok {
			t.Fatalf("expected a run to dispatch, got none")
		}
		if run.RunID != want {
			t.Fatalf("expected FIFO order within a tenant: got %q, want %q", run.RunID, want)
		}
	}
}

// TestDispatchNext_HigherWeightTenantGetsProportionallyMoreDispatches is the
// core fairness assertion: with tenant-heavy having 3x tenant-light's
// weight, and both tenants supplying an unbounded backlog and unlimited
// concurrency, tenant-heavy should receive roughly 3x the dispatches over a
// long enough run of DispatchNext calls — not "all of them" (that would be
// starvation of tenant-light, exactly what this feature exists to prevent)
// and not "an even split" (that would ignore Weight entirely).
func TestDispatchNext_HigherWeightTenantGetsProportionallyMoreDispatches(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-heavy", TenantSchedulingPolicy{Weight: 3, MaxConcurrentRuns: 1000})
	q.SetTenantPolicy("tenant-light", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 1000})

	const runsPerTenant = 300
	for i := 0; i < runsPerTenant; i++ {
		mustEnqueue(t, q, "tenant-heavy", fmt.Sprintf("heavy-run-%d", i))
		mustEnqueue(t, q, "tenant-light", fmt.Sprintf("light-run-%d", i))
	}

	dispatchCounts := map[TenantID]int{}
	for {
		run, ok := q.DispatchNext()
		if !ok {
			break
		}
		dispatchCounts[run.TenantID]++
	}

	if dispatchCounts["tenant-heavy"] != runsPerTenant || dispatchCounts["tenant-light"] != runsPerTenant {
		t.Fatalf("expected every enqueued run to eventually dispatch, got heavy=%d light=%d (want %d each)",
			dispatchCounts["tenant-heavy"], dispatchCounts["tenant-light"], runsPerTenant)
	}

	// Check proportionality over the *early* portion of the interleaving,
	// where both tenants still have backlog and the 3:1 weight ratio should
	// be clearly visible, rather than over the whole run (which necessarily
	// ends up 1:1 once tenant-light's smaller backlog... note both backlogs
	// are equal size here, so instead assert against the dispatch *order*:
	// tenant-heavy should be dispatched noticeably earlier/more often across
	// the first N dispatches than tenant-light.
	q2 := NewWeightedFairShareQueue()
	q2.SetTenantPolicy("tenant-heavy", TenantSchedulingPolicy{Weight: 3, MaxConcurrentRuns: 1000})
	q2.SetTenantPolicy("tenant-light", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 1000})
	for i := 0; i < 100; i++ {
		mustEnqueue(t, q2, "tenant-heavy", fmt.Sprintf("heavy-run-%d", i))
	}
	for i := 0; i < 100; i++ {
		mustEnqueue(t, q2, "tenant-light", fmt.Sprintf("light-run-%d", i))
	}

	const sampleWindow = 40 // small enough that light's 100-run backlog never runs dry within it
	heavyInWindow := 0
	for i := 0; i < sampleWindow; i++ {
		run, ok := q2.DispatchNext()
		if !ok {
			t.Fatalf("expected a run to dispatch at step %d", i)
		}
		if run.TenantID == "tenant-heavy" {
			heavyInWindow++
		}
	}

	// With a 3:1 weight ratio we expect roughly 30 of the 40 dispatches to
	// go to tenant-heavy; allow generous slack for DRR's rounding behavior
	// but assert it's clearly weighted, not a coin flip and not exclusive.
	if heavyInWindow < 20 || heavyInWindow > 36 {
		t.Fatalf("expected tenant-heavy (weight 3) to receive noticeably more than tenant-light (weight 1) "+
			"across %d dispatches, got heavy=%d (expected roughly 30)", sampleWindow, heavyInWindow)
	}
}

// TestDispatchNext_ConcurrencyCapPreventsStarvation is the other half of the
// fairness contract: even a tenant with a very high weight and a huge
// backlog must never hold more than MaxConcurrentRuns dispatched-but-not-yet-
// -completed runs, so lower-weight tenants always eventually get a turn.
func TestDispatchNext_ConcurrencyCapPreventsStarvation(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-greedy", TenantSchedulingPolicy{Weight: 100, MaxConcurrentRuns: 2})
	q.SetTenantPolicy("tenant-starved", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 10})

	for i := 0; i < 50; i++ {
		mustEnqueue(t, q, "tenant-greedy", fmt.Sprintf("greedy-run-%d", i))
	}
	mustEnqueue(t, q, "tenant-starved", "starved-run-1")

	dispatched := 0
	sawStarvedTenantDispatch := false
	for dispatched < 10 {
		run, ok := q.DispatchNext()
		if !ok {
			break
		}
		dispatched++
		if run.TenantID == "tenant-greedy" {
			if q.InFlightCount("tenant-greedy") > 2 {
				t.Fatalf("tenant-greedy exceeded its MaxConcurrentRuns cap of 2: in-flight=%d",
					q.InFlightCount("tenant-greedy"))
			}
			continue
		}
		sawStarvedTenantDispatch = true
		break
	}

	if !sawStarvedTenantDispatch {
		t.Fatal("expected tenant-starved's run to be dispatched once tenant-greedy hit its concurrency cap, " +
			"but it never was — this is exactly the starvation this feature must prevent")
	}
}

func TestMarkCompleted_FreesConcurrencySlotForNextDispatch(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-a", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 1})

	mustEnqueue(t, q, "tenant-a", "run-1")
	mustEnqueue(t, q, "tenant-a", "run-2")

	first, ok := q.DispatchNext()
	if !ok || first.RunID != "run-1" {
		t.Fatalf("expected run-1 to dispatch first, got %+v ok=%v", first, ok)
	}

	if _, ok := q.DispatchNext(); ok {
		t.Fatal("expected DispatchNext to withhold run-2 while tenant-a is at its MaxConcurrentRuns=1 cap")
	}

	q.MarkCompleted("tenant-a", first.RunID)

	second, ok := q.DispatchNext()
	if !ok || second.RunID != "run-2" {
		t.Fatalf("expected run-2 to dispatch after MarkCompleted freed the concurrency slot, got %+v ok=%v", second, ok)
	}
}

func TestPendingAndInFlightCounts(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-a", TenantSchedulingPolicy{Weight: 1, MaxConcurrentRuns: 5})

	mustEnqueue(t, q, "tenant-a", "run-1")
	mustEnqueue(t, q, "tenant-a", "run-2")

	if got := q.PendingCount("tenant-a"); got != 2 {
		t.Fatalf("expected PendingCount=2 before any dispatch, got %d", got)
	}

	run, ok := q.DispatchNext()
	if !ok {
		t.Fatal("expected a run to dispatch")
	}

	if got := q.PendingCount("tenant-a"); got != 1 {
		t.Fatalf("expected PendingCount=1 after one dispatch, got %d", got)
	}
	if got := q.InFlightCount("tenant-a"); got != 1 {
		t.Fatalf("expected InFlightCount=1 after one dispatch, got %d", got)
	}

	q.MarkCompleted("tenant-a", run.RunID)
	if got := q.InFlightCount("tenant-a"); got != 0 {
		t.Fatalf("expected InFlightCount=0 after MarkCompleted, got %d", got)
	}
}

func TestSetTenantPolicy_ClampsInvalidValues(t *testing.T) {
	q := NewWeightedFairShareQueue()
	q.SetTenantPolicy("tenant-a", TenantSchedulingPolicy{Weight: 0, MaxConcurrentRuns: -5})

	mustEnqueue(t, q, "tenant-a", "run-1")
	mustEnqueue(t, q, "tenant-a", "run-2")

	// MaxConcurrentRuns must be clamped to at least 1, or this tenant would
	// never be able to dispatch anything at all.
	run, ok := q.DispatchNext()
	if !ok || run.RunID != "run-1" {
		t.Fatalf("expected run-1 to dispatch despite an invalid (<1) configured MaxConcurrentRuns, got %+v ok=%v", run, ok)
	}
}

func mustEnqueue(t *testing.T, q TenantRunQueue, tenantID TenantID, runID string) {
	t.Helper()
	if err := q.Enqueue(QueuedPipelineRun{TenantID: tenantID, RunID: runID, PipelineID: "pipeline-x"}); err != nil {
		t.Fatalf("Enqueue(%q, %q) failed: %v", tenantID, runID, err)
	}
}
