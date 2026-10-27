// Package scheduling implements admission control for pipeline runs *before*
// they become a Temporal workflow execution. dag-workflow-scheduler serves
// every tenant on the platform out of the same Temporal namespace and the
// same execution-fleet capacity; without a fairness layer in front of
// client.ExecuteWorkflow, one tenant submitting a burst of runs (e.g. a CI
// pipeline retrying in a loop) can starve every other tenant's runs behind
// it, since Temporal itself has no notion of "tenant" and will happily start
// every workflow it's asked to start.
//
// TenantRunQueue is that fairness layer: callers Enqueue a run for a tenant,
// and a scheduling loop repeatedly calls DispatchNext to pull the next run
// that should actually be started, in an order that respects each tenant's
// configured priority weight and concurrency cap rather than plain FIFO
// across all tenants combined.
package scheduling

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// TenantID identifies the owning tenant of a queued run. Kept as a distinct
// type (rather than a bare string) so a caller can't accidentally pass a
// PipelineID or RunID where a TenantID is expected.
type TenantID string

// QueuedPipelineRun is one pipeline run waiting to be dispatched to
// dag-workflow-scheduler's Temporal client.ExecuteWorkflow call.
type QueuedPipelineRun struct {
	RunID      string
	TenantID   TenantID
	PipelineID string
	// EnqueuedAt is used only as a tie-breaker within a tenant's own queue
	// (oldest-first) — it never lets one tenant's backlog jump ahead of
	// another tenant's turn, which is the entire point of weighted fair
	// sharing over plain global FIFO.
	EnqueuedAt time.Time
}

// TenantSchedulingPolicy configures how a single tenant's runs are treated
// by the queue: how much of the shared dispatch capacity they're entitled to
// relative to other tenants, and a hard ceiling on how many of their runs
// may be in flight (already dispatched, not yet resolved) at once so a
// single tenant with a very high weight still cannot monopolize every
// execution-fleet worker simultaneously.
type TenantSchedulingPolicy struct {
	// Weight is this tenant's share of dispatch capacity relative to every
	// other tenant with a pending run, using the same weighted-fair-queueing
	// convention as network packet schedulers: a tenant with Weight 3 is
	// dispatched roughly 3x as often as a tenant with Weight 1 whenever both
	// have runs waiting. Must be >= 1.
	Weight int
	// MaxConcurrentRuns caps how many of this tenant's runs may be
	// in-flight (dispatched via MarkDispatched, not yet resolved via
	// MarkCompleted) at the same time, regardless of Weight. This is what
	// actually prevents starvation of *other* tenants in the pathological
	// case where one tenant has both a very high Weight and a very large
	// backlog: even fully "entitled" to dispatch, it still can't hold more
	// than MaxConcurrentRuns slots, leaving room for others.
	MaxConcurrentRuns int
}

// DefaultTenantSchedulingPolicy is applied to any tenant that enqueues a run
// without having called SetTenantPolicy first, so an unconfigured tenant
// gets reasonable, non-starving defaults rather than an error or unbounded
// concurrency.
var DefaultTenantSchedulingPolicy = TenantSchedulingPolicy{
	Weight:            1,
	MaxConcurrentRuns: 5,
}

// TenantRunQueue is the port dag-workflow-scheduler's HTTP trigger handler
// (see cmd/scheduler/main.go's newPipelineRunsHandler) and its dispatch loop
// depend on. Modeling it as an interface — rather than exposing
// WeightedFairShareQueue's concrete type directly — keeps the call sites
// swappable for a distributed implementation (e.g. Redis-backed, for
// multi-replica schedulers) without touching callers.
type TenantRunQueue interface {
	// Enqueue admits run into its tenant's backlog. Returns an error only if
	// run.TenantID or run.RunID is empty.
	Enqueue(run QueuedPipelineRun) error

	// DispatchNext selects and removes the single next run that should be
	// started right now, respecting every tenant's Weight and
	// MaxConcurrentRuns. Returns ok=false if no tenant currently has both a
	// pending run and free concurrency headroom (i.e. every eligible tenant
	// is either empty or already at its cap).
	DispatchNext() (run QueuedPipelineRun, ok bool)

	// MarkCompleted releases the in-flight concurrency slot a prior
	// DispatchNext call consumed for runID's tenant, so a future
	// DispatchNext call can admit another of that tenant's runs. Must be
	// called exactly once per run that DispatchNext returned, once the run
	// terminates (PASSED, FAILED, or CANCELLED).
	MarkCompleted(tenantID TenantID, runID string)

	// SetTenantPolicy installs (or replaces) the scheduling policy for a
	// tenant. Safe to call at any time, including while runs from that
	// tenant are already queued or in flight — takes effect on the next
	// DispatchNext call.
	SetTenantPolicy(tenantID TenantID, policy TenantSchedulingPolicy)

	// PendingCount reports how many runs are currently queued (not yet
	// dispatched) for tenantID, primarily for observability/metrics export.
	PendingCount(tenantID TenantID) int

	// InFlightCount reports how many of tenantID's dispatched runs have not
	// yet called MarkCompleted.
	InFlightCount(tenantID TenantID) int
}

// tenantQueueState is the per-tenant bookkeeping WeightedFairShareQueue
// keeps: its policy, its FIFO backlog of not-yet-dispatched runs, its
// current in-flight count, and its accumulated "deficit" counter used by the
// deficit-round-robin-style fairness algorithm in DispatchNext.
type tenantQueueState struct {
	policy   TenantSchedulingPolicy
	backlog  *list.List // of QueuedPipelineRun, oldest at Front()
	inFlight int
	// deficitCredits accumulates by policy.Weight every time this tenant is
	// passed over in a dispatch round-robin pass, and is spent (decremented
	// by 1) every time a run of theirs is actually dispatched. This is the
	// classic Deficit Round Robin (DRR) technique adapted from network
	// packet scheduling to run dispatch: it guarantees that over any long
	// enough window, each tenant's dispatch share converges to
	// policy.Weight / sum(all active tenants' weights), without needing to
	// recompute a full priority ordering on every single dispatch call.
	deficitCredits int
}

// WeightedFairShareQueue is the in-memory TenantRunQueue implementation.
// "In-memory" mirrors pipeline-run.pubsub.ts's PubSub caveat in the sibling
// TS gateway: correct and sufficient for a single dag-workflow-scheduler
// replica, but state is lost on restart and not shared across replicas. See
// SCALING.md for the multi-replica follow-up (a Redis- or Postgres-backed
// TenantRunQueue implementation behind the same interface).
type WeightedFairShareQueue struct {
	mu sync.Mutex

	// tenantOrder preserves first-seen tenant ordering so DispatchNext's
	// round-robin pass is deterministic (helpful for tests and for
	// reasoning about fairness behavior), rather than iterating a Go map in
	// its randomized order.
	tenantOrder []TenantID
	tenants     map[TenantID]*tenantQueueState

	// cursor is the index into tenantOrder that the next Deficit Round Robin
	// sweep in DispatchNext starts from, persisted across calls so dispatch
	// turns rotate through tenants evenly rather than always restarting the
	// sweep from tenantOrder[0] (which would bias toward whichever tenant
	// happened to register first).
	cursor int
}

// NewWeightedFairShareQueue constructs an empty queue. Tenant policies can
// be pre-registered via SetTenantPolicy, or left to fall back to
// DefaultTenantSchedulingPolicy on first Enqueue.
func NewWeightedFairShareQueue() *WeightedFairShareQueue {
	return &WeightedFairShareQueue{
		tenants: make(map[TenantID]*tenantQueueState),
	}
}

func (q *WeightedFairShareQueue) SetTenantPolicy(tenantID TenantID, policy TenantSchedulingPolicy) {
	if policy.Weight < 1 {
		policy.Weight = 1
	}
	if policy.MaxConcurrentRuns < 1 {
		policy.MaxConcurrentRuns = 1
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	state := q.stateForLocked(tenantID)
	state.policy = policy
}

func (q *WeightedFairShareQueue) Enqueue(run QueuedPipelineRun) error {
	if run.TenantID == "" {
		return fmt.Errorf("tenant_priority_run_queue: run %q has empty TenantID", run.RunID)
	}
	if run.RunID == "" {
		return fmt.Errorf("tenant_priority_run_queue: run for tenant %q has empty RunID", run.TenantID)
	}
	if run.EnqueuedAt.IsZero() {
		run.EnqueuedAt = time.Now()
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	state := q.stateForLocked(run.TenantID)
	state.backlog.PushBack(run)
	return nil
}

// DispatchNext implements Deficit Round Robin (DRR) — the same technique
// network packet schedulers use for weighted-fair bandwidth allocation,
// adapted here to run dispatch instead of packets. The cursor sits on one
// tenant across consecutive DispatchNext calls: on first arriving at a
// tenant with an exhausted (<=0) credit balance, it is topped up by exactly
// policy.Weight credits (its "quantum" for this turn); every dispatch from
// that tenant spends one credit; and the cursor only advances to the next
// tenant once that tenant's credits are exhausted again (or it has no
// backlog, or it is at its concurrency cap). The net effect over repeated
// calls: a Weight-3 tenant gets 3 consecutive dispatch turns for every 1 a
// Weight-1 tenant gets, and a tenant sitting at its MaxConcurrentRuns cap is
// skipped over (never blocking the tenants behind it in tenantOrder) without
// losing the credits it already banked for when capacity frees up.
func (q *WeightedFairShareQueue) DispatchNext() (QueuedPipelineRun, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	tenantCount := len(q.tenantOrder)
	for attempts := 0; attempts < tenantCount; attempts++ {
		tenantID := q.tenantOrder[q.cursor]
		state := q.tenants[tenantID]

		if state.backlog.Len() == 0 {
			q.advanceCursorLocked(tenantCount)
			continue // idle tenants neither earn nor spend credits, so they can't bank an unfair head start
		}

		if state.deficitCredits <= 0 {
			state.deficitCredits += state.policy.Weight // start of this tenant's turn: grant its quantum
		}

		if state.inFlight >= state.policy.MaxConcurrentRuns {
			// At its concurrency cap: move on to the next tenant so it isn't
			// starved behind this one, but leave the credit banked — once
			// MarkCompleted frees a slot, this tenant resumes its turn with
			// whatever quantum it already has rather than losing it.
			q.advanceCursorLocked(tenantCount)
			continue
		}

		front := state.backlog.Front()
		run := state.backlog.Remove(front).(QueuedPipelineRun)
		state.deficitCredits--
		state.inFlight++

		if state.deficitCredits <= 0 {
			q.advanceCursorLocked(tenantCount) // turn spent; give the next tenant in tenantOrder a chance
		}
		return run, true
	}

	return QueuedPipelineRun{}, false
}

// advanceCursorLocked moves the round-robin cursor to the next tenant in
// tenantOrder. Callers must hold q.mu and must not call this with
// tenantCount == 0.
func (q *WeightedFairShareQueue) advanceCursorLocked(tenantCount int) {
	q.cursor = (q.cursor + 1) % tenantCount
}

func (q *WeightedFairShareQueue) MarkCompleted(tenantID TenantID, runID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	state, ok := q.tenants[tenantID]
	if !ok {
		return
	}
	if state.inFlight > 0 {
		state.inFlight--
	}
	_ = runID // not separately tracked per-run today; kept in the signature so a future
	// implementation can validate runID was actually the one dispatched, and so
	// TenantRunQueue's interface doesn't need to change to add that later.
}

func (q *WeightedFairShareQueue) PendingCount(tenantID TenantID) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	state, ok := q.tenants[tenantID]
	if !ok {
		return 0
	}
	return state.backlog.Len()
}

func (q *WeightedFairShareQueue) InFlightCount(tenantID TenantID) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	state, ok := q.tenants[tenantID]
	if !ok {
		return 0
	}
	return state.inFlight
}

// stateForLocked returns tenantID's state, creating it (with
// DefaultTenantSchedulingPolicy) on first reference. Callers must hold q.mu.
func (q *WeightedFairShareQueue) stateForLocked(tenantID TenantID) *tenantQueueState {
	state, ok := q.tenants[tenantID]
	if ok {
		return state
	}

	state = &tenantQueueState{
		policy:  DefaultTenantSchedulingPolicy,
		backlog: list.New(),
	}
	q.tenants[tenantID] = state
	q.tenantOrder = append(q.tenantOrder, tenantID)
	return state
}

var _ TenantRunQueue = (*WeightedFairShareQueue)(nil)
