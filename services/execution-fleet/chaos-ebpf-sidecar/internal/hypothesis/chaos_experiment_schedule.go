// chaos_experiment_schedule.go lets a chaos experiment (a SteadyStateHypothesis plus a
// FaultInjection) be scheduled to run automatically — once at a specific time, or on a
// recurring cadence — instead of only ever being triggered by a manual control-plane dispatch.
package hypothesis

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ScheduleKind selects whether a ChaosExperimentSchedule fires exactly once or repeatedly.
type ScheduleKind string

const (
	// ScheduleKindOneShot fires exactly once, at FireAt.
	ScheduleKindOneShot ScheduleKind = "one-shot"
	// ScheduleKindRecurring fires every Interval, subject to the AllowedWeekdays/AllowedHour*
	// eligibility window.
	ScheduleKindRecurring ScheduleKind = "recurring"
)

// ChaosExperimentSchedule describes when a scheduled experiment is allowed to fire.
//
// NOTE: this is a deliberately minimal cron-*like* schedule (a fixed interval plus an optional
// weekday/hour eligibility window), not a full crontab-expression parser — pulling in a cron
// parsing library is out of scope for this pass (this repo's go.mod is hand-written with no
// network access to add a new dependency; see its NOTE comment). It covers the common
// chaos-scheduling need — "run this automatically every 30 minutes, but only Tuesday-Thursday
// business hours so an on-call engineer is awake to watch it" — without that dependency. A real
// cron-expression implementation is a drop-in replacement for NextFireTime's recurring branch.
type ChaosExperimentSchedule struct {
	Kind ScheduleKind

	// FireAt is when a ScheduleKindOneShot schedule fires. Ignored for ScheduleKindRecurring.
	FireAt time.Time

	// Interval is how often a ScheduleKindRecurring schedule is eligible to fire. Ignored for
	// ScheduleKindOneShot.
	Interval time.Duration
	// AllowedWeekdays restricts a recurring schedule to specific days; empty means every day.
	AllowedWeekdays []time.Weekday
	// AllowedHourStart/AllowedHourEnd restrict a recurring schedule to a local-time-of-day
	// window [AllowedHourStart, AllowedHourEnd), each in [0,24]; equal values (including the
	// zero value (0, 0)) mean "no hour restriction — any hour is eligible". Overnight windows
	// (e.g. 22 -> 4) are intentionally not supported by this minimal scheduler; split them into
	// two schedules if needed.
	AllowedHourStart int
	AllowedHourEnd   int
	// Location is the time.Location AllowedWeekdays/AllowedHourStart/AllowedHourEnd are
	// evaluated in. A nil Location defaults to time.UTC, so a schedule's eligibility window is
	// reproducible regardless of the host process's local timezone.
	Location *time.Location
}

// Validate checks that s describes a schedule NextFireTime can evaluate.
func (s ChaosExperimentSchedule) Validate() error {
	switch s.Kind {
	case ScheduleKindOneShot:
		if s.FireAt.IsZero() {
			return fmt.Errorf("chaos experiment schedule: one-shot schedule requires a non-zero FireAt")
		}
	case ScheduleKindRecurring:
		if s.Interval <= 0 {
			return fmt.Errorf("chaos experiment schedule: recurring schedule requires a positive Interval")
		}
		if s.AllowedHourStart < 0 || s.AllowedHourStart > 24 || s.AllowedHourEnd < 0 || s.AllowedHourEnd > 24 {
			return fmt.Errorf("chaos experiment schedule: AllowedHourStart/AllowedHourEnd must each be within [0,24]")
		}
		if s.AllowedHourStart != s.AllowedHourEnd && s.AllowedHourStart > s.AllowedHourEnd {
			return fmt.Errorf("chaos experiment schedule: AllowedHourStart must not be after AllowedHourEnd; overnight windows are not supported by this schedule")
		}
	default:
		return fmt.Errorf("chaos experiment schedule: unknown Kind %q", s.Kind)
	}
	return nil
}

func (s ChaosExperimentSchedule) location() *time.Location {
	if s.Location == nil {
		return time.UTC
	}
	return s.Location
}

// eligible reports whether t falls within this schedule's weekday/hour eligibility window.
// Callers must only call this for ScheduleKindRecurring schedules.
func (s ChaosExperimentSchedule) eligible(t time.Time) bool {
	t = t.In(s.location())

	if len(s.AllowedWeekdays) > 0 {
		matched := false
		for _, day := range s.AllowedWeekdays {
			if t.Weekday() == day {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if s.AllowedHourStart != s.AllowedHourEnd {
		hour := t.Hour()
		if hour < s.AllowedHourStart || hour >= s.AllowedHourEnd {
			return false
		}
	}

	return true
}

// maxRecurringLookaheadSteps bounds how many Interval-sized steps NextFireTime walks forward
// while searching for an eligibility-window match, so a misconfigured schedule whose window can
// never be satisfied fails fast with an error instead of looping effectively forever.
const maxRecurringLookaheadSteps = 100_000

// NextFireTime returns the next time strictly after `after` that s should fire, or false if s
// is a one-shot schedule whose FireAt is not after `after` (it has already fired, or is exactly
// `after`).
func (s ChaosExperimentSchedule) NextFireTime(after time.Time) (time.Time, bool, error) {
	if err := s.Validate(); err != nil {
		return time.Time{}, false, err
	}

	switch s.Kind {
	case ScheduleKindOneShot:
		if s.FireAt.After(after) {
			return s.FireAt, true, nil
		}
		return time.Time{}, false, nil

	case ScheduleKindRecurring:
		candidate := after.Add(s.Interval)
		for step := 0; step < maxRecurringLookaheadSteps; step++ {
			if s.eligible(candidate) {
				return candidate, true, nil
			}
			candidate = candidate.Add(s.Interval)
		}
		return time.Time{}, false, fmt.Errorf(
			"chaos experiment schedule: no eligible fire time found within %d steps of interval %s; check AllowedWeekdays/AllowedHourStart/AllowedHourEnd for an unsatisfiable window",
			maxRecurringLookaheadSteps, s.Interval,
		)

	default:
		return time.Time{}, false, fmt.Errorf("chaos experiment schedule: unknown Kind %q", s.Kind)
	}
}

// ScheduledExperiment binds a ChaosExperimentSchedule to the hypothesis-guarded fault injection
// it should trigger when due.
type ScheduledExperiment struct {
	// ID uniquely identifies this scheduled experiment within one ChaosExperimentScheduler.
	ID                 string
	Schedule           ChaosExperimentSchedule
	Hypothesis         SteadyStateHypothesis
	Injection          FaultInjection
	ExperimentDuration time.Duration
}

func (e ScheduledExperiment) validate() error {
	if e.ID == "" {
		return fmt.Errorf("scheduled experiment: ID is required")
	}
	if err := e.Schedule.Validate(); err != nil {
		return fmt.Errorf("scheduled experiment %q: %w", e.ID, err)
	}
	if err := e.Hypothesis.Validate(); err != nil {
		return fmt.Errorf("scheduled experiment %q: %w", e.ID, err)
	}
	if e.Injection == nil {
		return fmt.Errorf("scheduled experiment %q: Injection must not be nil", e.ID)
	}
	if e.ExperimentDuration <= 0 {
		return fmt.Errorf("scheduled experiment %q: ExperimentDuration must be positive", e.ID)
	}
	return nil
}

// scheduledEntry is the scheduler's internal bookkeeping record for one registered experiment.
type scheduledEntry struct {
	experiment ScheduledExperiment
	nextFireAt time.Time
	lastResult *GuardedExperimentResult
}

// ChaosExperimentScheduler holds a set of ScheduledExperiments and, on each caller-driven Tick,
// runs every experiment whose schedule is due through a HypothesisGuardedExperimentRunner. It is
// safe for concurrent use.
//
// Deliberately caller-driven (no internal goroutine/ticker) rather than self-scheduling: this
// matches cmd/chaos-ebpf-sidecar/main.go's existing pattern of leaving the real dispatch loop
// (its dag.node.dispatch.v1 Redpanda consumer) as an explicit NOTE-documented wiring point, and
// keeps this package free of background-goroutine lifecycle management. Production wiring calls
// Tick from whatever periodic driver main.go ends up using (a time.Ticker started alongside the
// real consumer loop, for example).
type ChaosExperimentScheduler struct {
	runner *HypothesisGuardedExperimentRunner

	mu        sync.Mutex
	scheduled map[string]*scheduledEntry
}

// NewChaosExperimentScheduler constructs a scheduler bound to runner, which actually evaluates
// each due experiment's steady-state hypothesis and injects/rolls back its fault.
func NewChaosExperimentScheduler(runner *HypothesisGuardedExperimentRunner) (*ChaosExperimentScheduler, error) {
	if runner == nil {
		return nil, fmt.Errorf("new chaos experiment scheduler: runner must not be nil")
	}
	return &ChaosExperimentScheduler{runner: runner, scheduled: make(map[string]*scheduledEntry)}, nil
}

// Schedule registers exp and computes its first due fire time relative to now. Scheduling an ID
// that is already registered replaces the previous registration.
func (s *ChaosExperimentScheduler) Schedule(exp ScheduledExperiment, now time.Time) error {
	if err := exp.validate(); err != nil {
		return fmt.Errorf("schedule chaos experiment: %w", err)
	}
	nextFireAt, hasNext, err := exp.Schedule.NextFireTime(now)
	if err != nil {
		return fmt.Errorf("schedule chaos experiment %q: %w", exp.ID, err)
	}
	if !hasNext {
		return fmt.Errorf("schedule chaos experiment %q: schedule has no future fire time as of %s (a one-shot FireAt in the past is refused)", exp.ID, now)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled[exp.ID] = &scheduledEntry{experiment: exp, nextFireAt: nextFireAt}
	return nil
}

// Cancel deregisters a scheduled experiment by ID. Canceling an unknown ID is a no-op.
func (s *ChaosExperimentScheduler) Cancel(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.scheduled, id)
}

// DueExperimentIDs returns the IDs of every scheduled experiment whose nextFireAt is at or
// before now, in deterministic (sorted) order. Exposed separately from Tick so callers/tests can
// inspect what would run without actually running it.
func (s *ChaosExperimentScheduler) DueExperimentIDs(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	due := make([]string, 0)
	for id, entry := range s.scheduled {
		if !entry.nextFireAt.After(now) {
			due = append(due, id)
		}
	}
	sort.Strings(due)
	return due
}

// Tick runs every currently-due scheduled experiment — sequentially, not concurrently, since
// chaos experiments deliberately do not overlap within one sidecar instance, so their blast
// radii and hypothesis polling never interact in confusing ways — through the scheduler's
// HypothesisGuardedExperimentRunner, reschedules recurring experiments' next fire time, and
// removes one-shot experiments that have now fired. Returns results keyed by experiment ID.
func (s *ChaosExperimentScheduler) Tick(ctx context.Context, now time.Time) map[string]*GuardedExperimentResult {
	results := make(map[string]*GuardedExperimentResult)

	for _, id := range s.DueExperimentIDs(now) {
		s.mu.Lock()
		entry, exists := s.scheduled[id]
		s.mu.Unlock()
		if !exists {
			// Canceled between DueExperimentIDs and this iteration.
			continue
		}

		result, err := s.runner.RunGuarded(ctx, entry.experiment.Hypothesis, entry.experiment.Injection, entry.experiment.ExperimentDuration)
		if err != nil {
			// Setup failure (e.g. steady state already violated before injection): record that
			// nothing runnable happened, and still advance/remove the schedule below so a
			// permanently-unhealthy target does not retry-loop every Tick forever unnoticed.
			result = &GuardedExperimentResult{Aborted: true, AbortReason: err.Error(), StartedAt: now, EndedAt: now}
		}
		results[id] = result

		s.mu.Lock()
		switch {
		case entry.experiment.Schedule.Kind == ScheduleKindOneShot:
			delete(s.scheduled, id)
		default:
			if next, hasNext, nextErr := entry.experiment.Schedule.NextFireTime(now); nextErr == nil && hasNext {
				entry.nextFireAt = next
				entry.lastResult = result
			} else {
				// Recurring schedule could not compute a next fire time (should be rare given
				// Validate() already ran at Schedule() time) — remove it rather than leave it
				// permanently due, mirroring the one-shot removal above.
				delete(s.scheduled, id)
			}
		}
		s.mu.Unlock()
	}

	return results
}
