// Package loadgen turns a declarative traffic shape (TrafficProfile) into
// real concurrent goroutine load (ConcurrentLoadGenerator) against a
// protocolclient.ProtocolClient.
package loadgen

import (
	"math"
	"sort"
	"time"
)

// TrafficProfile computes the target concurrency (number of simultaneously
// in-flight calls) as a function of elapsed time since the load phase
// started. ConcurrentLoadGenerator samples this on a ticker and
// spawns/throttles worker goroutines to converge on the returned value.
type TrafficProfile interface {
	// TargetConcurrency returns how many workers should be in flight at
	// elapsed time since the phase began. Implementations must be pure
	// functions of elapsed (no side effects, safe to call concurrently).
	TargetConcurrency(elapsed time.Duration) int

	// Duration is the total length of this phase; the generator stops
	// ramping (and reports the phase complete) once elapsed exceeds it.
	Duration() time.Duration

	// Name identifies the shape for logging/telemetry (e.g. "linear-ramp").
	Name() string
}

// LinearRampProfile increases concurrency at a constant rate from
// StartConcurrency to EndConcurrency over RampDuration, then holds at
// EndConcurrency for the remainder of the phase.
type LinearRampProfile struct {
	StartConcurrency int
	EndConcurrency   int
	RampDuration     time.Duration
	TotalDuration    time.Duration
}

func (p LinearRampProfile) TargetConcurrency(elapsed time.Duration) int {
	if p.RampDuration <= 0 || elapsed >= p.RampDuration {
		return p.EndConcurrency
	}
	fraction := float64(elapsed) / float64(p.RampDuration)
	delta := float64(p.EndConcurrency - p.StartConcurrency)
	return p.StartConcurrency + int(math.Round(delta*fraction))
}

func (p LinearRampProfile) Duration() time.Duration { return p.TotalDuration }
func (p LinearRampProfile) Name() string            { return "linear-ramp" }

// StepProfile holds concurrency at a fixed level for each Step's Duration,
// moving to the next step once the current one elapses. Steps run in
// order; once the last step elapses, concurrency holds at its level for
// the rest of TotalDuration.
type StepProfile struct {
	Steps         []Step
	TotalDuration time.Duration
}

// Step is one plateau of a StepProfile: Concurrency held for Duration.
type Step struct {
	Concurrency int
	Duration    time.Duration
}

func (p StepProfile) TargetConcurrency(elapsed time.Duration) int {
	var cursor time.Duration
	for _, step := range p.Steps {
		cursor += step.Duration
		if elapsed < cursor {
			return step.Concurrency
		}
	}
	if len(p.Steps) == 0 {
		return 0
	}
	return p.Steps[len(p.Steps)-1].Concurrency
}

func (p StepProfile) Duration() time.Duration { return p.TotalDuration }
func (p StepProfile) Name() string            { return "step" }

// SpikeProfile holds BaseConcurrency, jumps to PeakConcurrency for
// PlateauDuration starting at SpikeAt, then returns to BaseConcurrency.
// Useful for testing autoscaling/circuit-breaker reactions to sudden
// bursts.
type SpikeProfile struct {
	BaseConcurrency int
	PeakConcurrency int
	SpikeAt         time.Duration
	PlateauDuration time.Duration
	TotalDuration   time.Duration
}

func (p SpikeProfile) TargetConcurrency(elapsed time.Duration) int {
	if elapsed >= p.SpikeAt && elapsed < p.SpikeAt+p.PlateauDuration {
		return p.PeakConcurrency
	}
	return p.BaseConcurrency
}

func (p SpikeProfile) Duration() time.Duration { return p.TotalDuration }
func (p SpikeProfile) Name() string            { return "spike" }

// WaveProfile oscillates concurrency sinusoidally between MinConcurrency
// and MaxConcurrency with the given Period, useful for soak tests that
// probe how a target behaves under rhythmic load rather than a monotonic
// ramp.
type WaveProfile struct {
	MinConcurrency int
	MaxConcurrency int
	Period         time.Duration
	TotalDuration  time.Duration
}

func (p WaveProfile) TargetConcurrency(elapsed time.Duration) int {
	if p.Period <= 0 {
		return p.MinConcurrency
	}
	phase := 2 * math.Pi * float64(elapsed) / float64(p.Period)
	amplitude := float64(p.MaxConcurrency-p.MinConcurrency) / 2
	midpoint := float64(p.MinConcurrency) + amplitude
	return int(math.Round(midpoint + amplitude*math.Sin(phase)))
}

func (p WaveProfile) Duration() time.Duration { return p.TotalDuration }
func (p WaveProfile) Name() string            { return "wave" }

// ReplayPoint is one sample of a previously recorded concurrency curve,
// e.g. captured from production traffic to replay realistic load shapes.
type ReplayPoint struct {
	ElapsedSince time.Duration
	Concurrency  int
}

// ReplayCurveProfile reproduces a recorded timestamp/concurrency curve by
// linearly interpolating between the two nearest ReplayPoints for a given
// elapsed time. Points do not need to be pre-sorted; NewReplayCurveProfile
// sorts them once at construction.
type ReplayCurveProfile struct {
	points        []ReplayPoint
	totalDuration time.Duration
}

// NewReplayCurveProfile builds a ReplayCurveProfile from recorded points.
// The profile's Duration is the elapsed time of the last point unless
// totalDuration is explicitly larger (in which case concurrency holds flat
// at the last point's value for the remainder).
func NewReplayCurveProfile(points []ReplayPoint, totalDuration time.Duration) *ReplayCurveProfile {
	sorted := make([]ReplayPoint, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ElapsedSince < sorted[j].ElapsedSince })

	duration := totalDuration
	if len(sorted) > 0 && sorted[len(sorted)-1].ElapsedSince > duration {
		duration = sorted[len(sorted)-1].ElapsedSince
	}
	return &ReplayCurveProfile{points: sorted, totalDuration: duration}
}

func (p *ReplayCurveProfile) TargetConcurrency(elapsed time.Duration) int {
	if len(p.points) == 0 {
		return 0
	}
	if elapsed <= p.points[0].ElapsedSince {
		return p.points[0].Concurrency
	}
	last := p.points[len(p.points)-1]
	if elapsed >= last.ElapsedSince {
		return last.Concurrency
	}

	// Find the bracketing pair [before, after] and linearly interpolate.
	for i := 1; i < len(p.points); i++ {
		after := p.points[i]
		if elapsed > after.ElapsedSince {
			continue
		}
		before := p.points[i-1]
		span := after.ElapsedSince - before.ElapsedSince
		if span <= 0 {
			return before.Concurrency
		}
		fraction := float64(elapsed-before.ElapsedSince) / float64(span)
		delta := float64(after.Concurrency - before.Concurrency)
		return before.Concurrency + int(math.Round(delta*fraction))
	}
	return last.Concurrency
}

func (p *ReplayCurveProfile) Duration() time.Duration { return p.totalDuration }
func (p *ReplayCurveProfile) Name() string            { return "replay-curve" }
