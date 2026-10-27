package chaos

import (
	"context"
	"fmt"
	"sort"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/safety"
)

// PodRef identifies a candidate pod for fault injection without depending on a specific
// Kubernetes client library type, so this package stays testable without a live cluster.
type PodRef struct {
	Namespace string
	Name      string
	UID       string
	// Labels is the pod's label set, used for LabelSelector matching below.
	Labels map[string]string
}

// LabelSelector is a minimal, dependency-free equivalent of metav1.LabelSelector: a pod
// matches only if every entry in MatchLabels is present with an equal value. Empty selectors
// intentionally match nothing rather than everything, since an accidentally-empty selector
// paired with a destructive pod killer is exactly the kind of mistake this type exists to
// prevent from silently doing "kill everything".
type LabelSelector struct {
	MatchLabels map[string]string
}

// Matches reports whether pod satisfies s. An empty/nil selector matches no pods — callers
// that genuinely want "all pods" must say so explicitly by matching a label every candidate
// pod carries (e.g. a namespace-scoped selector), never by omission.
func (s LabelSelector) Matches(pod PodRef) bool {
	if len(s.MatchLabels) == 0 {
		return false
	}
	for key, want := range s.MatchLabels {
		got, ok := pod.Labels[key]
		if !ok || got != want {
			return false
		}
	}
	return true
}

// SelectMatchingPods filters candidates down to the ones matched by selector, preserving
// input order for deterministic behavior in tests and logs.
func SelectMatchingPods(candidates []PodRef, selector LabelSelector) []PodRef {
	matched := make([]PodRef, 0, len(candidates))
	for _, pod := range candidates {
		if selector.Matches(pod) {
			matched = append(matched, pod)
		}
	}
	return matched
}

// BlastRadiusLimit caps how much damage a single chaos action may do, evaluated against the
// full candidate population (not just the label-matched subset), so a badly scoped label
// selector cannot be used to bypass the cap.
type BlastRadiusLimit struct {
	// MaxBlastRadiusPercent caps kills as a percentage, 0-100, of the total candidate pod
	// count. Zero means "no percent-based cap" (MaxBlastRadiusCount, if set, still applies).
	MaxBlastRadiusPercent float64
	// MaxBlastRadiusCount caps kills as an absolute pod count. Zero means "no count-based cap".
	MaxBlastRadiusCount int
}

func (l BlastRadiusLimit) validate() error {
	if l.MaxBlastRadiusPercent == 0 && l.MaxBlastRadiusCount == 0 {
		return fmt.Errorf("blast radius limit: at least one of MaxBlastRadiusPercent or MaxBlastRadiusCount must be set; refusing an unbounded pod killer")
	}
	if l.MaxBlastRadiusPercent < 0 || l.MaxBlastRadiusPercent > 100 {
		return fmt.Errorf("blast radius limit: MaxBlastRadiusPercent must be within [0,100], got %f", l.MaxBlastRadiusPercent)
	}
	if l.MaxBlastRadiusCount < 0 {
		return fmt.Errorf("blast radius limit: MaxBlastRadiusCount must not be negative")
	}
	return nil
}

// allowedKillCount computes the maximum number of pods that may be killed out of
// totalCandidates given the configured caps, taking the stricter (smaller) of the two when
// both are set.
func (l BlastRadiusLimit) allowedKillCount(totalCandidates int) int {
	limit := totalCandidates // effectively "unbounded" until a cap narrows it
	if l.MaxBlastRadiusPercent > 0 {
		byPercent := int(l.MaxBlastRadiusPercent / 100 * float64(totalCandidates))
		if byPercent < limit {
			limit = byPercent
		}
	}
	if l.MaxBlastRadiusCount > 0 && l.MaxBlastRadiusCount < limit {
		limit = l.MaxBlastRadiusCount
	}
	return limit
}

// PodSelectionStrategy decides which pods among allCandidates are eligible targets for a chaos
// action, independent of the blast-radius cap BlastRadiusControlledPodKiller enforces
// afterward. LabelSelectorPodSelectionStrategy is the default — it reproduces the original
// label-selector-only behavior exactly. internal/targeting's DependencyAwarePodSelectionStrategy
// is an alternate strategy that additionally requires real upstream/downstream dependency
// evidence (from a service dependency graph derived from telemetry-query-api trace data) before
// a pod is eligible, so a chaos experiment can guarantee it is exercising a real failover path
// rather than killing an arbitrary label-matched pod.
type PodSelectionStrategy interface {
	// SelectCandidates returns the subset of allCandidates this strategy considers eligible for
	// targeting. It must not mutate allCandidates. Returning an empty slice (not an error) is
	// the correct response to "nothing is eligible" — mirroring LabelSelector.Matches's
	// default-safe posture of matching nothing rather than everything on ambiguous input.
	SelectCandidates(ctx context.Context, allCandidates []PodRef) ([]PodRef, error)
}

// LabelSelectorPodSelectionStrategy adapts a plain LabelSelector to the PodSelectionStrategy
// interface. This is the default strategy: BlastRadiusControlledPodKiller.PlanKillSet and
// KillMatching (the pre-existing, label-selector-only entry points) are implemented in terms of
// this strategy so their documented behavior is unchanged by the addition of
// PodSelectionStrategy.
type LabelSelectorPodSelectionStrategy struct {
	Selector LabelSelector
}

// SelectCandidates never returns an error — label matching is a pure, always-succeeding
// operation — which is what lets PlanKillSet/KillMatching wrap this strategy without changing
// their pre-existing error-free signatures.
func (s LabelSelectorPodSelectionStrategy) SelectCandidates(ctx context.Context, allCandidates []PodRef) ([]PodRef, error) {
	return SelectMatchingPods(allCandidates, s.Selector), nil
}

// PodKillTarget is the seam between this package's blast-radius-safe pod selection and the
// actual privileged Kubernetes delete-pod call.
//
// NOTE: A real implementation wraps a k8s.io/client-go kubernetes.Interface constructed from
// in-cluster config (rest.InClusterConfig()) with RBAC permission to `delete` pods, calling
// clientset.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{...}) — typically
// with a GracePeriodSeconds tuned per experiment so this behaves like a clean termination
// (SIGTERM-then-SIGKILL) rather than a hard kill, unless the experiment specifically wants to
// test ungraceful pod loss.
type PodKillTarget interface {
	// KillPod deletes the given pod from the cluster. Implementations should treat "already
	// gone" (NotFound) as success, since a pod dying between selection and kill is expected.
	KillPod(ctx context.Context, pod PodRef) error
}

// BlastRadiusControlledPodKiller kills a label-selected subset of pods while enforcing a hard
// cap on how many pods (absolute or percentage) may be killed in one action. The cap is
// enforced BEFORE any KillPod call is issued — if honoring a request would exceed the cap, the
// entire request is refused rather than partially executed.
type BlastRadiusControlledPodKiller struct {
	killer PodKillTarget
	limit  BlastRadiusLimit
}

// NewBlastRadiusControlledPodKiller constructs a pod killer bound to a fixed safety limit.
// Returns an error if limit is missing both caps, since an uncapped destructive pod killer is
// never a valid configuration for this type.
func NewBlastRadiusControlledPodKiller(killer PodKillTarget, limit BlastRadiusLimit) (*BlastRadiusControlledPodKiller, error) {
	if killer == nil {
		return nil, fmt.Errorf("new blast radius controlled pod killer: PodKillTarget must not be nil")
	}
	if err := limit.validate(); err != nil {
		return nil, fmt.Errorf("new blast radius controlled pod killer: %w", err)
	}
	return &BlastRadiusControlledPodKiller{killer: killer, limit: limit}, nil
}

// PlanKillSet computes, without killing anything, which pods a KillMatching call would target
// and whether it would be within the blast-radius cap. Exposed separately from KillMatching so
// operators/control-plane previews can show "this would kill N of M pods" before committing.
// This is the original, label-selector-only entry point; it is now implemented in terms of
// PlanKillSetWithStrategy wrapping selector in LabelSelectorPodSelectionStrategy, so its
// behavior and signature are unchanged.
func (k *BlastRadiusControlledPodKiller) PlanKillSet(allCandidates []PodRef, selector LabelSelector) (toKill []PodRef, allowed bool, allowedCount int) {
	// LabelSelectorPodSelectionStrategy.SelectCandidates never errors, so discarding the error
	// here is safe — see that method's doc comment.
	toKill, allowed, allowedCount, _ = k.PlanKillSetWithStrategy(context.Background(), allCandidates, LabelSelectorPodSelectionStrategy{Selector: selector})
	return toKill, allowed, allowedCount
}

// PlanKillSetWithStrategy generalizes PlanKillSet to any PodSelectionStrategy, not just a plain
// LabelSelector — e.g. internal/targeting's DependencyAwarePodSelectionStrategy. It returns an
// error only if strategy.SelectCandidates itself fails (e.g. a dependency-graph fetch error);
// "the selected set exceeds the blast-radius cap" is still reported via the allowed return
// value, not an error, matching PlanKillSet's existing convention.
func (k *BlastRadiusControlledPodKiller) PlanKillSetWithStrategy(ctx context.Context, allCandidates []PodRef, strategy PodSelectionStrategy) (toKill []PodRef, allowed bool, allowedCount int, err error) {
	matched, err := strategy.SelectCandidates(ctx, allCandidates)
	if err != nil {
		return nil, false, 0, fmt.Errorf("plan kill set: pod selection strategy failed: %w", err)
	}

	allowedCount = k.limit.allowedKillCount(len(allCandidates))
	if len(matched) > allowedCount {
		return matched, false, allowedCount, nil
	}
	return matched, true, allowedCount, nil
}

// KillMatching selects pods from allCandidates matching selector and kills every matched pod,
// but only if the matched count is within the configured blast-radius cap; otherwise it
// refuses the entire action with a descriptive error and kills nothing. On partial KillPod
// failure it kills as many of the approved set as possible and returns a combined error
// describing which pods failed. This is the original, label-selector-only entry point; it is
// now implemented in terms of KillMatchingWithStrategy, so its behavior and signature are
// unchanged.
func (k *BlastRadiusControlledPodKiller) KillMatching(ctx context.Context, allCandidates []PodRef, selector LabelSelector) error {
	return k.KillMatchingWithStrategy(ctx, allCandidates, LabelSelectorPodSelectionStrategy{Selector: selector})
}

// KillMatchingWithStrategy generalizes KillMatching to any PodSelectionStrategy. This is the
// single real entry point both KillMatching and dependency-aware callers funnel through, which
// is why the global-chaos-circuit-breaker check and the blast-radius cap enforcement both live
// here rather than being duplicated per strategy.
func (k *BlastRadiusControlledPodKiller) KillMatchingWithStrategy(ctx context.Context, allCandidates []PodRef, strategy PodSelectionStrategy) error {
	// Global kill-switch check: this is the entry point for the "Start"-equivalent action of
	// this injector (see internal/safety.GlobalChaosCircuitBreaker's doc comment). A tripped
	// breaker refuses every new pod kill, independent of and in addition to the blast-radius
	// cap below.
	if err := safety.Global().Check(ctx); err != nil {
		return fmt.Errorf("blast radius controlled pod killer: %w", err)
	}

	matched, allowed, allowedCount, err := k.PlanKillSetWithStrategy(ctx, allCandidates, strategy)
	if err != nil {
		return fmt.Errorf("blast radius controlled pod killer: %w", err)
	}
	if !allowed {
		return fmt.Errorf(
			"blast radius controlled pod killer: refusing to kill %d matched pod(s); exceeds blast radius cap of %d (out of %d total candidates, limit=%+v)",
			len(matched), allowedCount, len(allCandidates), k.limit,
		)
	}
	if len(matched) == 0 {
		return nil
	}

	// Deterministic ordering makes kill order reproducible across runs given the same input,
	// which matters for chaos-experiment repeatability.
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].Namespace != matched[j].Namespace {
			return matched[i].Namespace < matched[j].Namespace
		}
		return matched[i].Name < matched[j].Name
	})

	var failures []string
	for _, pod := range matched {
		if err := k.killer.KillPod(ctx, pod); err != nil {
			failures = append(failures, fmt.Sprintf("%s/%s: %v", pod.Namespace, pod.Name, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("blast radius controlled pod killer: %d of %d kills failed: %v", len(failures), len(matched), failures)
	}
	return nil
}
