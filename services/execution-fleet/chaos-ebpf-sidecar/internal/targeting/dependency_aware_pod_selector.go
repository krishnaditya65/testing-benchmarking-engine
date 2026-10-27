// dependency_aware_pod_selector.go implements chaos.PodSelectionStrategy against a
// ServiceDependencyGraph: it picks WHICH pod to target for
// chaos.BlastRadiusControlledPodKiller based on real upstream/downstream dependency edges
// rather than a static label selector alone, so a pod-kill experiment can guarantee it is
// exercising a real failover path instead of killing an arbitrary label-matched pod that
// nothing actually depends on.
package targeting

import (
	"context"
	"fmt"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/chaos"
)

// DependencyAwarePodSelectionStrategy implements chaos.PodSelectionStrategy. It restricts
// eligible pods to those belonging to a service that has at least MinDirectDependents distinct
// direct dependents in Graph — e.g. "only target replicas of a service that has >= 2 direct
// dependents, to guarantee we're testing real failover paths" — and optionally further narrows
// by InnerSelector, the same way the platform's other pod-targeting always has.
type DependencyAwarePodSelectionStrategy struct {
	// Graph is a pre-fetched ServiceDependencyGraph (via ServiceDependencyGraphClient). Fetching
	// is intentionally not done inside SelectCandidates: building the graph requires sampling
	// and correlating multiple traces, which is comparatively expensive and has its own
	// staleness/refresh-cadence concerns, so callers fetch it once (e.g. per chaos-experiment
	// dispatch, not per SelectCandidates call) and pass the result in.
	Graph ServiceDependencyGraph
	// ServiceLabelKey is the pod-label key whose value identifies which logical service a pod
	// belongs to (e.g. "app.kubernetes.io/name"), so this strategy can map a chaos.PodRef to the
	// service name it looks up in Graph.
	ServiceLabelKey string
	// MinDirectDependents is the minimum number of distinct upstream services that must call a
	// service before a pod belonging to that service becomes eligible for targeting. The
	// platform's typical use is MinDirectDependents=2: a service with only one caller can still
	// be a meaningful failover test, but requiring at least two is what specifically guarantees
	// the experiment is probing a shared dependency with more than one real failover path to
	// observe, rather than a single caller's isolated retry/circuit-breaker behavior.
	MinDirectDependents int
	// InnerSelector, if non-empty (has at least one entry in MatchLabels), further narrows
	// eligible pods by label on top of the dependency-graph eligibility check — e.g. still
	// respecting a namespace/environment scoping selector. A zero-value LabelSelector skips this
	// narrowing and considers every pod belonging to an eligible service.
	InnerSelector chaos.LabelSelector
}

// SelectCandidates implements chaos.PodSelectionStrategy.
func (s DependencyAwarePodSelectionStrategy) SelectCandidates(ctx context.Context, allCandidates []chaos.PodRef) ([]chaos.PodRef, error) {
	if s.ServiceLabelKey == "" {
		return nil, fmt.Errorf("dependency aware pod selection strategy: ServiceLabelKey is required")
	}
	if s.MinDirectDependents < 1 {
		return nil, fmt.Errorf("dependency aware pod selection strategy: MinDirectDependents must be >= 1 (0 would make every service in the graph eligible, defeating the point of this strategy)")
	}

	eligibleServices := s.Graph.ServicesWithAtLeastNDirectDependents(s.MinDirectDependents)
	if len(eligibleServices) == 0 {
		// No service in the graph meets the dependent-count bar — nothing is eligible. This
		// mirrors chaos.LabelSelector.Matches's documented default-safe posture: match nothing
		// rather than everything on ambiguous/insufficient input, not an error condition.
		return []chaos.PodRef{}, nil
	}
	eligibleServiceSet := make(map[string]struct{}, len(eligibleServices))
	for _, service := range eligibleServices {
		eligibleServiceSet[service] = struct{}{}
	}

	selected := make([]chaos.PodRef, 0)
	for _, pod := range allCandidates {
		serviceName, ok := pod.Labels[s.ServiceLabelKey]
		if !ok || serviceName == "" {
			continue
		}
		if _, eligible := eligibleServiceSet[serviceName]; !eligible {
			continue
		}
		if len(s.InnerSelector.MatchLabels) > 0 && !s.InnerSelector.Matches(pod) {
			continue
		}
		selected = append(selected, pod)
	}
	return selected, nil
}

// EligibleServices returns which services in s.Graph currently meet s.MinDirectDependents,
// independent of allCandidates. Exposed separately from SelectCandidates so operators/control-
// plane previews can show "these are the services this strategy would consider targeting" —
// mirroring chaos.BlastRadiusControlledPodKiller.PlanKillSet's preview-before-acting pattern —
// without needing a live pod candidate list on hand.
func (s DependencyAwarePodSelectionStrategy) EligibleServices() []string {
	return s.Graph.ServicesWithAtLeastNDirectDependents(s.MinDirectDependents)
}
