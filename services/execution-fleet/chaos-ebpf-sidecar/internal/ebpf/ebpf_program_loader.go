// Package ebpf describes how compiled eBPF programs get loaded into the kernel and attached to
// interfaces/cgroups on behalf of the chaos primitives in internal/chaos, and provides a
// simulated implementation for local development and tests. This package does not itself
// contain any eBPF C source or compiled .o objects — see CiliumEbpfProgramLoader's doc comment
// for what a real implementation needs.
package ebpf

import (
	"context"
	"fmt"
	"sync"
)

// AttachPointKind identifies the kind of kernel hook an eBPF program is attached to, mirroring
// the attach types cilium/ebpf's link package supports (link.AttachTCX, link.AttachCGroupInet,
// etc.) at a level of abstraction this package's callers care about.
type AttachPointKind string

const (
	// AttachPointTrafficControlIngress attaches a classifier/action program to an interface's
	// ingress qdisc (used for e.g. inbound TCP-reset injection).
	AttachPointTrafficControlIngress AttachPointKind = "tc-ingress"
	// AttachPointTrafficControlEgress attaches to an interface's egress qdisc.
	AttachPointTrafficControlEgress AttachPointKind = "tc-egress"
	// AttachPointCgroupSockOps attaches a sock_ops program scoped to a cgroup, used for
	// per-pod (cgroup-scoped) latency/loss shaping without touching the whole node interface.
	AttachPointCgroupSockOps AttachPointKind = "cgroup-sockops"
)

// AttachPoint fully identifies where a loaded program should be attached.
type AttachPoint struct {
	Kind AttachPointKind
	// InterfaceName is required for AttachPointTrafficControl{Ingress,Egress}.
	InterfaceName string
	// CgroupPath is required for AttachPointCgroupSockOps (e.g. a pod's
	// /sys/fs/cgroup/... path).
	CgroupPath string
}

func (p AttachPoint) validate() error {
	switch p.Kind {
	case AttachPointTrafficControlIngress, AttachPointTrafficControlEgress:
		if p.InterfaceName == "" {
			return fmt.Errorf("attach point %s: InterfaceName is required", p.Kind)
		}
	case AttachPointCgroupSockOps:
		if p.CgroupPath == "" {
			return fmt.Errorf("attach point %s: CgroupPath is required", p.Kind)
		}
	default:
		return fmt.Errorf("attach point: unknown kind %q", p.Kind)
	}
	return nil
}

// ProgramSpec describes an eBPF program to be loaded, independent of how it is ultimately
// compiled/embedded (real implementations typically embed a .o/.bpf.o built by bpf2go from C
// source via a Go //go:generate directive).
type ProgramSpec struct {
	// Name is a human-readable identifier for logs/telemetry, e.g. "tcp-reset-injector".
	Name string
	// ObjectFilePath is the path to the compiled BPF object file. Unused by
	// SimulatedProgramLoader; required by CiliumEbpfProgramLoader.
	ObjectFilePath string
	// SectionName selects which program within the object file to load (cilium/ebpf programs
	// can contain multiple named sections, e.g. "tc/ingress_reset").
	SectionName string
}

func (s ProgramSpec) validate() error {
	if s.Name == "" {
		return fmt.Errorf("program spec: Name is required")
	}
	return nil
}

// ProgramHandle identifies one successfully-loaded program instance, returned by Load and
// consumed by Attach/Detach/Stats.
type ProgramHandle string

// ProgramStats reports basic execution counters for an attached program. Real fields depend on
// the eBPF map layout the program exports; these are the ones the chaos package's telemetry
// currently cares about.
type ProgramStats struct {
	// PacketsMatched is how many packets the program's classifier logic matched (regardless of
	// what action, if any, was taken).
	PacketsMatched uint64
	// PacketsDropped is how many matched packets were dropped (packet-loss simulation).
	PacketsDropped uint64
	// ResetsInjected is how many matched TCP connections received an injected RST.
	ResetsInjected uint64
}

// EbpfProgramLoader is the lifecycle contract for loading a compiled eBPF program into the
// kernel, attaching it to a hook point, detaching it, and reading back its runtime stats.
// Implementations must be safe for concurrent use.
type EbpfProgramLoader interface {
	// Load reads/verifies/loads spec into the kernel (or, for a simulated loader, into
	// in-memory state) and returns a handle for subsequent calls. Loading does not attach the
	// program to anything yet.
	Load(ctx context.Context, spec ProgramSpec) (ProgramHandle, error)
	// Attach binds a previously loaded program to point. A program may be attached to at most
	// one AttachPoint at a time in this model; attaching an already-attached handle to a new
	// point first detaches it from the old one.
	Attach(ctx context.Context, handle ProgramHandle, point AttachPoint) error
	// Detach unbinds handle from wherever it is currently attached. Detaching an unattached
	// handle is a no-op, not an error.
	Detach(ctx context.Context, handle ProgramHandle) error
	// Stats returns the current runtime counters for handle.
	Stats(ctx context.Context, handle ProgramHandle) (ProgramStats, error)
}

// loadedProgram is SimulatedProgramLoader's bookkeeping record for one Load call.
type loadedProgram struct {
	spec     ProgramSpec
	attached bool
	point    AttachPoint
	stats    ProgramStats
}

// SimulatedProgramLoader is an in-memory EbpfProgramLoader that tracks loaded/attached program
// state without touching the kernel. It exists so local development, unit tests, and CI can
// exercise the chaos package's control flow (load -> attach -> observe stats -> detach) on a
// laptop with no CAP_BPF, no compiled .o files, and no real network namespace access.
type SimulatedProgramLoader struct {
	mu       sync.Mutex
	programs map[ProgramHandle]*loadedProgram
	nextID   uint64
}

// NewSimulatedProgramLoader constructs an empty simulated loader.
func NewSimulatedProgramLoader() *SimulatedProgramLoader {
	return &SimulatedProgramLoader{programs: make(map[ProgramHandle]*loadedProgram)}
}

func (l *SimulatedProgramLoader) Load(ctx context.Context, spec ProgramSpec) (ProgramHandle, error) {
	if err := spec.validate(); err != nil {
		return "", fmt.Errorf("simulated ebpf load: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.nextID++
	handle := ProgramHandle(fmt.Sprintf("sim-%s-%d", spec.Name, l.nextID))
	l.programs[handle] = &loadedProgram{spec: spec}
	return handle, nil
}

func (l *SimulatedProgramLoader) Attach(ctx context.Context, handle ProgramHandle, point AttachPoint) error {
	if err := point.validate(); err != nil {
		return fmt.Errorf("simulated ebpf attach: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	program, ok := l.programs[handle]
	if !ok {
		return fmt.Errorf("simulated ebpf attach: unknown program handle %q; was it loaded via Load?", handle)
	}
	program.attached = true
	program.point = point
	return nil
}

func (l *SimulatedProgramLoader) Detach(ctx context.Context, handle ProgramHandle) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	program, ok := l.programs[handle]
	if !ok {
		// Detaching a handle that was never loaded is a caller bug, unlike detaching an
		// unattached-but-loaded handle, which is fine.
		return fmt.Errorf("simulated ebpf detach: unknown program handle %q", handle)
	}
	program.attached = false
	program.point = AttachPoint{}
	return nil
}

func (l *SimulatedProgramLoader) Stats(ctx context.Context, handle ProgramHandle) (ProgramStats, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	program, ok := l.programs[handle]
	if !ok {
		return ProgramStats{}, fmt.Errorf("simulated ebpf stats: unknown program handle %q", handle)
	}
	return program.stats, nil
}

// RecordSimulatedActivity lets test/dev code fabricate plausible stats for handle, since a
// simulated loader has no real traffic flowing through it to count. Not part of the
// EbpfProgramLoader interface.
func (l *SimulatedProgramLoader) RecordSimulatedActivity(handle ProgramHandle, matched, dropped, resets uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	program, ok := l.programs[handle]
	if !ok {
		return fmt.Errorf("simulated ebpf record activity: unknown program handle %q", handle)
	}
	program.stats.PacketsMatched += matched
	program.stats.PacketsDropped += dropped
	program.stats.ResetsInjected += resets
	return nil
}

// CiliumEbpfProgramLoader is intended to be the real EbpfProgramLoader backend, built on
// github.com/cilium/ebpf.
//
// NOTE: This is intentionally left unimplemented in this starter repo. A real implementation
// needs:
//   - Compiled BPF object files (.o), typically produced by bpf2go from C source targeting the
//     TC/cgroup hooks declared above, checked into the repo or built by a dedicated CI step
//     with clang/llvm available (not part of a normal `go build`).
//   - github.com/cilium/ebpf's ebpf.NewCollection/ebpf.LoadCollectionSpec to load the object
//     file's programs and maps into the kernel.
//   - github.com/cilium/ebpf/link to create the actual attachment (link.AttachTCX for TC
//     hooks, link.AttachCgroup for cgroup/sock_ops hooks), which must be kept open (not
//     garbage collected) for the attachment to remain active — Detach must Close() the link.
//   - The process to run with CAP_BPF, CAP_NET_ADMIN, and CAP_SYS_ADMIN (kernel-version
//     dependent), typically via a Kubernetes pod SecurityContext with those capabilities
//     added rather than full `privileged: true` where the kernel version allows it.
//   - Reading ProgramStats back out of the program's BPF maps (e.g. a PERCPU_ARRAY counter
//     map) via ebpf.Map.Lookup, summed across CPUs.
//
// None of that is implemented here; this type exists so the production wiring point in
// cmd/chaos-ebpf-sidecar/main.go is visible and named correctly even though it cannot be
// constructed yet.
type CiliumEbpfProgramLoader struct{}
