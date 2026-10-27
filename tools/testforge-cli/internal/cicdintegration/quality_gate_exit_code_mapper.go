// Package cicdintegration translates pipeline-api-gateway's quality-gate
// results into process exit codes, which is the entire mechanism by which
// GitHub Actions/GitLab CI/Jenkins learn a pipeline run regressed. CI
// runners only understand exit codes — they have no concept of a "quality
// gate" — so this mapping is the load-bearing contract between the
// platform's domain model and generic CI tooling.
package cicdintegration

// QualityGateResult mirrors the QualityGateResult GraphQL type returned by
// pipeline-api-gateway once a PipelineRun completes (see
// docs/api-specifications/pipeline-api-gateway.graphql).
type QualityGateResult struct {
	Name      string
	Passed    bool
	Severity  string // "blocking" | "warning"
	Metric    string
	Observed  string
	Threshold string
}

// Exit codes are deliberately spread out rather than collapsed to a single
// "1 = failure" so a CI log or a `$?` check downstream of `testforge run`
// can distinguish *why* the build failed without re-parsing CLI output.
const (
	// ExitSuccess: every blocking quality gate passed (warning gates may
	// still have failed; see WarningGateFailed below for that distinction).
	ExitSuccess = 0

	// ExitGenericError: the CLI itself failed before or independent of any
	// quality-gate evaluation (network error, bad auth, malformed pipeline
	// YAML that slipped past `testforge validate`).
	ExitGenericError = 1

	// ExitBlockingGateFailed: at least one gate with severity "blocking"
	// failed (e.g. P99 latency exceeded its assertion, DB consistency
	// check found orphaned rows). This is the code CI branch-protection
	// rules should treat as "must fix before merge".
	ExitBlockingGateFailed = 10

	// ExitWarningGateFailed: only "warning"-severity gates failed; every
	// blocking gate passed. Builds configured to be strict can still fail
	// on this code, but the default GitHub Actions/GitLab CI snippets in
	// tools/testforge-cli/README.md treat it as a non-fatal annotation.
	ExitWarningGateFailed = 11

	// ExitPipelineRunErrored: the PipelineRun itself errored out before
	// quality gates could even be evaluated (e.g. environment-lifecycle-manager
	// failed to provision the ephemeral namespace, or a DAG node crashed
	// mid-execution). Distinct from a failed assertion — this means the
	// test never actually ran to completion.
	ExitPipelineRunErrored = 20
)

// MapExitCode inspects a completed run's quality-gate results (plus whether
// the run itself errored) and returns the process exit code `testforge run`
// should terminate with. It is pure and side-effect-free so it's covered by
// unit tests independent of any HTTP call to pipeline-api-gateway.
func MapExitCode(runErrored bool, gates []QualityGateResult) int {
	if runErrored {
		return ExitPipelineRunErrored
	}

	sawBlockingFailure := false
	sawWarningFailure := false

	for _, g := range gates {
		if g.Passed {
			continue
		}
		switch g.Severity {
		case "blocking":
			sawBlockingFailure = true
		default:
			sawWarningFailure = true
		}
	}

	switch {
	case sawBlockingFailure:
		return ExitBlockingGateFailed
	case sawWarningFailure:
		return ExitWarningGateFailed
	default:
		return ExitSuccess
	}
}

// FailedGateNames returns the names of every failed gate, blocking or
// warning, in the order they were reported — used to render the CI-friendly
// summary testforge run prints before exiting.
func FailedGateNames(gates []QualityGateResult) []string {
	var names []string
	for _, g := range gates {
		if !g.Passed {
			names = append(names, g.Name)
		}
	}
	return names
}
