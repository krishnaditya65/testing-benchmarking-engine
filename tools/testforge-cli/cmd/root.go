// Package cmd wires up testforge-cli's Cobra command tree. testforge is
// intentionally headless (no TUI, no interactive prompts) because its
// primary caller is CI/CD, not a human at a terminal — see
// tools/testforge-cli/README.md for the GitOps usage pattern this was built
// for.
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Global flags shared by every subcommand.
var (
	// apiGatewayURL is the pipeline-api-gateway base URL testforge talks to.
	// Defaults to the in-cluster CI runner convention; overridden in local
	// dev via --api-gateway-url or TESTFORGE_API_GATEWAY_URL.
	apiGatewayURL string

	// apiToken authenticates the CLI to pipeline-api-gateway's REST
	// submission endpoint (docs/api-specifications/openapi/testforge-cli-submission-endpoint.yaml).
	apiToken string

	// outputFormat controls whether human-readable or machine-readable
	// (json) output is printed, so testforge can be piped into other CI
	// tooling.
	outputFormat string
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "testforge",
		Short: "GitOps-native CLI for the distributed testing & benchmarking platform",
		Long: `testforge submits declarative pipeline definitions to pipeline-api-gateway
and validates them against the DAG schema before they're committed or merged.

It is designed to run headless inside CI/CD (GitHub Actions, GitLab CI,
Jenkins) as the GitOps enforcement point for load/chaos/functional test
pipelines defined as YAML alongside application code.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&apiGatewayURL, "api-gateway-url", envOrDefault("TESTFORGE_API_GATEWAY_URL", "https://pipeline-api-gateway.testforge.internal"),
		"Base URL of pipeline-api-gateway's REST submission endpoint")
	root.PersistentFlags().StringVar(&apiToken, "api-token", envOrDefault("TESTFORGE_API_TOKEN", ""),
		"Bearer token used to authenticate against pipeline-api-gateway (falls back to TESTFORGE_API_TOKEN)")
	root.PersistentFlags().StringVar(&outputFormat, "output", "text", `Output format: "text" or "json"`)

	root.AddCommand(newRunCmd())
	root.AddCommand(newValidateCmd())

	return root
}

// Execute runs the CLI and returns the process exit code. main.go calls
// os.Exit with this value directly rather than letting Cobra call
// os.Exit itself, so that cmd/run.go's quality-gate mapping (see
// internal/cicdintegration/quality_gate_exit_code_mapper.go) is the single
// source of truth for non-zero exit codes.
func Execute() int {
	root := newRootCmd()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "testforge:", err)
		return 1
	}
	return exitCode
}

// exitCode is set by subcommands (primarily `run`) that need to propagate a
// specific quality-gate exit code rather than the generic 0/1 success/error
// split.
var exitCode int

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
