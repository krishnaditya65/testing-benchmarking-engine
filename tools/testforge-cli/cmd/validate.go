package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli/internal/cicdintegration"
	"github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli/internal/pipelineconfig"
)

func newValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate [pipeline.yaml]...",
		Short: "Lint one or more pipeline definitions against the DAG schema, without submitting them",
		Long: `validate parses each given pipeline YAML file and checks it against the same
DAG rules dag-workflow-scheduler enforces at submission time: known node
types, no duplicate node ids, no dangling dependsOn references, and no
dependency cycles.

Intended for use as a pre-commit hook or a fast CI lint step that runs
before "testforge run", so a malformed pipeline never reaches
pipeline-api-gateway in the first place.`,
		Example: `  testforge validate ./pipelines/checkout-load-test.yaml
  testforge validate ./pipelines/*.yaml`,
		Args: cobra.MinimumNArgs(1),
		RunE: runValidateCmd,
	}
}

func runValidateCmd(cmd *cobra.Command, paths []string) error {
	totalIssues := 0

	for _, path := range paths {
		def, err := pipelineconfig.Parse(path)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "✗ %s: %v\n", path, err)
			totalIssues++
			continue
		}

		issues := def.ValidateDAG()
		if len(issues) == 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "✓ %s: %q is a valid pipeline (%d nodes)\n", path, def.Metadata.Name, len(def.Spec.Nodes))
			continue
		}

		fmt.Fprintf(cmd.ErrOrStderr(), "✗ %s: %d issue(s)\n", path, len(issues))
		for _, issue := range issues {
			fmt.Fprintf(cmd.ErrOrStderr(), "    - %s\n", issue)
		}
		totalIssues += len(issues)
	}

	if totalIssues > 0 {
		exitCode = cicdintegration.ExitGenericError
		return fmt.Errorf("%d pipeline definition issue(s) found across %d file(s)", totalIssues, len(paths))
	}

	exitCode = cicdintegration.ExitSuccess
	return nil
}
