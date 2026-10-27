package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli/internal/cicdintegration"
	"github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli/internal/pipelineconfig"
)

var (
	runPipelinePath string
	runEnvironment  string
	runWaitTimeout  time.Duration
)

func newRunCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "run",
		Short: "Submit a pipeline definition to pipeline-api-gateway and wait for its result",
		Example: `  testforge run --pipeline ./pipelines/checkout-load-test.yaml --environment staging
  testforge run --pipeline ./pipelines/checkout-load-test.yaml --environment production --wait-timeout 30m`,
		RunE: runRunCmd,
	}

	c.Flags().StringVar(&runPipelinePath, "pipeline", "", "Path to a pipeline definition YAML file (required)")
	c.Flags().StringVar(&runEnvironment, "environment", "", `Target environment: e.g. "staging", "production" (required)`)
	c.Flags().DurationVar(&runWaitTimeout, "wait-timeout", 20*time.Minute, "How long to poll pipeline-api-gateway for the run's terminal status before giving up")
	_ = c.MarkFlagRequired("pipeline")
	_ = c.MarkFlagRequired("environment")

	return c
}

func runRunCmd(cmd *cobra.Command, args []string) error {
	def, err := pipelineconfig.Parse(runPipelinePath)
	if err != nil {
		exitCode = cicdintegration.ExitGenericError
		return err
	}

	// Fail fast locally before ever making a network call — the same check
	// `testforge validate` runs in a pre-commit hook.
	if issues := def.ValidateDAG(); len(issues) > 0 {
		exitCode = cicdintegration.ExitGenericError
		fmt.Fprintf(cmd.ErrOrStderr(), "testforge: %s failed DAG validation:\n", runPipelinePath)
		for _, issue := range issues {
			fmt.Fprintf(cmd.ErrOrStderr(), "  - %s\n", issue)
		}
		return fmt.Errorf("%d validation issue(s) found", len(issues))
	}

	submission := pipelineRunSubmission{
		PipelineName: def.Metadata.Name,
		Environment:  runEnvironment,
		Definition:   def,
	}

	fmt.Fprintf(cmd.OutOrStdout(), "testforge: submitting pipeline %q to %s (environment=%s)\n",
		def.Metadata.Name, apiGatewayURL, runEnvironment)

	result, err := submitAndAwaitPipelineRun(submission, runWaitTimeout)
	if err != nil {
		exitCode = cicdintegration.ExitGenericError
		return err
	}

	code := cicdintegration.MapExitCode(result.RunErrored, result.QualityGates)
	exitCode = code

	printRunSummary(cmd, result)

	if code != cicdintegration.ExitSuccess {
		failed := cicdintegration.FailedGateNames(result.QualityGates)
		return fmt.Errorf("pipeline run %s did not satisfy its quality gates: %v", result.RunID, failed)
	}
	return nil
}

// pipelineRunSubmission is the request body posted to
// POST /v1/pipeline-runs, matching
// docs/api-specifications/openapi/testforge-cli-submission-endpoint.yaml.
type pipelineRunSubmission struct {
	PipelineName string                             `json:"pipelineName"`
	Environment  string                             `json:"environment"`
	Definition   *pipelineconfig.PipelineDefinition `json:"definition"`
}

// pipelineRunResult is the polled/terminal state of a submitted run.
type pipelineRunResult struct {
	RunID        string                              `json:"runId"`
	Status       string                              `json:"status"`
	RunErrored   bool                                `json:"runErrored"`
	QualityGates []cicdintegration.QualityGateResult `json:"qualityGates"`
}

// submitAndAwaitPipelineRun posts the pipeline to pipeline-api-gateway,
// then polls GET /v1/pipeline-runs/{id} until it reaches a terminal status
// or waitTimeout elapses. Kept as a small, isolated function so it's the
// one place network/HTTP concerns live in cmd/run.go.
func submitAndAwaitPipelineRun(sub pipelineRunSubmission, waitTimeout time.Duration) (*pipelineRunResult, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	body, err := json.Marshal(sub)
	if err != nil {
		return nil, fmt.Errorf("encoding submission: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, apiGatewayURL+"/v1/pipeline-runs", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+apiToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("submitting pipeline run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("pipeline-api-gateway rejected submission (%s): %s", resp.Status, raw)
	}

	var submitted pipelineRunResult
	if err := json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		return nil, fmt.Errorf("decoding submission response: %w", err)
	}

	return pollUntilTerminal(client, submitted.RunID, waitTimeout)
}

// pollUntilTerminal is a placeholder poll loop. In production this backs
// off and eventually subscribes to pipeline-api-gateway's GraphQL
// subscription channel instead of polling REST, but the CI-friendly
// contract (final exit code) is identical either way.
func pollUntilTerminal(client *http.Client, runID string, waitTimeout time.Duration) (*pipelineRunResult, error) {
	deadline := time.Now().Add(waitTimeout)
	url := fmt.Sprintf("%s/v1/pipeline-runs/%s", apiGatewayURL, runID)

	for {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		if apiToken != "" {
			req.Header.Set("Authorization", "Bearer "+apiToken)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("polling pipeline run %s: %w", runID, err)
		}

		var result pipelineRunResult
		decodeErr := json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("decoding poll response for %s: %w", runID, decodeErr)
		}

		if isTerminalStatus(result.Status) {
			return &result, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for pipeline run %s to finish (last status: %s)", waitTimeout, runID, result.Status)
		}

		time.Sleep(5 * time.Second)
	}
}

func isTerminalStatus(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "ERRORED", "CANCELLED":
		return true
	default:
		return false
	}
}

func printRunSummary(cmd *cobra.Command, result *pipelineRunResult) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "testforge: pipeline run %s finished with status %s\n", result.RunID, result.Status)
	for _, g := range result.QualityGates {
		status := "PASS"
		if !g.Passed {
			status = "FAIL"
		}
		fmt.Fprintf(out, "  [%s] %-10s %-30s observed=%s threshold=%s\n", status, g.Severity, g.Name, g.Observed, g.Threshold)
	}
}
