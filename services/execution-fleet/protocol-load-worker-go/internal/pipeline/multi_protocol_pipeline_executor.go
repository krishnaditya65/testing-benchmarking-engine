// Package pipeline chains protocolclient.ProtocolClient calls across
// different protocols into one ordered sequence, threading extracted
// variables from earlier steps into later ones — e.g. fetch a JWT over
// REST, extract it, then open an authenticated gRPC stream with it.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/extraction"
	"github.com/distributed-testing-benchmarking-tool/protocol-load-worker-go/internal/protocolclient"
)

// RequestBuilder builds a protocolclient.Request for one PipelineStep,
// given the VariableContext accumulated by every step that ran before it.
// Implementations typically call ctx.Interpolate on URL/header/body
// templates so values extracted by earlier steps land in the right place.
type RequestBuilder func(varCtx *extraction.VariableContext) protocolclient.Request

// ExtractionRule describes one value to pull out of a step's response body
// and store into the shared VariableContext for later steps to consume.
type ExtractionRule struct {
	// JSONPath is the expression evaluated against the response body.
	JSONPath string
	// VariableName is the key later steps read the value back under.
	VariableName string
	// Optional, when true, means a failed extraction is logged but does
	// not fail the pipeline step (useful for best-effort telemetry
	// fields). When false (the default), an extraction failure fails the
	// step and the whole pipeline run.
	Optional bool
}

// PipelineStep is one call in a MultiProtocolPipelineExecutor run: which
// client executes it, how to build the request from accumulated
// variables, and what to extract from the response afterward.
type PipelineStep struct {
	// Name identifies the step for logging/telemetry/error messages
	// (e.g. "fetch-jwt", "authenticated-stream").
	Name string

	// Client executes this step's request. Different steps commonly use
	// different ProtocolClient implementations (REST for the auth call,
	// gRPC for the payload call).
	Client protocolclient.ProtocolClient

	// BuildRequest constructs the request from the variables extracted
	// by prior steps.
	BuildRequest RequestBuilder

	// Extractions runs, in order, against the step's response body after
	// a successful call.
	Extractions []ExtractionRule
}

// StepResult records the outcome of one executed PipelineStep for
// diagnostics and telemetry export.
type StepResult struct {
	StepName string
	Response protocolclient.Response
	Err      error
	Started  time.Time
	Elapsed  time.Duration
}

// PipelineResult is the full record of a MultiProtocolPipelineExecutor
// run: every step's outcome plus the final variable state.
type PipelineResult struct {
	Steps     []StepResult
	Variables *extraction.VariableContext
	Err       error
}

// MultiProtocolPipelineExecutor runs an ordered list of PipelineSteps
// against potentially different protocols, threading a shared
// VariableContext between them so a value extracted from one step's
// response (e.g. an auth token) can be injected into a later step's
// request.
type MultiProtocolPipelineExecutor struct {
	Steps     []PipelineStep
	Extractor *extraction.JsonPathVariableExtractor
}

// NewMultiProtocolPipelineExecutor builds an executor for steps, wiring a
// default JsonPathVariableExtractor.
func NewMultiProtocolPipelineExecutor(steps []PipelineStep) *MultiProtocolPipelineExecutor {
	return &MultiProtocolPipelineExecutor{
		Steps:     steps,
		Extractor: extraction.NewJsonPathVariableExtractor(),
	}
}

// Execute runs every step in order against a fresh VariableContext,
// stopping at the first step whose call fails or whose non-optional
// extraction fails. It always returns a PipelineResult describing every
// step that ran, even on failure, so callers can see exactly how far the
// pipeline got.
func (e *MultiProtocolPipelineExecutor) Execute(ctx context.Context) PipelineResult {
	varCtx := extraction.NewVariableContext()
	result := PipelineResult{Variables: varCtx}

	for _, step := range e.Steps {
		if err := ctx.Err(); err != nil {
			result.Err = fmt.Errorf("pipeline: aborted before step %q: %w", step.Name, err)
			return result
		}

		stepResult, err := e.runStep(ctx, step, varCtx)
		result.Steps = append(result.Steps, stepResult)
		if err != nil {
			result.Err = fmt.Errorf("pipeline: step %q failed: %w", step.Name, err)
			return result
		}
	}
	return result
}

// runStep executes one step's request, then applies its extraction rules
// against the response body into varCtx.
func (e *MultiProtocolPipelineExecutor) runStep(ctx context.Context, step PipelineStep, varCtx *extraction.VariableContext) (StepResult, error) {
	if step.Client == nil {
		return StepResult{StepName: step.Name}, fmt.Errorf("step has no Client configured")
	}
	if step.BuildRequest == nil {
		return StepResult{StepName: step.Name}, fmt.Errorf("step has no BuildRequest configured")
	}

	req := step.BuildRequest(varCtx)

	started := time.Now()
	resp, err := step.Client.Execute(ctx, req)
	elapsed := time.Since(started)

	stepResult := StepResult{
		StepName: step.Name,
		Response: resp,
		Started:  started,
		Elapsed:  elapsed,
	}

	if err != nil {
		stepResult.Err = err
		return stepResult, err
	}

	for _, rule := range step.Extractions {
		if _, extractErr := e.Extractor.ExtractInto(resp.Body, rule.JSONPath, rule.VariableName, varCtx); extractErr != nil {
			if rule.Optional {
				continue
			}
			wrapped := fmt.Errorf("extracting variable %q via %q: %w", rule.VariableName, rule.JSONPath, extractErr)
			stepResult.Err = wrapped
			return stepResult, wrapped
		}
	}

	return stepResult, nil
}
