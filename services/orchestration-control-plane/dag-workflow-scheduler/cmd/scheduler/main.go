// Command scheduler is the dag-workflow-scheduler entrypoint. It wires
// together a Temporal client + worker for DagPipelineWorkflow, a Redpanda
// producer for dispatching DAG nodes to the Execution Fleet, a
// WeightedFairShareQueue admission-control layer, and an HTTP server
// exposing POST /pipeline-runs for pipeline-api-gateway to trigger pipeline
// runs.
//
// End-to-end flow (see README.md for the full write-up): POST
// /pipeline-runs enqueues the run into WeightedFairShareQueue keyed by
// tenant, rather than starting a DagPipelineWorkflow execution directly; a
// background QueueDrainDispatcher continuously pulls the next eligible run
// off that queue -- respecting each tenant's weight and MaxConcurrentRuns --
// and starts it via client.ExecuteWorkflow, only marking the run's
// concurrency slot free again once the workflow has actually terminated.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/dispatch"
	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/domain"
	"github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/scheduling"
	internalworkflow "github.com/distributed-testing-benchmarking-tool/dag-workflow-scheduler/internal/workflow"
)

// dagPipelineTaskQueue is the Temporal task queue that this scheduler's
// worker polls, and that DagPipelineWorkflow executions are started on.
const dagPipelineTaskQueue = "dag-pipeline-task-queue"

// defaultTenantID is used when an incoming startPipelineRunRequest doesn't
// specify a tenant (e.g. an older pipeline-api-gateway build that hasn't
// been updated to send one yet). Every such request shares one fairness
// bucket rather than being rejected outright, which keeps this change
// backward compatible with the existing HTTP contract.
const defaultTenantID scheduling.TenantID = "unspecified-tenant"

// schedulerConfig holds the environment-derived settings needed to wire the
// scheduler's dependencies. Reading these once at startup (rather than
// scattering os.Getenv calls) keeps main's construction logic readable.
type schedulerConfig struct {
	temporalHostPort  string
	temporalNamespace string
	redpandaBrokers   []string
	httpListenAddr    string
}

func loadSchedulerConfigFromEnv() schedulerConfig {
	cfg := schedulerConfig{
		temporalHostPort:  getEnvOrDefault("TEMPORAL_HOST_PORT", "dag-workflow-temporal:7233"),
		temporalNamespace: getEnvOrDefault("TEMPORAL_NAMESPACE", "default"),
		httpListenAddr:    getEnvOrDefault("HTTP_LISTEN_ADDR", ":8080"),
	}
	brokers := getEnvOrDefault("REDPANDA_BROKERS", "dag-workflow-redpanda:9092")
	cfg.redpandaBrokers = strings.Split(brokers, ",")
	return cfg
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("dag-workflow-scheduler: fatal: %v", err)
	}
}

func run() error {
	cfg := loadSchedulerConfigFromEnv()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.temporalHostPort,
		Namespace: cfg.temporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("main: dial temporal at %q: %w", cfg.temporalHostPort, err)
	}
	defer temporalClient.Close()

	fleetDispatcher := dispatch.NewRedpandaExecutionFleetDispatcher(cfg.redpandaBrokers)
	defer func() {
		if closeErr := fleetDispatcher.Close(); closeErr != nil {
			log.Printf("dag-workflow-scheduler: warning: closing redpanda dispatcher: %v", closeErr)
		}
	}()

	activities := internalworkflow.NewDagPipelineActivities(fleetDispatcher)

	dagWorker := worker.New(temporalClient, dagPipelineTaskQueue, worker.Options{})
	dagWorker.RegisterWorkflow(internalworkflow.DagPipelineWorkflow)
	dagWorker.RegisterActivityWithOptions(activities.DispatchNodeToExecutionFleetActivity, activity.RegisterOptions{
		Name: internalworkflow.DispatchNodeToExecutionFleetActivityName,
	})

	// runQueue is the WeightedFairShareQueue admission-control layer
	// (internal/scheduling/tenant_priority_run_queue.go): the HTTP handler
	// enqueues into it instead of calling client.ExecuteWorkflow directly,
	// and drainDispatcher below is what actually drains it.
	runQueue := scheduling.NewWeightedFairShareQueue()

	runStarter := newTemporalRunStarterAdapter(temporalClient)
	drainDispatcher := scheduling.NewQueueDrainDispatcher(runQueue, runStarter, scheduling.QueueDrainDispatcherOptions{})

	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go drainDispatcher.Run(dispatchCtx)

	httpServer := newPipelineRunsHTTPServer(cfg.httpListenAddr, runQueue, runStarter)

	workerErrCh := make(chan error, 1)
	go func() {
		if err := dagWorker.Run(worker.InterruptCh()); err != nil {
			workerErrCh <- fmt.Errorf("temporal worker: %w", err)
			return
		}
		workerErrCh <- nil
	}()

	httpErrCh := make(chan error, 1)
	go func() {
		log.Printf("dag-workflow-scheduler: HTTP trigger endpoint listening on %s", cfg.httpListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrCh <- fmt.Errorf("http server: %w", err)
			return
		}
		httpErrCh <- nil
	}()

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-shutdownCh:
		log.Printf("dag-workflow-scheduler: received signal %s, shutting down", sig)
	case err := <-workerErrCh:
		if err != nil {
			return err
		}
	case err := <-httpErrCh:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("dag-workflow-scheduler: warning: HTTP server shutdown: %v", err)
	}

	return nil
}

// newPipelineRunsHTTPServer builds the HTTP server exposing POST
// /pipeline-runs, the entrypoint pipeline-api-gateway calls to start a new
// pipeline run.
func newPipelineRunsHTTPServer(listenAddr string, runQueue scheduling.TenantRunQueue, runStarter *temporalRunStarterAdapter) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/pipeline-runs", newPipelineRunsHandler(runQueue, runStarter))

	return &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// startPipelineRunRequest is the JSON body pipeline-api-gateway sends to
// POST /pipeline-runs. TenantID drives WeightedFairShareQueue admission
// control; it's optional (falls back to defaultTenantID) so this stays
// backward compatible with callers that don't send it yet.
type startPipelineRunRequest struct {
	Definition domain.PipelineDefinition `json:"definition"`
	TenantID   string                    `json:"tenantId"`
}

// startPipelineRunResponse acknowledges that a pipeline run was admitted
// into WeightedFairShareQueue. Because dispatch is now asynchronous (a run
// may sit queued behind higher-priority runs from other tenants before
// QueueDrainDispatcher actually starts it), WorkflowID is the deterministic
// Temporal workflow ID this run *will* execute under once started -- computed
// up front so callers can poll/link to it immediately -- rather than a value
// only known after client.ExecuteWorkflow returns. RunID is this scheduler's
// own queued-run identifier.
type startPipelineRunResponse struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

// dagPipelineWorkflowID derives the deterministic Temporal workflow ID for a
// given pipeline + queued-run pair. Deterministic (rather than randomly
// generated at ExecuteWorkflow time) so the HTTP handler can hand it back to
// the caller immediately at enqueue time, before QueueDrainDispatcher has
// necessarily started the run.
func dagPipelineWorkflowID(pipelineID, runID string) string {
	return fmt.Sprintf("dag-pipeline-run-%s-%s", pipelineID, runID)
}

// newPipelineRunsHandler returns an http.Handler implementing POST
// /pipeline-runs: it decodes a PipelineDefinition, admits it into runQueue
// keyed by tenant, and responds with the run's IDs. The run's
// DagPipelineWorkflow execution is started later, asynchronously, by
// QueueDrainDispatcher once WeightedFairShareQueue's fairness algorithm
// selects it -- not synchronously by this handler.
func newPipelineRunsHandler(runQueue scheduling.TenantRunQueue, runStarter *temporalRunStarterAdapter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var reqBody startPipelineRunRequest
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
		if reqBody.Definition.PipelineID == "" {
			http.Error(w, "definition.pipelineId is required", http.StatusBadRequest)
			return
		}

		tenantID := scheduling.TenantID(reqBody.TenantID)
		if tenantID == "" {
			tenantID = defaultTenantID
		}

		runID := uuid.NewString()
		runStarter.registerDefinition(runID, reqBody.Definition)

		err := runQueue.Enqueue(scheduling.QueuedPipelineRun{
			RunID:      runID,
			TenantID:   tenantID,
			PipelineID: reqBody.Definition.PipelineID,
			EnqueuedAt: time.Now(),
		})
		if err != nil {
			runStarter.takeDefinition(runID) // don't leak the registered definition
			http.Error(w, fmt.Sprintf("failed to enqueue pipeline run: %v", err), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(startPipelineRunResponse{
			WorkflowID: dagPipelineWorkflowID(reqBody.Definition.PipelineID, runID),
			RunID:      runID,
		})
	})
}

// temporalRunStarterAdapter implements scheduling.RunStarterPort over a
// Temporal client, and is the sole place PipelineDefinition payloads are
// held between "admitted into runQueue" and "actually started" -- since
// scheduling.QueuedPipelineRun deliberately carries only routing metadata
// (RunID/TenantID/PipelineID), not the full DAG, keeping the scheduling
// package free of any dependency on the exact shape of a pipeline
// definition.
type temporalRunStarterAdapter struct {
	temporalClient client.Client

	mu                 sync.Mutex
	pendingDefinitions map[string]domain.PipelineDefinition
}

func newTemporalRunStarterAdapter(temporalClient client.Client) *temporalRunStarterAdapter {
	return &temporalRunStarterAdapter{
		temporalClient:     temporalClient,
		pendingDefinitions: make(map[string]domain.PipelineDefinition),
	}
}

// registerDefinition stashes definition under runID, to be retrieved by
// StartRun once QueueDrainDispatcher dispatches this run. Called by the HTTP
// handler at Enqueue time, before the run is necessarily ever dispatched.
func (a *temporalRunStarterAdapter) registerDefinition(runID string, definition domain.PipelineDefinition) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pendingDefinitions[runID] = definition
}

// takeDefinition removes and returns runID's registered definition, if any.
func (a *temporalRunStarterAdapter) takeDefinition(runID string) (domain.PipelineDefinition, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	definition, ok := a.pendingDefinitions[runID]
	if ok {
		delete(a.pendingDefinitions, runID)
	}
	return definition, ok
}

// StartRun implements scheduling.RunStarterPort by starting run's
// DagPipelineWorkflow execution via client.ExecuteWorkflow, using the same
// deterministic workflow ID dagPipelineWorkflowID handed back to the caller
// at enqueue time.
func (a *temporalRunStarterAdapter) StartRun(ctx context.Context, run scheduling.QueuedPipelineRun) (scheduling.RunHandle, error) {
	definition, ok := a.takeDefinition(run.RunID)
	if !ok {
		return scheduling.RunHandle{}, fmt.Errorf("main: no registered PipelineDefinition for run %q (tenant %q)", run.RunID, run.TenantID)
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        dagPipelineWorkflowID(run.PipelineID, run.RunID),
		TaskQueue: dagPipelineTaskQueue,
	}

	workflowRun, err := a.temporalClient.ExecuteWorkflow(ctx, workflowOptions, internalworkflow.DagPipelineWorkflow, definition)
	if err != nil {
		return scheduling.RunHandle{}, fmt.Errorf("main: start DagPipelineWorkflow for run %q: %w", run.RunID, err)
	}

	return scheduling.RunHandle{WorkflowID: workflowRun.GetID(), RunID: workflowRun.GetRunID()}, nil
}

// AwaitCompletion implements scheduling.RunStarterPort by blocking on
// Temporal's own workflow-completion signal for handle, which is what lets
// QueueDrainDispatcher hold the tenant's concurrency slot open for the
// run's entire lifetime rather than releasing it as soon as the workflow
// merely *starts*.
func (a *temporalRunStarterAdapter) AwaitCompletion(ctx context.Context, handle scheduling.RunHandle) error {
	var result domain.PipelineRunResult
	return a.temporalClient.GetWorkflow(ctx, handle.WorkflowID, handle.RunID).Get(ctx, &result)
}
