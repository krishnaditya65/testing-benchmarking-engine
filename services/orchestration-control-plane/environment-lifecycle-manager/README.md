# environment-lifecycle-manager

Orchestration Control Plane service that provisions and tears down ephemeral
test environments that DAG pipeline runs execute against, and coordinates
with the Execution Fleet layer's spot-instance-based workers to checkpoint
load-test state before spot preemption.

## Feature manifest mapping

**"Environment lifecycle management via Terraform/Crossplane"**

- `src/provisioning/terraform-execution.port.ts` — `TerraformExecutionPort`, the
  infrastructure-agnostic contract for provisioning via Terraform.
- `src/provisioning/terraform-cli-execution.adapter.ts` — `TerraformCliExecutionAdapter`
  (builds real `terraform apply`/`terraform output -json`/`terraform destroy`
  argv and parses CLI output) plus `NodeChildProcessCommandRunner`, the real
  `child_process.execFile`-based process spawner.
- `src/provisioning/crossplane-composition.port.ts` — `CrossplaneCompositionPort`,
  the equivalent contract for provisioning via Crossplane compositions.
- `src/provisioning/crossplane-kubectl-execution.adapter.ts` —
  `CrossplaneKubectlExecutionAdapter` (builds an in-memory Crossplane claim
  manifest and drives `kubectl apply`/`kubectl get`/`kubectl delete`).
- `src/provisioning/command-runner.port.ts` — `CommandRunnerPort`, the shared
  process-execution abstraction both CLI adapters depend on, keeping them
  unit-testable without spawning real processes.
- `src/provisioning/ephemeral-environment-provisioner.ts` —
  `EphemeralEnvironmentProvisioner`, the use-case orchestrator that picks a
  backend (`terraform` | `crossplane`), drives provisioning/teardown, and
  keeps each environment's `EnvironmentLifecycleStateMachine` in the
  `src/provisioning/environment-registry.port.ts` registry.
- `src/domain/environment-lifecycle-state-machine.ts` —
  `EnvironmentLifecycleStateMachine`, the state machine enforcing the
  environment lifecycle:

  ```
  PROVISIONING -> READY -> POLICY_VERIFYING -> TEST_RUNNING -> READY (idle again)
                                                            \-> TEARING_DOWN -> DESTROYED
                                             \-> FAILED_POLICY_CHECK (terminal)
                                             \-> DESTROYED (gate call itself errored)
  READY -> TEARING_DOWN -> DESTROYED
  PROVISIONING | READY | POLICY_VERIFYING | TEST_RUNNING -> DESTROYED (failure path)
  ```

  Illegal transitions throw `InvalidEnvironmentLifecycleTransitionError`, and
  every transition is recorded with a timestamp via `getTransitionHistory()`.
  Every environment must pass through `POLICY_VERIFYING` before
  `TEST_RUNNING` — there is no direct `READY -> TEST_RUNNING` transition —
  so the `services/distributed-systems-validation` policy gate
  (`src/provisioning/policy-verification-gate.port.ts`) always runs first.
  See `../../distributed-systems-validation/service-mesh-policy-verifier/README.md`
  and `../../distributed-systems-validation/consistency-verification-worker/README.md`
  for what that gate checks.

**"Service Mesh Route Verification" / "Data Consistency Benchmarking" pre-test gate**

`src/provisioning/policy-verification-gate.port.ts` — `PolicyVerificationGatePort`, the seam
`EphemeralEnvironmentProvisioner.beginTestExecution()` calls out through while an environment is
in `POLICY_VERIFYING`, aggregating checks from `service-mesh-policy-verifier` (Istio/Linkerd
mesh resilience, Kubernetes/IAM drift) and `consistency-verification-worker` (ACID/eventual
consistency under chaos-induced partitions) into a single `PolicyVerificationVerdict`. A failing
verdict routes the environment to the terminal `FAILED_POLICY_CHECK` state and throws
`PolicyVerificationFailedError`; a transport-level failure calling the gate (as opposed to a real
failing verdict) routes to `DESTROYED` instead, via the same infra-error failure path Terraform/
Crossplane errors already use. `NotWiredPolicyVerificationGateAdapter` is the default adapter
until a real HTTP client to those two services is wired — see the port's doc comment.

**"Durability by design" — spot instance checkpointing**

ARCHITECTURE.md states:

> Load tests survive spot instance preemption because worker state is
> checkpointed to Redis before each load-phase transition (see
> environment-lifecycle-manager and protocol-load-worker-go).

This is implemented concretely by
`src/spot-scheduling/cost-aware-spot-instance-scheduler.ts`'s
`CostAwareSpotInstanceScheduler.checkpointBeforeSpotRequest()`, which
serializes the current `TestPhaseState` to JSON and writes it — with a TTL —
to Redis (matching docker-compose's `coordinator-state-redis` service) via
`RedisTestStateCheckpointStore` (`src/spot-scheduling/test-state-checkpoint-store.port.ts`)
**before** `requestSpotCapacity()` is called for the next load phase. If a
spot instance is preempted mid-phase, `resumeFromCheckpointAfterPreemption()`
reads the last checkpoint back so the load test can resume without losing
progress.

`requestSpotCapacity()` itself performs real cost-aware candidate selection:
it filters spot instance candidates to those within `maxPricePerHour` and
below `maxAcceptableInterruptionRiskScore`, then picks the cheapest
remaining candidate (ties broken by lowest interruption risk).

## Layout

```
src/
  domain/
    environment-lifecycle-state-machine.ts
  provisioning/
    command-runner.port.ts
    terraform-execution.port.ts
    terraform-cli-execution.adapter.ts
    crossplane-composition.port.ts
    crossplane-kubectl-execution.adapter.ts
    environment-registry.port.ts
    policy-verification-gate.port.ts
    ephemeral-environment-provisioner.ts
  spot-scheduling/
    test-state-checkpoint-store.port.ts
    cost-aware-spot-instance-scheduler.ts
  index.ts   # composition root: createEnvironmentLifecycleManager(config)
```

## Development

```
npm install
npm run build   # tsc
npm run dev      # tsx watch src/index.ts
npm run lint
npm test         # jest
```

`src/index.ts` exports `createEnvironmentLifecycleManager(config)` rather
than running anything at import time — call it from a real entry point
(HTTP server bootstrap, worker process, etc.) with a Redis connection URL to
get back a wired `EphemeralEnvironmentProvisioner` and
`CostAwareSpotInstanceScheduler`.
