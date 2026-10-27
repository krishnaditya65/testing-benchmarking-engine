# Scaling notes

This service's provisioning logic is mostly stateless: Terraform and
Crossplane are themselves the source of truth for infrastructure state
(Terraform's state backend, Crossplane's composite resources living on the
Kubernetes API server), so `environment-lifecycle-manager` does not need to
own a durable copy of "what infrastructure exists" beyond the in-process
`EnvironmentRegistryPort` used to track environments this instance is
actively driving through their lifecycle. It is, however, genuinely
stateful with respect to short-lived load-test checkpoint data written to
`coordinator-state-redis` via `RedisTestStateCheckpointStore` — that data
must persist across this service's own restarts so an in-flight load phase
can be resumed after either a spot preemption or a redeploy of this
service.

If this process crashes mid-provisioning, infrastructure is left in a
knowable, recoverable state: re-querying `terraform state list` /
`terraform output` for the workspace, or `kubectl get
compositeresourceclaim` for the claim, tells a restarted instance (or an
operator) exactly what exists and its current outputs, so recovery is a
reconciliation read rather than a guess. Horizontal scaling — running
multiple instances of this service concurrently — is safe for provisioning
different environments in parallel, but concurrent provisioning/teardown
operations against the *same* `environmentId` from two instances is not
currently guarded against; a production deployment needs a distributed lock
(e.g. a Redis- or Postgres-backed advisory lock keyed by `environmentId`)
around `EphemeralEnvironmentProvisioner`'s provision/teardown calls before
running more than one instance — **this is a real TODO, not implemented in
this starter repo**. Spot-instance checkpointing
(`CostAwareSpotInstanceScheduler.checkpointBeforeSpotRequest`) is what
directly protects long-running load-test phases from being lost to
preemption, independent of how many instances of this control-plane service
are running.
