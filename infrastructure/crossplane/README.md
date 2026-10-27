# infrastructure/crossplane

The **on-demand** path, as opposed to `infrastructure/terraform`'s **account
bootstrap** path. Read both READMEs together — they're two halves of the same
provisioning story, split by lifecycle frequency and who triggers them:

| | `infrastructure/terraform/` | `infrastructure/crossplane/` |
|---|---|---|
| Creates | VPC, EKS cluster, node groups, IAM, state bucket | Kubernetes namespace, ResourceQuota, NetworkPolicies inside an already-running cluster |
| Triggered by | A human (or infra-CI) running `terraform apply` | `environment-lifecycle-manager` calling the Kubernetes API at pipeline-run time |
| Frequency | Once per environment, rarely changed after | Once per pipeline run — potentially hundreds of times a day |
| Lifetime | Durable, long-lived | Ephemeral — torn down when the run finishes or its TTL expires |
| Failure blast radius | Account/cluster-wide if misapplied | Scoped to one pipeline run's namespace |

## Why Crossplane and not just calling the Kubernetes API directly

`environment-lifecycle-manager` could `kubectl create namespace` directly, but the
Composition in `compositions/ephemeral-test-namespace-composition.yaml` buys three
things a hand-rolled call wouldn't:

1. **A single declarative unit.** Namespace + ResourceQuota + default-deny
   NetworkPolicy + execution-fleet-allow NetworkPolicy + TTL bookkeeping are created
   and, critically, garbage-collected together as one Claim — no partial-teardown
   bugs where a NetworkPolicy survives an already-deleted namespace.
2. **A typed contract.** The XRD's `openAPIV3Schema` (`pipelineRunId`,
   `maxConcurrentVirtualUsers`, `ttlMinutes`) is the actual interface
   `environment-lifecycle-manager` codes against — it's validated by the API
   server, not by convention.
3. **Composable tiers.** `ephemeral-test-namespace-standard` is one Composition;
   nothing stops adding `ephemeral-test-namespace-restricted` (tighter quotas, no
   egress) for lower-trust pipeline sources later, without changing the XRD or the
   caller.

## Flow

1. Pipeline run created → `dag-workflow-scheduler` reaches an
   `environment-provision` DAG node.
2. `environment-lifecycle-manager` creates an `EphemeralTestNamespace` Claim with
   `pipelineRunId` set to the run's ID.
3. Crossplane reconciles the Composition into a real `Namespace`, `ResourceQuota`,
   and two `NetworkPolicy` objects.
4. `environment-lifecycle-manager` polls the Claim's `status.namespace`, returns it
   to `dag-workflow-scheduler`, which passes it to `protocol-load-worker-go` pods as
   their target namespace context.
5. On run completion (success, failure, or TTL expiry) `environment-lifecycle-manager`
   deletes the Claim; Crossplane cascades the delete to every composed resource.
