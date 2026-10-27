# infrastructure/terraform

The **account bootstrap** path — the infrastructure that must exist before any
Kubernetes manifest, Helm chart, or Crossplane composition in this repo can be
applied. Run once per cloud account/environment, not per pipeline run.

```
terraform/
├── modules/
│   └── cloud-account-bootstrap/    VPC, EKS cluster, execution-fleet +
│                                    control-plane node groups, IRSA roles,
│                                    Terraform state bucket + lock table
└── environments/
    ├── staging/main.tf              Small, spot-only, single AZ pair
    └── production/main.tf           Full HPA-ceiling sizing, multi-AZ,
                                      larger control-plane node group
```

## What this does NOT do

This module does **not** create per-pipeline-run test namespaces. That is a
fundamentally different lifecycle — created and destroyed potentially hundreds of
times a day, driven by `environment-lifecycle-manager`, not a human running
`terraform apply`. That path is `infrastructure/crossplane/`. See
`infrastructure/crossplane/README.md` for the full split rationale; the short
version: **Terraform owns durable cloud-account-level infrastructure, Crossplane
owns ephemeral in-cluster infrastructure.**

## Bootstrapping order

1. `cd environments/staging && terraform init && terraform apply` with a local
   backend first (the S3 state bucket doesn't exist yet).
2. Once `terraform_state_bucket` output exists, uncomment/point the `backend "s3"`
   block at it and run `terraform init -migrate-state`.
3. Repeat for `environments/production`.
4. Feed `eks_cluster_name` / `eks_cluster_endpoint` outputs into your
   `kubectl`/`helm` CI context to deploy `infrastructure/kubernetes` and
   `infrastructure/helm-charts` against the new cluster.

## Node group split

Two node groups per environment, not one:

- **execution-fleet** — spot capacity, tainted `testforge.io/execution-fleet`,
  network-optimized instance types (`c6in.*`). Safe to interrupt because worker
  state is checkpointed to Redis before each load-phase transition.
- **control-plane** — on-demand only, general-purpose instance types (`m6i.*`).
  Runs Temporal, ClickHouse, Redpanda — never safe to preempt mid-write.
