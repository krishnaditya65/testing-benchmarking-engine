# infrastructure/kubernetes

Kustomize-style manifests for everything in `infrastructure/kubernetes/README.md`'s
scope: the **long-lived platform workloads** (orchestration control plane, execution
fleet, telemetry lake DaemonSets). Ephemeral per-pipeline-run namespaces are a
different, on-demand path — see `infrastructure/crossplane/README.md`.

## Base + overlay strategy

```
kubernetes/
├── base/                    Environment-agnostic manifests. Every object here
│                             must be valid and (mostly) deployable on its own —
│                             overlays only ever *patch*, they never invent new
│                             base behavior.
│   ├── namespace.yaml
│   ├── execution-fleet-worker-deployment.yaml   Deployment + HPA for
│   │                                             protocol-load-worker-go
│   ├── chaos-ebpf-sidecar-daemonset.yaml        Privileged eBPF fault-injection
│   │                                             agent, one per execution-fleet node
│   ├── control-plane-services-deployment.yaml   pipeline-api-gateway,
│   │                                             dag-workflow-scheduler,
│   │                                             environment-lifecycle-manager
│   ├── telemetry-clickhouse-statefulset.yaml    telemetry-metrics-clickhouse:
│   │                                             ConfigMap-mounted storage-policy.xml
│   │                                             (hot/cold tiering, see migration 005),
│   │                                             singleton StatefulSet + headless Service
│   └── kustomization.yaml
└── overlays/
    ├── staging/              Small fixed replica counts, low HPA ceiling
    │                         (25 workers max) so a bad load test in a
    │                         feature branch can't exhaust the cluster.
    │                         ClickHouse stays on base's single hot-tier PVC
    │                         (no cold-tier volume patch).
    └── production/           Higher replica floors, full 500-worker HPA
                               ceiling from base, larger control-plane
                               resource requests/limits, and a
                               clickhouse-cold-tier-volume-patch.yaml adding
                               a second, on-cluster cold-tier PVC to
                               telemetry-metrics-clickhouse.
```

Why base+overlay instead of one templated manifest set: the objects that differ
between staging and production are *only* replica counts, image tags, and a couple
of resource ceilings — everything else (probes, env var wiring, topic names,
security contexts) must stay byte-for-byte identical across environments so a
staging deploy is actually representative of what will run in production. Kustomize
patches make every intentional difference a one-line diff you can review; a
Helm-values-driven single template would let those differences silently multiply
implicit `{{ if .Values.env }}` branches instead.

## Usage

```bash
# Render and diff before applying
kubectl kustomize infrastructure/kubernetes/overlays/staging | kubectl diff -f -

# Apply
kubectl apply -k infrastructure/kubernetes/overlays/production
```

## Relationship to infrastructure/helm-charts/

The Helm charts in `infrastructure/helm-charts/` are the GitOps-managed deployment
mechanism (Argo CD `Application` per chart, one chart per service). These raw
Kustomize manifests are the reference/bootstrap path — useful for `kubectl kustomize
| kubectl diff` sanity checks, local `kind`/`minikube` bring-up, and as the source
manifests that `helm template` output is diffed against in CI to catch chart drift.
