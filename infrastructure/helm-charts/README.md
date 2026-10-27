# infrastructure/helm-charts

One chart per service, used as the GitOps deployment unit (each chart maps to one
Argo CD `Application`). `pipeline-api-gateway/` is the fully fleshed-out reference
chart — every other chart directory below is a thin stub whose README just points
back at it as the template to copy.

```
helm-charts/
├── pipeline-api-gateway/        Reference chart — copy this structure for any
│   ├── Chart.yaml                new service chart.
│   ├── values.yaml
│   ├── README.md
│   └── templates/
│       ├── _helpers.tpl
│       ├── deployment.yaml       Gateway container + OPA policy-eval sidecar
│       │                         (see pipeline-api-gateway/README.md)
│       ├── service.yaml
│       ├── hpa.yaml
│       └── opa-policy-bundle-configmap.yaml   Rego bundle for the OPA sidecar
├── protocol-load-worker-go/     Stub — see its README.md
├── dag-workflow-scheduler/      Stub — see its README.md
└── telemetry-query-api/         Stub — see its README.md
```

## Chart pattern

Every service chart in this repo follows the same five-file shape as
`pipeline-api-gateway`:

1. `Chart.yaml` — name matches the service directory name exactly (see
   ARCHITECTURE.md naming conventions), `type: application`.
2. `values.yaml` — image, replica/autoscaling config, resources, probes, env,
   scheduling (`nodeSelector`/`tolerations`/`affinity`).
3. `templates/_helpers.tpl` — `<chart>.name`, `<chart>.fullname`, `<chart>.labels`,
   `<chart>.selectorLabels`, `<chart>.serviceAccountName` — copy verbatim, only the
   `define` prefix changes.
4. `templates/deployment.yaml` + `templates/service.yaml` + optional
   `templates/hpa.yaml` (omit for services that don't autoscale, e.g. singleton
   stateful components).
5. A service-specific env var block wiring in whatever that service actually needs
   (Postgres DSN, Redpanda brokers, Temporal address, OTLP endpoint — see each
   service's own `README.md` for its actual dependency list).

To add a new chart: copy `pipeline-api-gateway/`, rename the directory, `s/pipeline-api-gateway/<new-service-name>/g` across all files, then adjust `values.yaml`
env/ports/resources to match the service.

## Why per-service charts instead of one umbrella chart

Each service has its own release cadence, its own rollback boundary, and (per
ARCHITECTURE.md "Independent failure domains") must never have its deploy coupled
to another service's. An umbrella chart would make `helm rollback` an all-or-
nothing operation across unrelated services; per-service charts keep it scoped.
