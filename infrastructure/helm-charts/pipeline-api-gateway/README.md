# pipeline-api-gateway Helm chart

Reference chart for `services/orchestration-control-plane/pipeline-api-gateway` — see
`infrastructure/helm-charts/README.md` for the pattern every other chart in this repo copies.

```
pipeline-api-gateway/
├── Chart.yaml
├── values.yaml
├── README.md
└── templates/
    ├── _helpers.tpl
    ├── deployment.yaml
    ├── service.yaml
    ├── hpa.yaml
    └── opa-policy-bundle-configmap.yaml
```

## What this chart deploys

- A `Deployment` running the `pipeline-api-gateway` NestJS BFF/GraphQL container plus, in the
  same Pod, an **OPA (Open Policy Agent) sidecar container** (see "OPA sidecar" below).
- A `Service` exposing the GraphQL/WebSocket ports, plus the chart's `ServiceAccount`.
- A `HorizontalPodAutoscaler` (CPU-only, see the comment in `values.yaml` for why this differs
  from `protocol-load-worker-go`'s custom-metric scaling).
- A `ConfigMap` (`opa-policy-bundle-configmap.yaml`) carrying the OPA sidecar's Rego policy
  bundle.

## OPA sidecar

`pipeline-api-gateway`'s `PolicyGateService` calls out to OPA's Data API via
`opa-rego-policy-evaluator.adapter.ts` before every run trigger (see ADR 0005 and ADR 0013).
That adapter defaults to `OPA_SIDECAR_BASE_URL=http://localhost:8181`, so OPA must run as a
sidecar in the same Pod, not a separate Service — this chart wires that up:

- `templates/deployment.yaml` adds an `opa-sidecar` container (image
  `openpolicyagent/opa`, configurable via `values.yaml`'s `opaSidecar.image`) started with
  `opa run --server --addr=:8181 -b /policies`, i.e. OPA's standard bundle-directory flag
  (`-b`), not `--config-file` — there's no need for OPA's discovery/config-file mode here since
  the whole bundle is a handful of static files baked into a ConfigMap.
- `templates/opa-policy-bundle-configmap.yaml` embeds the actual content of
  `services/orchestration-control-plane/pipeline-api-gateway/src/policy-gate/example-policies/*.rego`
  (both files, verbatim) as ConfigMap data keys, mounted into the sidecar at `/policies` —
  matching the `-b /policies` bundle directory OPA is started with.
- The gateway container gets `OPA_SIDECAR_BASE_URL=http://localhost:8181` as an env var
  (`values.yaml`'s `env.opaSidecarBaseUrl`) so the adapter's default and this chart's actual
  wiring can never silently drift apart.
- Toggle the whole sidecar (container + ConfigMap + volume + gateway env var) off with
  `opaSidecar.enabled: false` — e.g. for a local `helm template` dry run against a
  developer's own standalone `opa run` process. Default is `true`; running this chart with
  policy evaluation unwired means every run trigger fails closed
  (`ServiceUnavailableException`, per ADR 0005's Consequences), so `enabled: false` should only
  ever be a deliberate, temporary override.

**Keeping the bundle in sync:** there is no build-time codegen step that regenerates
`opa-policy-bundle-configmap.yaml` from `example-policies/*.rego` — a change to either `.rego`
file must be copied into the ConfigMap by hand (or via a follow-up CI check) until that's
automated. See the comment at the top of `opa-policy-bundle-configmap.yaml`.

## Values

In addition to the standard image/replica/autoscaling/resources/probes/scheduling blocks (see
`infrastructure/helm-charts/README.md`'s "Chart pattern"), this chart's `values.yaml` adds:

- `env.opaSidecarBaseUrl` — the gateway container's `OPA_SIDECAR_BASE_URL`, always
  `http://localhost:8181` in-cluster since OPA is a same-Pod sidecar, not a Service.
- `opaSidecar.enabled` — default `true`.
- `opaSidecar.image.repository` / `.tag` / `.pullPolicy` — the OPA image.
- `opaSidecar.port` — the port OPA listens on and the gateway calls; must match the port baked
  into `opa-rego-policy-evaluator.adapter.ts`'s default `OPA_SIDECAR_BASE_URL` (`8181`) if you
  ever change it here.
- `opaSidecar.resources` — the sidecar is a lightweight in-memory policy evaluator, so its
  request/limit defaults are much smaller than the gateway container's.
