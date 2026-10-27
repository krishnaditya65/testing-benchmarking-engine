# helm-charts/protocol-load-worker-go (stub)

Chart for `services/execution-fleet/protocol-load-worker-go`. Not yet fleshed out —
copy the structure of `infrastructure/helm-charts/pipeline-api-gateway/` (see its
`README.md` for the pattern) and adjust:

- `values.yaml`: no `service.yaml`/ClusterIP needed (workers accept no inbound
  traffic); autoscaling metrics should mirror
  `infrastructure/kubernetes/base/execution-fleet-worker-deployment.yaml`'s HPA —
  CPU **and** the custom `active-virtual-users` metric, not CPU alone.
- `templates/deployment.yaml`: include the `lifecycle.preStop` checkpoint-and-drain
  hook documented in the Kustomize base manifest.
- `nodeSelector`/`tolerations`: target `testforge.io/execution-fleet: "true"`
  tainted nodes, not the control-plane node group.
