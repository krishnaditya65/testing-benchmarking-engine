# helm-charts/dag-workflow-scheduler (stub)

Chart for `services/orchestration-control-plane/dag-workflow-scheduler`. Not yet
fleshed out — copy the structure of
`infrastructure/helm-charts/pipeline-api-gateway/` (see its `README.md` for the
pattern) and adjust:

- `values.yaml`: env should wire `TESTFORGE_TEMPORAL_ADDRESS` and
  `TESTFORGE_DISPATCH_TOPIC` (`dag.node.dispatch.v1`), matching
  `infrastructure/kubernetes/base/control-plane-services-deployment.yaml`.
- No `templates/hpa.yaml` needed at small-to-medium scale — this process is
  low-QPS orchestration, not throughput-bound (see ARCHITECTURE.md "Independent
  scaling"); revisit only if Temporal workflow-poll latency becomes a bottleneck.
- `nodeSelector`: target `testforge.io/control-plane: "true"`.
