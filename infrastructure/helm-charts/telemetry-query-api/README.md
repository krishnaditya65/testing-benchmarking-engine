# helm-charts/telemetry-query-api (stub)

Chart for `services/telemetry-lake/telemetry-query-api`. Not yet fleshed out —
copy the structure of `infrastructure/helm-charts/pipeline-api-gateway/` (see its
`README.md` for the pattern) and adjust:

- `values.yaml`: env should wire the ClickHouse DSN and Redis cache connection
  this service reads P50/P99/error-rate aggregates and `active-virtual-users`
  (consumed by `protocol-load-worker-go`'s HPA) from.
- `templates/hpa.yaml`: CPU-based scaling is sufficient — this is a read path,
  query volume tracks dashboard/API consumers, not running pipeline load.
- `nodeSelector`: target `testforge.io/control-plane: "true"`.
