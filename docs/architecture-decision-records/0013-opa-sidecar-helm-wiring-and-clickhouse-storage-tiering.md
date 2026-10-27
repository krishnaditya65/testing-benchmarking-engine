# 0013. OPA sidecar Helm wiring and end-to-end ClickHouse storage tiering (local dev included)

- Status: Accepted
- Date: 2026-08-11
- Owners: pipeline-api-gateway maintainers, telemetry-lake maintainers, platform engineering

## Context

ARCHITECTURE.md's "Known scope boundaries" note and ADR 0005's Consequences section both
documented, deliberately, two gaps rather than silently dropping them:

**OPA sidecar not wired into the Helm charts.** `opa-rego-policy-evaluator.adapter.ts` calls out
to an OPA sidecar's Data API (`POST /v1/data/testforge/policies/violations`, default
`OPA_SIDECAR_BASE_URL=http://localhost:8181`) as the *first* step of every run trigger
(`PolicyGateService.evaluateBeforeRun`), and fails closed
(`ServiceUnavailableException`) if that sidecar is unreachable. Neither
`infrastructure/kubernetes/base/control-plane-services-deployment.yaml` nor
`infrastructure/helm-charts/pipeline-api-gateway` actually ran OPA — meaning the GitOps-managed
Helm deploy path could not trigger a single run without an operator manually patching in a
sidecar first.

**ClickHouse's cold-tier storage policy was production/staging-only.** Migration 005
(`005_hot_tier_ttl_and_cold_tier_storage_policy.sql`) correctly documented that ClickHouse
storage policies are declared in server config XML, not SQL, and that policy config didn't
exist anywhere in the repo as a real, loadable artifact — only as a comment block describing
the shape it should take. `docker-compose.yml`'s `telemetry-metrics-clickhouse` stayed
single-disk, so local dev could never exercise the tiering mechanism migration 005 introduces,
and `infrastructure/kubernetes/base/` had no ClickHouse manifest at all to wire that XML into
for staging/production either.

## Decision

**OPA sidecar, wired into the Helm chart only** (not the raw Kustomize manifests, which are the
reference/bootstrap path per `infrastructure/kubernetes/README.md`, not the GitOps deploy
mechanism):

- `infrastructure/helm-charts/pipeline-api-gateway/templates/deployment.yaml` adds an
  `opa-sidecar` container (`openpolicyagent/opa`, configurable image/tag) in the same Pod as
  `pipeline-api-gateway`, started with `opa run --server --addr=:8181 -b /policies` — OPA's
  standard bundle-directory flag, not `--config-file` mode, since the bundle is a small, fully
  static set of files.
- `templates/opa-policy-bundle-configmap.yaml` embeds
  `example-policies/no-chaos-injection-without-approval.rego` and
  `example-policies/production-namespace-requires-teardown-node.rego` verbatim as ConfigMap
  data keys, mounted at `/policies` in the sidecar — matching the `-b /policies` flag exactly.
- `values.yaml`'s new `opaSidecar.*` block (default `enabled: true`) follows the chart's
  existing image/resources value-shape conventions; `env.opaSidecarBaseUrl` wires the gateway
  container's `OPA_SIDECAR_BASE_URL` to `http://localhost:8181` so the adapter's default and
  this chart's actual sidecar port can never silently drift apart.
- Port `8181` was read directly out of `opa-rego-policy-evaluator.adapter.ts`'s default
  `OPA_SIDECAR_BASE_URL` rather than assumed, so the sidecar and the adapter agree without
  either side needing an override.

**ClickHouse storage tiering, made real end-to-end instead of documented-only:**

- `services/telemetry-lake/clickhouse-schema-migrations/server-config/storage-policy.xml` is
  the actual, loadable config XML migration 005's comment block previously only described:
  `hot_to_cold_tiered` policy, `default` disk for the hot volume, S3-backed `cold_tier_s3` disk
  for the cold volume — names chosen to match migration 005's existing SQL exactly (no rename
  was needed; this file is now the authoritative source for that naming going forward).
- `server-config/storage-policy.local-dev.xml` is a local-disk variant (same policy/volume
  names, `<type>local</type>` cold disk instead of S3) — `docker-compose.yml`'s
  `telemetry-metrics-clickhouse` now mounts it plus a second bind-mounted volume
  (`.docker-volumes/clickhouse-cold-tier`), so local dev is no longer single-disk. It exercises
  the identical `TTL ... TO VOLUME` mechanism as staging/production, just backed by a cheaper,
  simpler local disk.
- `infrastructure/kubernetes/base/telemetry-clickhouse-statefulset.yaml` is a new StatefulSet
  (none previously existed in `infrastructure/kubernetes/base/`) mounting `storage-policy.xml`
  via a ConfigMap into `/etc/clickhouse-server/config.d/`, with one `volumeClaimTemplates` entry
  for the hot tier (base's cold tier is S3, so it needs no PVC by default).
- `infrastructure/kubernetes/overlays/production/clickhouse-cold-tier-volume-patch.yaml` (a
  JSON6902 patch, since `volumeClaimTemplates` has no merge key for a strategic-merge patch to
  key off of) appends a second, distinct PVC as the on-cluster cold-tier alternative for
  clusters/cloud accounts without S3 access. Staging deliberately does not apply this patch —
  it stays on base's single hot-tier PVC + S3 cold tier, the same way staging already runs
  smaller than production elsewhere in this repo (e.g. `worker-hpa-ceiling-patch.yaml`).

## Consequences

**Positive:**

- The Helm-deployed `pipeline-api-gateway` can now actually trigger a run end-to-end without an
  operator manually patching in an OPA sidecar first — closing the fail-closed operational gap
  ADR 0005 flagged as a negative trade-off.
- The two `example-policies/*.rego` files are no longer just example/reference Rego sitting
  next to the adapter that calls them — they are the actual bundle a Helm-deployed gateway
  evaluates against.
- Local development now exercises the real hot/cold tiering mechanism (`TTL ... TO VOLUME`,
  `storage_policy`) instead of that code path being entirely untested outside staging/
  production — migration 005 succeeds identically in every environment instead of being a
  documented no-op-fail case locally.
- `infrastructure/kubernetes/base/` gaining a ClickHouse StatefulSet closes a second, smaller
  gap as a side effect: previously there was no raw-Kustomize reference manifest for
  `telemetry-metrics-clickhouse` at all, only the Helm-adjacent `docker-compose.yml` version.

**Negative / trade-offs:**

- The OPA policy bundle ConfigMap is a hand-copied embed of `example-policies/*.rego`, not a
  `configMapGenerator` or build-time codegen step — a future change to either `.rego` file must
  be copied into `opa-policy-bundle-configmap.yaml` by hand (or via a follow-up CI drift check)
  until that's automated. Both files' comments call this out explicitly.
- `storage-policy.xml`'s S3 disk uses environment-derived credentials
  (`use_environment_credentials`) and a placeholder bucket endpoint — real deployment still
  requires ops to point it at an actual bucket/IAM role via config management; this ADR makes
  the config shape real and loadable, not the credential provisioning around it.
- The production cold-tier PVC patch is an *alternative* on-cluster path, not the default cold
  tier (S3 remains the default in `storage-policy.xml`) — a cluster that actually wants to use
  that PVC as ClickHouse's cold disk still needs its own `storage-policy.xml` variant pointing
  the `cold` volume at a `local`-type disk under that PVC's mount path, the same way
  `storage-policy.local-dev.xml` does for local dev. This ADR wires up the PVC and documents the
  pattern; it does not ship that variant XML pre-made, since no production cluster in this repo
  currently needs it.
- This ADR does not touch the scheduler's HTTP entrypoint or approval auto-resume gaps —
  those remain open, tracked separately, and are explicitly out of scope here.
