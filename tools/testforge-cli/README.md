# testforge-cli

Headless Go CLI for GitOps-native pipeline execution in CI/CD. `testforge` is how
declarative pipeline definitions committed alongside application code get lint-checked
before merge and executed against `pipeline-api-gateway` as part of a normal
CI/CD run — no human clicking "run" in `workflow-canvas-frontend` required.

## Implements

- Declarative pipeline authoring for CI (`testforge run`, `testforge validate`)
- Pre-commit / pre-merge DAG lint step, independent of the control plane being
  reachable
- Quality-gate-to-exit-code mapping so GitHub Actions/GitLab CI/Jenkins can block a
  merge on a regression, matching the feature-manifest's "CI/CD quality gates" item

## Layout

```
testforge-cli/
├── main.go
├── go.mod
├── cmd/
│   ├── root.go       Cobra root command, shared flags (--api-gateway-url, --api-token)
│   ├── run.go         `testforge run` — submit + await a pipeline run
│   └── validate.go     `testforge validate` — local DAG lint, no network call
├── internal/
│   ├── pipelineconfig/
│   │   └── yaml_pipeline_definition_parser.go   YAML → DAG struct + validation
│   └── cicdintegration/
│       └── quality_gate_exit_code_mapper.go      QualityGateResult[] → process exit code
├── examples/
│   └── pipelines/
│       └── checkout-load-test.yaml   Reference pipeline: smoke test → seed 1M
│                                       rows → 50k-VU load test → assert DB
│                                       consistency → teardown
└── .github/workflows/example-usage.yml   Copy-paste GitHub Actions snippet
```

## Install

```bash
go install github.com/testforge/distributed-testing-benchmarking-tool/tools/testforge-cli@latest
```

Or build from source:

```bash
cd tools/testforge-cli
go build -o testforge .
```

## Usage

Validate a pipeline definition locally (no network access required — this is what
runs in a pre-commit hook):

```bash
testforge validate ./pipelines/checkout-load-test.yaml
```

Submit a pipeline to `pipeline-api-gateway` and block until it finishes:

```bash
testforge run \
  --pipeline ./pipelines/checkout-load-test.yaml \
  --environment staging \
  --api-token "$TESTFORGE_API_TOKEN"
```

`testforge run` exits:

| Exit code | Meaning |
|---|---|
| `0` | Every blocking quality gate passed |
| `1` | CLI-level error (bad YAML, network failure, auth failure) |
| `10` | At least one **blocking** quality gate failed |
| `11` | Only **warning**-severity gates failed |
| `20` | The pipeline run itself errored before gates could be evaluated |

See `internal/cicdintegration/quality_gate_exit_code_mapper.go` for the full mapping
logic — it's pure and unit-testable independent of the HTTP call.

## Pre-commit hook

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: testforge-validate
      name: Validate testforge pipeline definitions
      entry: testforge validate
      language: system
      files: ^pipelines/.*\.ya?ml$
```

## CI/CD

See `.github/workflows/example-usage.yml` for a full GitHub Actions job that lints on
every PR touching a pipeline file, then runs the pipeline and lets a non-zero exit
code from `testforge run` fail the job (and, via branch protection, block the merge).
The same two-command shape (`testforge validate` then `testforge run`) applies
directly to GitLab CI (`script:` stages) and a Jenkins `sh` step in a `Jenkinsfile`.

## Pipeline definition schema

A pipeline definition is a `PipelineDefinition` YAML document with a `spec.nodes` DAG
and an optional `spec.qualityGates` list. Node `type` values must be one of the kinds
`services/shared-libraries/dag-domain-model` defines:

- `environment-provision` — provisions an ephemeral namespace via
  `environment-lifecycle-manager` / `infrastructure/crossplane`
- `api-smoke-test` — fast functional pass over critical endpoints
- `database-seed` — synthetic/PII-masked data generation via `test-data-factory-service`
- `load-test` — sustained concurrent virtual-user traffic via `protocol-load-worker-go`
- `chaos-injection` — fault injection via `chaos-ebpf-sidecar`
- `database-consistency-assertion` — post-load data-integrity checks
- `environment-teardown` — tears down the ephemeral namespace

`examples/pipelines/checkout-load-test.yaml` uses six of these seven node types end
to end and is the canonical example to copy when authoring a new pipeline.
