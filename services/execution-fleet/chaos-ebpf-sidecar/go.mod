module github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar

go 1.22

require (
	github.com/cilium/ebpf v0.15.0
	k8s.io/api v0.30.2
	k8s.io/apimachinery v0.30.2
	k8s.io/client-go v0.30.2
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.28.0
	go.opentelemetry.io/otel/sdk v1.28.0
	google.golang.org/grpc v1.65.0
)

// NOTE: This go.mod is hand-written for a starter/offline repository. Running
// `go mod tidy` against a live module proxy is required before this service can
// actually be built, in order to resolve the full transitive dependency graph
// and populate go.sum. No network access is available in this environment.
