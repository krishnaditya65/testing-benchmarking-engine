module github.com/distributed-testing-benchmarking-tool/consistency-verification-worker

go 1.22

// NOTE: This go.mod intentionally lists no third-party requires. Every real distributed-database
// client this service will eventually drive (github.com/gocql/gocql for Cassandra/ScyllaDB,
// github.com/jackc/pgx/v5 for CockroachDB's Postgres wire protocol, go.opentelemetry.io/otel for
// telemetry export per ARCHITECTURE.md's OTLP contract) is referenced only in NOTE comments in
// internal/consistency/quorum_write_driver.go, not vendored or imported — this is a
// starter/offline repository with no module-proxy network access, matching the pattern in
// services/execution-fleet/chaos-ebpf-sidecar/go.mod. Adding a real driver means: add it here as
// a `require`, run `go mod tidy` against a live proxy to populate go.sum, then swap the relevant
// NOTE-commented constructor for a real one.
