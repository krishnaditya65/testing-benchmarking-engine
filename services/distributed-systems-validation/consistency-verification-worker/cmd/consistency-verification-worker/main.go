// Command consistency-verification-worker drives "Data Consistency Benchmarking" (see
// feature-manifest section 5, "Distributed Systems & Infrastructure Validation") against a
// target CockroachDB, Cassandra, or ScyllaDB cluster: it writes at a configurable quorum level,
// coordinates with a chaos-ebpf-sidecar-induced network partition via the same
// chaos.fault-injection-event.v1 Redpanda topic contract every chaos-aware service consumes,
// and reports ACID/eventual-consistency violations it observes across replicas.
//
// It never talks to other services synchronously — matching ARCHITECTURE.md's "no synchronous
// RPC that could cascade a failure backward" rule — it only consumes the chaos fault-injection
// topic and (once wired to a real telemetry exporter) emits its ConsistencyVerificationReport
// outward via OTLP, the same way every other execution/validation worker does.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-testing-benchmarking-tool/consistency-verification-worker/internal/consistency"
)

// config holds every operator-tunable setting for this process, populated from flags with
// environment-variable fallbacks — mirrors the pattern in
// services/execution-fleet/chaos-ebpf-sidecar/cmd/chaos-ebpf-sidecar/main.go so both services
// configure the same way under `go run` locally and as a Kubernetes container.
type config struct {
	// targetDatabaseKind selects which DatabaseConsistencyDriver to construct: "cockroachdb" or
	// "cassandra" (also used for ScyllaDB, which speaks the same CQL wire protocol).
	targetDatabaseKind string
	// targetHosts is the comma-separated seed host list for the target cluster.
	targetHosts string
	// requestedConsistencyLevel is the ConsistencyLevel writes are driven at during
	// verification runs (see internal/consistency/quorum_write_driver.go).
	requestedConsistencyLevel string
	// redpandaBrokers is the comma-separated bootstrap broker list for consuming
	// chaos.fault-injection-event.v1 events describing when a partition is active.
	redpandaBrokers string
	// otelCollectorEndpoint is the OTLP gRPC endpoint of the node-local OTel Collector
	// DaemonSet, per ARCHITECTURE.md's telemetry contract.
	otelCollectorEndpoint string
}

func loadConfigFromFlags() config {
	var cfg config

	flag.StringVar(&cfg.targetDatabaseKind, "target-database-kind", envOrDefault("TARGET_DATABASE_KIND", "cockroachdb"), "distributed database under test: cockroachdb | cassandra")
	flag.StringVar(&cfg.targetHosts, "target-hosts", envOrDefault("TARGET_HOSTS", "localhost:26257"), "comma-separated seed hosts for the target cluster")
	flag.StringVar(&cfg.requestedConsistencyLevel, "consistency-level", envOrDefault("REQUESTED_CONSISTENCY_LEVEL", string(consistency.ConsistencyLevelQuorum)), "consistency level to drive writes at: ONE | QUORUM | ALL | LINEARIZABLE")
	flag.StringVar(&cfg.redpandaBrokers, "redpanda-brokers", envOrDefault("REDPANDA_BROKERS", "localhost:9092"), "comma-separated Redpanda bootstrap brokers")
	flag.StringVar(&cfg.otelCollectorEndpoint, "otel-collector-endpoint", envOrDefault("OTEL_COLLECTOR_ENDPOINT", "localhost:4317"), "OTLP gRPC endpoint of the local OTel Collector")
	flag.Parse()

	return cfg
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// buildDriver constructs the DatabaseConsistencyDriver named by cfg.targetDatabaseKind. Both
// concrete drivers are currently stubs (see internal/consistency/quorum_write_driver.go's NOTE
// comments) — this function exists so the wiring point for a real pgx/gocql-backed driver is
// visible and named correctly even though it cannot be constructed yet.
func buildDriver(cfg config, seedReplicas []consistency.ReplicaEndpoint) (consistency.DatabaseConsistencyDriver, error) {
	switch cfg.targetDatabaseKind {
	case "cockroachdb":
		return consistency.NewCockroachDbDriver(seedReplicas), nil
	case "cassandra", "scylladb":
		return consistency.NewCassandraDriver(seedReplicas, 3), nil
	default:
		return nil, unsupportedDatabaseKindError{kind: cfg.targetDatabaseKind}
	}
}

type unsupportedDatabaseKindError struct{ kind string }

func (e unsupportedDatabaseKindError) Error() string {
	return "consistency-verification-worker: unsupported --target-database-kind " + e.kind + " (expected cockroachdb | cassandra | scylladb)"
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfigFromFlags()

	seedReplicas := parseSeedReplicas(cfg.targetHosts)
	driver, err := buildDriver(cfg, seedReplicas)
	if err != nil {
		logger.Error("startup refused", "error", err)
		os.Exit(1)
	}

	acidSuite := consistency.NewAcidPropertyAssertionSuite(driver)
	partitionVerifier := consistency.NewPartitionInducedReadVerifier(driver)

	logger.Info("consistency-verification-worker started",
		"target_database_kind", cfg.targetDatabaseKind,
		"target_hosts", cfg.targetHosts,
		"requested_consistency_level", cfg.requestedConsistencyLevel,
		"redpanda_brokers", cfg.redpandaBrokers,
		"otel_collector_endpoint", cfg.otelCollectorEndpoint,
	)

	// NOTE: production wiring subscribes here to the chaos.fault-injection-event.v1 Redpanda
	// topic (shared-libraries/protocol-contracts/proto/chaos_fault_injection.proto), and for
	// every event whose lifecycle_state transitions to CHAOS_FAULT_LIFECYCLE_STATE_ACTIVE,
	// resolves a PartitionTopology for the affected replicas and calls
	// partitionVerifier.Verify(...), then publishes the resulting ConsistencyVerificationReport
	// (and acidSuite.RunAll's AcidAssertionSuiteReport, run once per environment as a baseline
	// check independent of any specific partition) via OTLP to the collector. That consumer
	// loop, and the OTLP exporter construction, are not implemented in this starter repo — see
	// internal/consistency/partition_induced_read_verifier.go and
	// internal/consistency/acid_property_assertion_suite.go for what each call produces.
	_ = acidSuite
	_ = partitionVerifier

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-ctx.Done()
	logger.Info("consistency-verification-worker shutdown complete")
}

// parseSeedReplicas turns a comma-separated host:port list into ReplicaEndpoints with
// synthetic NodeIDs, sufficient for local development/tests. Production wiring resolves real
// NodeID/DatacenterOrRegion values from the target database's own cluster-topology API
// (`SHOW RANGES` / `nodetool status`) rather than this flat parse.
func parseSeedReplicas(hosts string) []consistency.ReplicaEndpoint {
	var replicas []consistency.ReplicaEndpoint
	start := 0
	nodeIndex := 0
	for i := 0; i <= len(hosts); i++ {
		if i == len(hosts) || hosts[i] == ',' {
			if i > start {
				hostPort := hosts[start:i]
				replicas = append(replicas, consistency.ReplicaEndpoint{
					NodeID:   nodeName(nodeIndex),
					HostPort: hostPort,
				})
				nodeIndex++
			}
			start = i + 1
		}
	}
	return replicas
}

func nodeName(index int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	if index < len(letters) {
		return "node-" + string(letters[index])
	}
	return "node-" + string(rune('a'+index))
}
