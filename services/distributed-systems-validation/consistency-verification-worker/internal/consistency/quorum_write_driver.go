// Package consistency implements "Data Consistency Benchmarking" (see feature-manifest
// section 5, "Distributed Systems & Infrastructure Validation"): driving writes at a
// configurable quorum/consistency level against a distributed database, then — coordinated
// with a chaos-induced network partition — verifying reads across replicas do not violate the
// consistency guarantee the database claims to offer.
package consistency

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ConsistencyLevel is the write/read quorum requested for one operation, using Cassandra/
// ScyllaDB's naming (the more granular vocabulary) since CockroachDB's own levels (its default
// SERIALIZABLE isolation) map onto ONE/QUORUM/ALL coarsely enough for cross-database comparison
// in a single ConsistencyVerificationReport — see partition_induced_read_verifier.go.
type ConsistencyLevel string

const (
	// ConsistencyLevelOne requires exactly one replica to acknowledge — fastest, weakest.
	ConsistencyLevelOne ConsistencyLevel = "ONE"
	// ConsistencyLevelQuorum requires a majority of the configured replication factor to
	// acknowledge — the level under test for most "does eventual consistency actually converge
	// under partition" scenarios.
	ConsistencyLevelQuorum ConsistencyLevel = "QUORUM"
	// ConsistencyLevelAll requires every replica to acknowledge — strongest, used as the
	// control/baseline against which QUORUM/ONE results are compared.
	ConsistencyLevelAll ConsistencyLevel = "ALL"
	// ConsistencyLevelLinearizable is CockroachDB's default: every write and read goes through
	// its Raft-consensus range leaseholder, so the database itself claims linearizability
	// rather than exposing a client-tunable level. Requesting this level from a
	// DatabaseConsistencyDriver is only meaningful for CockroachDbDriver.
	ConsistencyLevelLinearizable ConsistencyLevel = "LINEARIZABLE"
)

// ReplicaEndpoint identifies one node in the cluster under test, addressable independently of
// the driver's normal load-balanced connection so partition_induced_read_verifier.go can target
// reads at a *specific* replica (including one on the minority side of an induced partition).
type ReplicaEndpoint struct {
	NodeID       string
	HostPort     string
	// DatacenterOrRegion groups replicas for multi-region partition scenarios — chaos-ebpf-
	// sidecar's induced partitions are typically drawn along this boundary (see
	// partition_induced_read_verifier.go's PartitionTopology).
	DatacenterOrRegion string
}

// WriteRequest is one row/key write to drive through the DatabaseConsistencyDriver.
type WriteRequest struct {
	Key   string
	Value []byte
	// AtConsistencyLevel is the level this specific write must be acknowledged at. Left
	// separate from a driver-wide default because ACID assertion suites (see
	// acid_property_assertion_suite.go) deliberately mix levels within one test scenario.
	AtConsistencyLevel ConsistencyLevel
}

// WriteAcknowledgement records what actually happened for one WriteRequest, including which
// replicas are known to have durably applied it — the ground truth
// partition_induced_read_verifier.go compares subsequent reads against.
type WriteAcknowledgement struct {
	Key                string
	AcknowledgedAt     time.Time
	AcknowledgedByNodeIDs []string
	// CommitIndexOrTimestamp is the driver-specific write-ordering token: a Raft log index for
	// CockroachDB, a write-timestamp (server-side, per Cassandra's last-write-wins semantics)
	// for Cassandra/ScyllaDB. Opaque to callers outside this package; used only for ordering
	// comparisons inside PartitionInducedReadVerifier.
	CommitIndexOrTimestamp int64
}

// ReadResult is one read of Key from a specific replica, used to detect divergence.
type ReadResult struct {
	Key                    string
	Value                  []byte
	Found                  bool
	ReadFromNodeID         string
	ReadAt                 time.Time
	CommitIndexOrTimestamp int64
}

// DatabaseConsistencyDriver is the seam between this package's write/read-comparison logic and
// a specific distributed database's real wire protocol. Implementations must be safe for
// concurrent use, since QuorumWriteBenchmarkDriver drives writes from many goroutines to
// simulate realistic concurrent load during a partition scenario.
type DatabaseConsistencyDriver interface {
	// DatabaseKind identifies which engine this driver talks to, stamped onto
	// ConsistencyVerificationReport so results from different engines are never silently mixed.
	DatabaseKind() string
	// Write performs req against the cluster and blocks until req.AtConsistencyLevel is
	// satisfied or ctx is cancelled/times out (the latter is itself a meaningful chaos-testing
	// signal: a write that cannot reach quorum during a partition should time out, not hang
	// forever or silently downgrade).
	Write(ctx context.Context, req WriteRequest) (WriteAcknowledgement, error)
	// ReadFromReplica performs a direct, replica-pinned read — critically, NOT load-balanced —
	// against endpoint, so PartitionInducedReadVerifier can compare what the minority and
	// majority sides of a partition each believe the current value is.
	ReadFromReplica(ctx context.Context, endpoint ReplicaEndpoint, key string) (ReadResult, error)
	// ListReplicas returns the current replica set responsible for key, so the verifier knows
	// which ReplicaEndpoints to read from without hardcoding cluster topology.
	ListReplicas(ctx context.Context, key string) ([]ReplicaEndpoint, error)
}

// QuorumWriteBenchmarkDriver drives a batch of WriteRequests concurrently against an underlying
// DatabaseConsistencyDriver and collects every WriteAcknowledgement (and error) it produced,
// preserving per-write ordering information needed later to detect lost writes.
type QuorumWriteBenchmarkDriver struct {
	driver DatabaseConsistencyDriver
}

// NewQuorumWriteBenchmarkDriver wraps driver with the concurrent-batch-write benchmarking
// behavior used by ConsistencyVerificationReport generation.
func NewQuorumWriteBenchmarkDriver(driver DatabaseConsistencyDriver) *QuorumWriteBenchmarkDriver {
	return &QuorumWriteBenchmarkDriver{driver: driver}
}

// WriteBatchOutcome is one WriteRequest's outcome inside a driven batch: exactly one of
// Acknowledgement or Err is populated.
type WriteBatchOutcome struct {
	Request        WriteRequest
	Acknowledgement WriteAcknowledgement
	Err            error
}

// DriveConcurrentWrites issues every request in requests concurrently (one goroutine per
// request, capped by maxInFlight) against the wrapped driver and returns one WriteBatchOutcome
// per request, in the same order requests were given — callers must not assume completion
// order matched submission order, only that the returned slice is index-aligned with requests.
func (d *QuorumWriteBenchmarkDriver) DriveConcurrentWrites(
	ctx context.Context,
	requests []WriteRequest,
	maxInFlight int,
) []WriteBatchOutcome {
	if maxInFlight <= 0 {
		maxInFlight = 1
	}

	outcomes := make([]WriteBatchOutcome, len(requests))
	semaphore := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup

	for i, req := range requests {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(index int, request WriteRequest) {
			defer wg.Done()
			defer func() { <-semaphore }()

			ack, err := d.driver.Write(ctx, request)
			outcomes[index] = WriteBatchOutcome{Request: request, Acknowledgement: ack, Err: err}
		}(i, req)
	}

	wg.Wait()
	return outcomes
}

// ---------------------------------------------------------------------------------------------
// CockroachDbDriver
// ---------------------------------------------------------------------------------------------

// CockroachDbDriver is a DatabaseConsistencyDriver for CockroachDB.
//
// NOTE: real wiring requires github.com/jackc/pgx/v5 (CockroachDB speaks the Postgres wire
// protocol) with connection options set for its SQL-level transaction retry contract:
//
//	pool, err := pgxpool.New(ctx, connString) // connString: "postgresql://root@node:26257/bench?sslmode=verify-full"
//	err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
//	    _, err := tx.Exec(ctx, "UPSERT INTO consistency_probe (key, value, written_at) VALUES ($1, $2, now())", req.Key, req.Value)
//	    return err
//	}) // crdbpgx wraps ExecuteTx with CockroachDB's client-side transaction-retry loop
//	    // (github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx), required because CockroachDB
//	    // surfaces serialization conflicts as retryable errors rather than blocking.
//
// CockroachDB does not expose a client-tunable consistency level the way Cassandra does — every
// read/write is linearizable by default via Raft consensus per range — so Write always targets
// ConsistencyLevelLinearizable regardless of req.AtConsistencyLevel, and ReadFromReplica's
// endpoint targeting requires issuing `SET follower_read_timestamp` style hints or connecting
// directly to a specific node's SQL port to observe (intentionally) stale follower reads.
type CockroachDbDriver struct {
	// clusterNodes is the full set of SQL-accessible nodes this driver was configured against.
	// Kept here (rather than only inside a real pgx pool) because ListReplicas needs a stable
	// topology view independent of pgx's internal connection routing.
	clusterNodes []ReplicaEndpoint
}

// NewCockroachDbDriver constructs a driver scoped to the given cluster topology.
//
// NOTE: this constructor does not open any real connection — see the type doc comment for what
// real pgx pool construction requires. It exists so callers can wire
// QuorumWriteBenchmarkDriver/PartitionInducedReadVerifier against a concrete type today, and
// swap this constructor's body for real pgx dialing without changing any call site.
func NewCockroachDbDriver(clusterNodes []ReplicaEndpoint) *CockroachDbDriver {
	return &CockroachDbDriver{clusterNodes: clusterNodes}
}

func (d *CockroachDbDriver) DatabaseKind() string { return "cockroachdb" }

func (d *CockroachDbDriver) Write(ctx context.Context, req WriteRequest) (WriteAcknowledgement, error) {
	return WriteAcknowledgement{}, fmt.Errorf(
		"cockroachdb driver: Write not wired to a real pgx connection pool (key %q) — "+
			"see CockroachDbDriver's doc comment for the crdbpgx.ExecuteTx call this must make",
		req.Key,
	)
}

func (d *CockroachDbDriver) ReadFromReplica(ctx context.Context, endpoint ReplicaEndpoint, key string) (ReadResult, error) {
	return ReadResult{}, fmt.Errorf(
		"cockroachdb driver: ReadFromReplica not wired to a real per-node pgx connection (node %q, key %q)",
		endpoint.NodeID, key,
	)
}

func (d *CockroachDbDriver) ListReplicas(ctx context.Context, key string) ([]ReplicaEndpoint, error) {
	if len(d.clusterNodes) == 0 {
		return nil, fmt.Errorf("cockroachdb driver: no cluster nodes configured")
	}
	// NOTE: a real implementation queries `SHOW RANGES FROM TABLE consistency_probe` (or
	// crdb_internal.ranges) to find the actual replica set owning key's range, rather than
	// returning every configured node — CockroachDB's replication factor is typically 3 or 5,
	// far fewer than the full cluster size.
	return d.clusterNodes, nil
}

// ---------------------------------------------------------------------------------------------
// CassandraDriver
// ---------------------------------------------------------------------------------------------

// CassandraDriver is a DatabaseConsistencyDriver for Cassandra and ScyllaDB (both speak
// Cassandra's CQL wire protocol, so one driver implementation covers both).
//
// NOTE: real wiring requires github.com/gocql/gocql:
//
//	cluster := gocql.NewCluster(hosts...)
//	cluster.Consistency = mapConsistencyLevel(req.AtConsistencyLevel) // gocql.Quorum, gocql.One, gocql.All
//	session, err := cluster.CreateSession()
//	err = session.Query(`INSERT INTO consistency_probe (key, value, written_at) VALUES (?, ?, toTimestamp(now()))`,
//	    req.Key, req.Value).Consistency(mapConsistencyLevel(req.AtConsistencyLevel)).WithContext(ctx).Exec()
//
// ReadFromReplica's replica-pinning requires gocql's HostFilter/policy hooks
// (gocql.WhiteListHostPolicy or a custom gocql.HostSelectionPolicy scoped to endpoint.HostPort)
// since gocql's default token-aware policy will otherwise route a query to whichever replica it
// judges fastest — the opposite of what a partition-side-comparison read needs.
type CassandraDriver struct {
	seedHosts []ReplicaEndpoint
	// replicationFactor informs ListReplicas' NOTE-documented fallback below; a real
	// implementation instead derives the actual owning replicas per key from the cluster's
	// token ring (via gocql's Session.hostSource / an explicit `nodetool getendpoints` style
	// query), which is not resolvable from replicationFactor alone.
	replicationFactor int
}

// NewCassandraDriver constructs a driver scoped to the given seed hosts and replication factor.
//
// NOTE: this constructor does not open any real session — see the type doc comment.
func NewCassandraDriver(seedHosts []ReplicaEndpoint, replicationFactor int) *CassandraDriver {
	return &CassandraDriver{seedHosts: seedHosts, replicationFactor: replicationFactor}
}

func (d *CassandraDriver) DatabaseKind() string { return "cassandra" }

func (d *CassandraDriver) Write(ctx context.Context, req WriteRequest) (WriteAcknowledgement, error) {
	return WriteAcknowledgement{}, fmt.Errorf(
		"cassandra driver: Write not wired to a real gocql session (key %q, consistency %s) — "+
			"see CassandraDriver's doc comment for the session.Query(...).Consistency(...) call this must make",
		req.Key, req.AtConsistencyLevel,
	)
}

func (d *CassandraDriver) ReadFromReplica(ctx context.Context, endpoint ReplicaEndpoint, key string) (ReadResult, error) {
	return ReadResult{}, fmt.Errorf(
		"cassandra driver: ReadFromReplica not wired to a real replica-pinned gocql session (node %q, key %q)",
		endpoint.NodeID, key,
	)
}

func (d *CassandraDriver) ListReplicas(ctx context.Context, key string) ([]ReplicaEndpoint, error) {
	if len(d.seedHosts) == 0 {
		return nil, fmt.Errorf("cassandra driver: no seed hosts configured")
	}
	limit := d.replicationFactor
	if limit <= 0 || limit > len(d.seedHosts) {
		limit = len(d.seedHosts)
	}
	// NOTE: a real implementation resolves key's actual owning replicas from the cluster's
	// token ring rather than taking the first replicationFactor seed hosts — this is a
	// placeholder that is only correct when seedHosts happens to already be key's owning set.
	return d.seedHosts[:limit], nil
}
