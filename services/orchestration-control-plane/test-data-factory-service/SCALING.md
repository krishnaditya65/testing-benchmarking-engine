# Scaling notes

This service is stateless and horizontally scalable: each generation/seeding
job is a self-contained invocation of `createTestDataFactory(...).seedDataset(...)`
with no shared in-memory state across invocations, so any number of job
instances can run concurrently without coordination beyond the DAG
scheduler's own step ordering. It scales vertically for the CPU-bound part
of the work (faker-driven record generation and SHA-256 masking benefit
from more cores/clock speed per instance), and horizontally by sharding
entity generation across worker instances -- for example, running one
instance per entity within a topological "wave" produced by
`RelationalDatasetOrchestrator.computeTopologicalGenerationOrder`, since
entities with no FK dependency on each other can safely generate in
parallel. `RelationalDatasetOrchestrator.generateBatchesInDependencyOrder`
streams fixed-size batches (default 10,000 records) rather than building
one giant in-memory array, which keeps peak memory bounded even when a
`DataSeedNode` step asks for millions of rows for a single entity. The
failure mode for a crashed generation job is a straightforward retry of
that job from its last completed batch; **note that this starter repo does
not yet implement batch-offset checkpointing**, so a resumed job currently
re-generates from the beginning of the failed entity rather than from the
last successfully-seeded batch -- tracking and persisting batch offsets per
job is a real TODO before this is safe to run unattended against very
large entities. Both `PostgresSeedingAdapter` and `MongoSeedingAdapter` are
constructor-injected with a pooled client (`pg.Pool`, and the `mongodb.Db`
handle backed by the driver's own connection pool), so a transient
connection loss to the target database surfaces as a failed batch rather
than crashing the whole process.
