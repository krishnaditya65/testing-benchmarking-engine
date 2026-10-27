package dispatch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// dispatchIdempotencyKeyNamespace scopes the derived key to this specific
// wire contract (NodeDispatchTopic's payload shape) so that, if some other
// idempotency-keyed operation is ever added to this package sharing the same
// DispatchDeduplicationStore, its keys cannot collide with dispatch keys
// even given the same (pipelineRunID, nodeID, attemptGeneration) tuple.
const dispatchIdempotencyKeyNamespace = "dag.node.dispatch.v1"

// DeriveDispatchIdempotencyKey computes the deterministic key that identifies
// "dispatch pipeline node nodeID, for pipeline run pipelineRunID, on attempt
// attemptGeneration" as a single, at-most-once effect.
//
// attemptGeneration is expected to be the Temporal activity attempt number
// (activity.GetInfo(ctx).Attempt in workflow/activities.go) for the call
// that is about to publish to NodeDispatchTopic. Composing the key from all
// three fields means:
//
//   - Two calls describing the *same* attempt (e.g. a scheduler process that
//     crashed after successfully writing to Redpanda but before Temporal
//     durably recorded the activity as complete, so the still-in-flight
//     attempt gets re-invoked against the same task token) derive the exact
//     same key and are recognized as the same logical dispatch by
//     DispatchDeduplicationStore — this is the "50k-VU load test doesn't get
//     run twice" guarantee.
//   - A genuinely new attempt (Temporal bumped attemptGeneration because the
//     previous attempt failed outright, e.g. Redpanda was unreachable and
//     nothing was ever produced) intentionally derives a *different* key, so
//     the retry is allowed to dispatch — suppressing that case would turn
//     transient-failure retries into permanently stuck nodes.
//
// The key itself is a hex-encoded SHA-256 digest rather than the raw
// concatenated string so that (a) it has a fixed, predictable length
// regardless of how large pipelineRunID/nodeID get, and (b) it is safe to
// use directly as a cache/Redis key without further escaping.
func DeriveDispatchIdempotencyKey(pipelineRunID string, nodeID string, attemptGeneration int32) string {
	raw := fmt.Sprintf("%s|%s|%s|%d", dispatchIdempotencyKeyNamespace, pipelineRunID, nodeID, attemptGeneration)
	digest := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(digest[:])
}
