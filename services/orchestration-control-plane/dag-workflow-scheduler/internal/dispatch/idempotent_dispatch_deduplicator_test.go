package dispatch

import (
	"context"
	"testing"
	"time"
)

func TestInMemoryDispatchDeduplicationStore_SecondCallForSameKeyIsNotRecordedAgain(t *testing.T) {
	store := NewInMemoryDispatchDeduplicationStore()

	recordedNow, err := store.MarkDispatchedIfAbsent(context.Background(), "key-a", time.Hour)
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if !recordedNow {
		t.Fatal("first call for a fresh key must report recordedNow=true")
	}

	recordedNow, err = store.MarkDispatchedIfAbsent(context.Background(), "key-a", time.Hour)
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
	if recordedNow {
		t.Fatal("second call for the same, still-live key must report recordedNow=false")
	}
}

func TestInMemoryDispatchDeduplicationStore_DistinctKeysAreIndependent(t *testing.T) {
	store := NewInMemoryDispatchDeduplicationStore()

	for _, key := range []string{"key-a", "key-b", "key-c"} {
		recordedNow, err := store.MarkDispatchedIfAbsent(context.Background(), key, time.Hour)
		if err != nil {
			t.Fatalf("key %q: unexpected error: %v", key, err)
		}
		if !recordedNow {
			t.Fatalf("key %q: expected recordedNow=true for a never-before-seen key", key)
		}
	}
}

// TestInMemoryDispatchDeduplicationStore_ExpiredRecordCanBeRecordedAgain
// proves the TTL is actually honored: once the record's expiry has passed, a
// key that was previously dispatched is treated as fresh again. This
// matters operationally -- without expiry, a long-lived scheduler process's
// dedup map would grow forever across every node of every pipeline run it
// ever dispatched.
func TestInMemoryDispatchDeduplicationStore_ExpiredRecordCanBeRecordedAgain(t *testing.T) {
	store := NewInMemoryDispatchDeduplicationStore()

	currentTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return currentTime }

	recordedNow, err := store.MarkDispatchedIfAbsent(context.Background(), "expiring-key", time.Minute)
	if err != nil {
		t.Fatalf("initial call: unexpected error: %v", err)
	}
	if !recordedNow {
		t.Fatal("initial call must report recordedNow=true")
	}

	// Still within the TTL window: must be suppressed as a duplicate.
	currentTime = currentTime.Add(30 * time.Second)
	recordedNow, err = store.MarkDispatchedIfAbsent(context.Background(), "expiring-key", time.Minute)
	if err != nil {
		t.Fatalf("within-TTL call: unexpected error: %v", err)
	}
	if recordedNow {
		t.Fatal("within-TTL call must report recordedNow=false")
	}

	// Past the TTL window: the key should be treated as fresh again.
	currentTime = currentTime.Add(2 * time.Minute)
	recordedNow, err = store.MarkDispatchedIfAbsent(context.Background(), "expiring-key", time.Minute)
	if err != nil {
		t.Fatalf("post-TTL call: unexpected error: %v", err)
	}
	if !recordedNow {
		t.Fatal("post-TTL call must report recordedNow=true (previous record expired)")
	}
}
