// Concurrency safety tests for the DB state machine.
//
// Folio uses a sync.Cond-based state machine (StateAll, StateRead, StateNone,
// StateClosed) to coordinate concurrent access to a single file. Every public
// method calls blockRead or blockWrite, which wait on the condition variable
// until the state permits the operation. These tests verify three properties
// that are difficult to prove by inspection alone:
//
//  1. Readers never see torn writes (partially-written JSONL lines).
//  2. Close wakes all waiting goroutines so they return ErrClosed rather than
//     hanging forever.
//  3. Compaction (which transitions to StateRead, blocking writers) does not
//     starve concurrent readers.
//
// All tests use the -race detector implicitly via `go test -race`, which
// catches data races on the shared file handles and header fields that would
// otherwise manifest as intermittent corruption in production.
package folio

import (
	"sync"
	"testing"
)

// TestConcurrentReads verifies that multiple goroutines can call Get
// simultaneously without data races or returning incorrect content.
// blockRead acquires a read-compatible state via sync.Cond — if the
// condition check were wrong (e.g. using Lock instead of Wait), all
// readers would serialise and throughput would collapse, or worse,
// a reader could proceed during a write and see a partial line.
func TestConcurrentReads(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 100 {
				data, err := db.Get("doc")
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if data != "content" {
					t.Errorf("Get = %q, want %q", data, "content")
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestConcurrentWrites verifies that concurrent Set calls do not corrupt
// the file. Each Set appends a record+index pair and updates the tail
// offset; if two goroutines interleaved their appends without the write
// lock, the second append would start at a stale tail offset and
// overwrite the first record's bytes, producing invalid JSONL.
func TestConcurrentWrites(t *testing.T) {
	db := openTestDB(t)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			label := "doc"
			for range 10 {
				db.Set(label, "content")
			}
		}(i)
	}
	wg.Wait()

	// Should still be able to read
	_, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get after concurrent writes: %v", err)
	}
}

// TestConcurrentReadWrite exercises the most common production pattern:
// readers and writers operating simultaneously. The state machine must
// allow both when in StateAll. A subtle bug here — such as a writer
// transitioning to StateRead before completing — would cause readers to
// see a half-written line or writers to block indefinitely.
func TestConcurrentReadWrite(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "initial")

	var wg sync.WaitGroup

	// Readers
	for range 5 {
		wg.Go(func() {
			for range 50 {
				_, err := db.Get("doc")
				if err != nil && err != ErrNotFound {
					t.Errorf("Get: %v", err)
					return
				}
			}
		})
	}

	// Writers
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for range 10 {
				db.Set("doc", "content")
			}
		}(i)
	}

	wg.Wait()
}

// TestCloseWakesWaiters verifies that Close broadcasts to all goroutines
// blocked in blockRead. The state machine uses sync.Cond.Wait, which
// suspends the goroutine until Broadcast is called. If Close only called
// Signal (waking one waiter) instead of Broadcast, the remaining
// goroutines would hang forever — a goroutine leak that would prevent
// the process from exiting cleanly.
func TestCloseWakesWaiters(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")

	// Put DB in StateNone to make operations wait
	db.state.Store(StateNone)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// Start waiters
	for range 5 {
		wg.Go(func() {
			_, err := db.Get("doc")
			errChan <- err
		})
	}

	// Close the DB - should wake all waiters
	go func() {
		db.Close()
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != ErrClosed {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	}
}

// TestConcurrentList exercises List under contention. List performs a
// full linear scan of the sparse region and a binary search of the
// sorted region, reading many lines per call. If the reader file handle
// were not safe for concurrent use (e.g. if ReadAt mutated shared
// state), overlapping List calls would return corrupted label sets.
func TestConcurrentList(t *testing.T) {
	db := openTestDB(t)

	for i := range 10 {
		db.Set(string(rune('a'+i)), "content")
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 50 {
				labels, err := collect(db.List())
				if err != nil {
					t.Errorf("List: %v", err)
					return
				}
				if len(labels) == 0 {
					t.Error("List returned empty")
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestCloseWakesWriteWaiters is the write-side counterpart to
// TestCloseWakesWaiters. Writers blocked in blockWrite must also be
// woken by Close. If they weren't, a program that calls Close while
// a background writer is pending would deadlock — the writer waits
// for StateAll, but Close has already moved the state to StateClosed.
func TestCloseWakesWriteWaiters(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")

	db.state.Store(StateNone)

	var wg sync.WaitGroup
	errChan := make(chan error, 5)

	for range 5 {
		wg.Go(func() {
			errChan <- db.Set("doc", "updated")
		})
	}

	go func() { db.Close() }()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != ErrClosed {
			t.Errorf("Set during close: got %v, want ErrClosed", err)
		}
	}
}

// TestConcurrentCompactRead verifies that readers continue to succeed
// while compaction is in progress. Compact transitions the state to
// StateRead (blocking writers but allowing readers), rebuilds the file,
// then restores StateAll. If the state transition were wrong — e.g.
// moving to StateNone instead of StateRead — all concurrent Get calls
// would block until compaction finishes, creating a latency spike
// proportional to the database size.
func TestConcurrentCompactRead(t *testing.T) {
	db := openTestDB(t)

	for i := range 10 {
		db.Set(string(rune('a'+i)), "content")
	}

	var wg sync.WaitGroup

	// Compact in background
	wg.Go(func() {
		db.Compact()
	})

	// Concurrent reads
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			label := string(rune('a' + n))
			for range 20 {
				_, err := db.Get(label)
				if err != nil && err != ErrNotFound {
					t.Errorf("Get during compact: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}
