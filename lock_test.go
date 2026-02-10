package folio

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLocking(t *testing.T) {
	tmp := t.TempDir()

	cfg := Config{
		HashAlgorithm: AlgXXHash3,
	}

	// Process 1: Open DB
	db1, err := Open(filepath.Join(tmp, "test.folio"), cfg)
	if err != nil {
		t.Fatalf("d1 open failed: %v", err)
	}
	defer db1.Close()

	// Process 2: Open DB (should succeed finding file, sharing lock is tricky to test in same process if flock is file-descriptor based)
	// flock is usually fd-based. If we open the file again, we get a new fd.
	db2, err := Open(filepath.Join(tmp, "test.folio"), cfg)
	if err != nil {
		t.Fatalf("db2 open failed: %v", err)
	}
	defer db2.Close()

	// 1. DB1 acquires Write Lock (Set)
	// We simulate a long write by holding the lock manually if we can,
	// but since blockWrite is internal, we can just call Set.

	// Better test: Acquire lock manually on db1.lock
	err = db1.lock.Lock(LockExclusive)
	if err != nil {
		t.Fatalf("db1 manual lock failed: %v", err)
	}

	// 2. DB2 tries to acquire Write Lock (should block/fail if we used non-blocking, but we use blocking).
	// Since it's blocking, running this in the same goroutine would deadlock if we wait forever.
	// We'll spawn a goroutine for DB2.

	done := make(chan bool)
	go func() {
		// Try to acquire lock
		err := db2.lock.Lock(LockExclusive)
		if err != nil {
			t.Errorf("db2 lock failed: %v", err)
		}
		db2.lock.Unlock()
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("db2 acquired lock while db1 held it!")
	case <-time.After(100 * time.Millisecond):
		// Expected: DB2 is blocked
	}

	// 3. DB1 release
	db1.lock.Unlock()

	// 4. DB2 should now succeed
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("db2 failed to acquire lock after release")
	}
}

func TestReadWriteLocking(t *testing.T) {
	tmp := t.TempDir()
	cfg := Config{HashAlgorithm: AlgXXHash3}

	db1, _ := Open(filepath.Join(tmp, "rw.folio"), cfg)
	defer db1.Close()

	db2, _ := Open(filepath.Join(tmp, "rw.folio"), cfg)
	defer db2.Close()

	// DB1 holds Shared Lock (Read)
	if err := db1.lock.Lock(LockShared); err != nil {
		t.Fatal(err)
	}

	// DB2 wants Exclusive Lock (Write) -> Should Block
	done := make(chan bool)
	go func() {
		db2.lock.Lock(LockExclusive)
		db2.lock.Unlock()
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("db2 acquired write lock while db1 held read lock")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	db1.lock.Unlock()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("db2 stuck")
	}
}
