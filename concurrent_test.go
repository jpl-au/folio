package folio

import (
	"sync"
	"testing"
)

func TestConcurrentReads(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
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
		}()
	}
	wg.Wait()
}

func TestConcurrentWrites(t *testing.T) {
	db := openTestDB(t)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			label := "doc"
			for j := 0; j < 10; j++ {
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

func TestConcurrentReadWrite(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "initial")

	var wg sync.WaitGroup

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := db.Get("doc")
				if err != nil && err != ErrNotFound {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}()
	}

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				db.Set("doc", "content")
			}
		}(i)
	}

	wg.Wait()
}

func TestCloseWakesWaiters(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")

	// Put DB in StateNone to make operations wait
	db.state.Store(StateNone)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// Start waiters
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Get("doc")
			errChan <- err
		}()
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

func TestConcurrentList(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 10; i++ {
		db.Set(string(rune('a'+i)), "content")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				labels, err := db.List()
				if err != nil {
					t.Errorf("List: %v", err)
					return
				}
				if len(labels) == 0 {
					t.Error("List returned empty")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentCompactRead(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 10; i++ {
		db.Set(string(rune('a'+i)), "content")
	}

	var wg sync.WaitGroup

	// Compact in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		db.Compact()
	}()

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			label := string(rune('a' + n))
			for j := 0; j < 20; j++ {
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
