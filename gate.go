// Concurrency gating for the three-layer lock protocol.
//
// blockWrite and blockRead acquire all three concurrency layers (state
// check → OS flock → RWMutex) before allowing an operation to proceed.
// On return the caller holds db.mu (Lock or RLock) and db.lock; both
// must be released in the defer of the calling method.
package folio

import "github.com/jpl-au/folio/internal/flock"

func (db *DB) blockWrite() error {
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

	if err := db.lock.Acquire(flock.Exclusive); err != nil {
		return err
	}

	db.cond.L.Lock()
	for db.state.Load() != StateAll {
		if db.state.Load() == StateClosed {
			db.cond.L.Unlock()
			db.lock.Release()
			return ErrClosed
		}
		db.cond.Wait()
	}
	db.mu.Lock()
	db.cond.L.Unlock()
	return nil
}

func (db *DB) blockRead() error {
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

	if err := db.lock.Acquire(flock.Shared); err != nil {
		return err
	}

	db.cond.L.Lock()
	for db.state.Load() == StateNone || db.state.Load() == StateClosed {
		if db.state.Load() == StateClosed {
			db.cond.L.Unlock()
			db.lock.Release()
			return ErrClosed
		}
		db.cond.Wait()
	}
	db.mu.RLock()
	db.cond.L.Unlock()
	return nil
}
