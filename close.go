// Database teardown.
package folio

import "errors"

// Close flushes state, clears the dirty flag if set, and releases all
// file handles. Any blocked operations wake up and receive ErrClosed.
func (db *DB) Close() error {
	db.cond.L.Lock()
	db.state.Store(StateClosed)
	db.cond.Broadcast()
	db.cond.L.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	// Drain in-flight flock calls before closing the fd.
	if db.lock != nil {
		db.lock.SetFile(nil)
	}

	var errs []error

	if db.header.Error == 1 {
		db.header.Error = 0
		db.header.State[stCount] = db.count.Load()
		hdrBytes, err := db.header.encode()
		if err != nil {
			errs = append(errs, err)
		} else if _, err := db.writer.WriteAt(hdrBytes, 0); err != nil {
			errs = append(errs, err)
		}
		if err := db.writer.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := munmapFile(db.mapped); err != nil {
		errs = append(errs, err)
	}
	db.mapped = nil
	if err := db.reader.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.writer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.root.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
