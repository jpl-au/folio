// Hash algorithm migration.
//
// Rehash changes the hash algorithm for all records in place.
// No temp file is needed since _id is fixed at 16 hex chars.
package folio

// Rehash changes the hash algorithm for all records.
func (db *DB) Rehash(newAlg int) error {
	db.state.Store(StateNone)
	defer func() {
		db.cond.L.Lock()
		db.state.Store(StateAll)
		db.cond.Broadcast()
		db.cond.L.Unlock()
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	info, _ := db.reader.Stat()
	entries := scanm(db.reader, HeaderSize, info.Size(), 0)

	cache := map[string]string{}

	for _, entry := range entries {
		lbl := entry.Label
		if lbl == "" {
			record, _ := line(db.reader, entry.SrcOff)
			lbl = label(record)
		}
		if cache[lbl] == "" {
			cache[lbl] = hash(lbl, newAlg)
		}
		db.writer.WriteAt([]byte(cache[lbl]), entry.SrcOff+16)
	}

	// Update header
	db.header.Algorithm = newAlg
	db.header.Timestamp = now()
	hdrBytes, _ := db.header.encode()
	db.writer.WriteAt(hdrBytes, 0)
	db.writer.Sync()

	return nil
}
