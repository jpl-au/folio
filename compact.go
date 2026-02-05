// Convenience wrappers for common repair operations.
package folio

// Compact reorganises the database preserving history.
func (db *DB) Compact() error {
	return db.Repair(nil)
}

// Purge reorganises the database removing all history.
func (db *DB) Purge() error {
	return db.Repair(&CompactOptions{PurgeHistory: true})
}
