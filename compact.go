// Public entry points for the two common Repair modes.
package folio

// Compact merges the sparse region back into sorted order, restoring
// binary search performance. All history is preserved.
func (db *DB) Compact() error {
	return db.Repair(nil)
}

// Purge does the same as Compact but also drops history records,
// permanently removing all previous versions of every document.
func (db *DB) Purge() error {
	return db.Repair(&CompactOptions{PurgeHistory: true})
}
