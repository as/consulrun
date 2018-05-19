// +build !windows,!plan9,!linux,!openbsd

package bolt

func fdatasync(db *DB) error {
	return db.file.Sync()
}
