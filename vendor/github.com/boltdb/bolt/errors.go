package bolt

import "errors"

var (
	ErrDatabaseNotOpen = errors.New("database not open")

	ErrDatabaseOpen = errors.New("database already open")

	ErrInvalid = errors.New("invalid database")

	ErrVersionMismatch = errors.New("version mismatch")

	ErrChecksum = errors.New("checksum error")

	ErrTimeout = errors.New("timeout")
)

var (
	ErrTxNotWritable = errors.New("tx not writable")

	ErrTxClosed = errors.New("tx closed")

	ErrDatabaseReadOnly = errors.New("database is in read-only mode")
)

var (
	ErrBucketNotFound = errors.New("bucket not found")

	ErrBucketExists = errors.New("bucket already exists")

	ErrBucketNameRequired = errors.New("bucket name required")

	ErrKeyRequired = errors.New("key required")

	ErrKeyTooLarge = errors.New("key too large")

	ErrValueTooLarge = errors.New("value too large")

	ErrIncompatibleValue = errors.New("incompatible value")
)
