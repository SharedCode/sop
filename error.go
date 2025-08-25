package sop

import "fmt"

// ErrorCode enumerates SOP error categories used across packages.
type ErrorCode int

const (
	// Unknown represents an unspecified error condition.
	Unknown ErrorCode = iota
	// LockAcquisitionFailure indicates failure to acquire a required lock.
	LockAcquisitionFailure
	// FileIOError represents file I/O related errors, e.g. encountered by BlobStore (w/ & w/o EC).
	// This should not generate Failover event because BlobStore errors are either handled internally for no EC
	// or by EC replication feature.
	FileIOError
	// FailoverQualifiedError marks an error that qualifies the operation for failover handling.
	FailoverQualifiedError = 77 + iota
	// FileIOErrorFailoverQualified represents file I/O related errors.
	FileIOErrorFailoverQualified
	// RestoreRegistryFileSectorFailure indicates a failure while restoring a registry file sector.
	RestoreRegistryFileSectorFailure
)

// Error is a SOP-specific error carrying a code, the wrapped error and optional user data.
type Error struct {
	Code     ErrorCode
	Err      error
	UserData any
}

// Error implements the error interface by formatting the code, user data, and wrapped error details.
func (e Error) Error() string {
	return fmt.Errorf("error code: %d, user data: %v, details: %w", e.Code, e.UserData, e.Err).Error()
}
