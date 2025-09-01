package sop

import (
	"fmt"
	"time"
)

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

// ErrTimeout is returned when an operation exceeds its allowed time budget.
//
// Semantics:
// - If a context cancellation or deadline triggered the timeout, Cause carries the
//   original context error (context.Canceled or context.DeadlineExceeded). Unwrap()
//   returns that Cause so errors.Is(err, context.DeadlineExceeded) works.
// - If the operation-specific maximum duration triggered the timeout, Cause may be nil;
//   MaxTime contains the configured bound for the operation.
//
// This enables callers to branch on timeouts consistently while preserving the
// original context semantics when applicable.
type ErrTimeout struct {
	// Name is a short label for the operation (e.g., "transaction", "lockFileBlockRegion").
	Name string
	// MaxTime is the maximum duration allowed for the operation when applicable.
	MaxTime time.Duration
	// Cause is the underlying timeout/cancellation cause, typically a context error.
	Cause error
}

func (e ErrTimeout) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s timed out (maxTime=%v): %v", e.Name, e.MaxTime, e.Cause)
	}
	return fmt.Sprintf("%s timed out (maxTime=%v)", e.Name, e.MaxTime)
}

// Unwrap exposes the underlying cause (e.g., context.DeadlineExceeded) for errors.Is/As.
func (e ErrTimeout) Unwrap() error { return e.Cause }
