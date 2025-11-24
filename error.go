package sop

import (
	"context"
	"errors"
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
//   - If a context cancellation or deadline triggered the timeout, Cause carries the
//     original context error (context.Canceled or context.DeadlineExceeded). Unwrap()
//     returns that Cause so errors.Is(err, context.DeadlineExceeded) works.
//   - If the operation-specific maximum duration triggered the timeout, Cause may be nil;
//     MaxTime contains the configured bound for the operation.
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

// HandleLockAcquisitionFailure checks if the error is a LockAcquisitionFailure and if so,
// attempts to rollback the blocking transaction using the provided rollbackFunc.
// If rollback succeeds, it takes over the lock and releases it using unlockFunc.
// Returns nil if the failure was handled (lock taken over and released), otherwise returns the original error.
func HandleLockAcquisitionFailure(ctx context.Context, err error,
	rollbackFunc func(context.Context, UUID) error,
	unlockFunc func(context.Context, []*LockKey) error) error {

	var se Error
	if errors.As(err, &se) && se.Code == LockAcquisitionFailure {
		if lk, ok := se.UserData.(*LockKey); ok && !lk.LockID.IsNil() {
			// Attempt to rollback the blocking transaction.
			if rerr := rollbackFunc(ctx, lk.LockID); rerr == nil {
				// Take over the lock and release it so we can acquire it in the retry.
				lk.IsLockOwner = true
				if uerr := unlockFunc(ctx, []*LockKey{lk}); uerr != nil {
					// If unlock fails, we can't really do much but return the original error
					// or maybe the unlock error. But returning original error keeps flow simple
					// as the retry loop will likely fail again.
					// However, since we successfully rolled back, we should probably return nil
					// and let the retry happen, but the lock is still there.
					// Let's return the unlock error if it happens, or nil if success.
					return uerr
				}
				// Return nil to indicate successful handling (caller should retry).
				return nil
			}
		}
	}
	return err
}
