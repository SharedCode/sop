package sop

import "fmt"

type ErrorCode int

const (
	Unknown = iota
	LockAcquisitionFailure
	FailoverQualifiedError = 77 + iota
	FileIOError
	RestoreRegistryFileSectorFailure
)

// SOP custom error.
type Error struct {
	Code     ErrorCode
	Err      error
	UserData any
}

func (e Error) Error() string {
	return fmt.Errorf("error code: %d, user data: %v, details: %w", e.Code, e.UserData, e.Err).Error()
}
