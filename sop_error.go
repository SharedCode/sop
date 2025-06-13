package sop

import "fmt"

type ErrorCode int

const (
	Unknown = iota
	LockAcquisitionFailure
	RestoreRegistryFileSectorFailure
	FileIOError
)

// SOP custom error.
type Error[T any] struct {
	Code     ErrorCode
	Err      error
	UserData T
}

func (e Error[T]) Error() string {
	return fmt.Errorf("Error %d: %w, user data: %v", e.Code, e.Err, e.UserData).Error()
}
