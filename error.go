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

type ErrorMetadata interface {
	GetCode() ErrorCode
	GetError() error
}

// SOP custom error.
type Error[T any] struct {
	ErrorMetadata
	Code     ErrorCode
	Err      error
	UserData T
}

func (e Error[T]) GetCode() ErrorCode {
	return e.Code
}
func (e Error[T]) GetError() error {
	return e.Err
}

func (e Error[T]) Error() string {
	return fmt.Errorf("Error %d: %w, user data: %v", e.Code, e.Err, e.UserData).Error()
}
