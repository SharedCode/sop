package sop

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestShouldRetry_NonRetryableSentinels(t *testing.T) {
	if ShouldRetry(nil) {
		t.Fatalf("nil should not retry")
	}
	if ShouldRetry(context.Canceled) {
		t.Fatalf("context.Canceled should not retry")
	}
	if ShouldRetry(context.DeadlineExceeded) {
		t.Fatalf("context.DeadlineExceeded should not retry")
	}
}

func TestShouldRetry_NonRetryableSyscallErrno(t *testing.T) {
	cases := []error{
		&os.PathError{Op: "write", Path: "/tmp/x", Err: syscall.EROFS},
		&os.PathError{Op: "write", Path: "/tmp/x", Err: syscall.ENOSPC},
		&os.PathError{Op: "open", Path: "/tmp/x", Err: syscall.EMFILE},
		&os.PathError{Op: "open", Path: "/tmp/x", Err: syscall.EACCES},
		&os.PathError{Op: "link", Path: "/tmp/x", Err: syscall.EXDEV},
	}
	for i, e := range cases {
		if ShouldRetry(e) {
			t.Fatalf("case %d expected non-retryable: %v", i, e)
		}
	}
}

func TestShouldRetry_RetryableTransient(t *testing.T) {
	// EBUSY and EAGAIN are typically transient
	cases := []error{
		&os.PathError{Op: "rename", Path: "/tmp/x", Err: syscall.EBUSY},
		&os.SyscallError{Syscall: "read", Err: syscall.EAGAIN},
	}
	for i, e := range cases {
		if !ShouldRetry(e) {
			t.Fatalf("case %d expected retryable: %v", i, e)
		}
	}
}

func TestShouldRetry_StringFallback(t *testing.T) {
	// Unknown error text containing read-only file system should be non-retryable
	e := errors.New("device is mounted read-only file system")
	if !strings.Contains(e.Error(), "read-only file system") {
		t.Skip("environment normalization")
	}
	if ShouldRetry(e) {
		t.Fatalf("string EROFS heuristic should be non-retryable")
	}
}
