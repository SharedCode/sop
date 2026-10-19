package sop

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sethvargo/go-retry"
)

func fastRetry(t *testing.T) {
	t.Helper()
	old := RetryStartDuration
	RetryStartDuration = time.Millisecond
	t.Cleanup(func() { RetryStartDuration = old })
}

func TestRetry_SucceedsFirstTry(t *testing.T) {
	fastRetry(t)
	var calls int32
	gaveUp := false
	err := Retry(context.Background(),
		func(context.Context) error { atomic.AddInt32(&calls, 1); return nil },
		func(context.Context) { gaveUp = true })
	if err != nil || calls != 1 || gaveUp {
		t.Fatalf("err=%v calls=%d gaveUp=%v", err, calls, gaveUp)
	}
}

func TestRetry_RecoversAfterTransientFailures(t *testing.T) {
	fastRetry(t)
	var calls int32
	err := Retry(context.Background(), func(context.Context) error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return retryableForTest(errors.New("flaky"))
		}
		return nil
	}, nil)
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil and 3", err, calls)
	}
}

func TestRetry_GivesUpAfterSixAttempts(t *testing.T) {
	fastRetry(t)
	var calls int32
	gaveUp := false
	boom := errors.New("always fails")
	err := Retry(context.Background(),
		func(context.Context) error { atomic.AddInt32(&calls, 1); return retryableForTest(boom) },
		func(context.Context) { gaveUp = true })
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if calls != 6 {
		t.Fatalf("calls = %d, want 1 initial + 5 retries", calls)
	}
	if !gaveUp {
		t.Fatal("gaveUpTask must run when retries are exhausted")
	}
}

func TestRetry_NilGaveUpTaskIsSafe(t *testing.T) {
	fastRetry(t)
	err := Retry(context.Background(), func(context.Context) error { return retryableForTest(errors.New("x")) }, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetry_StopsOnContextCancel(t *testing.T) {
	fastRetry(t)
	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	err := Retry(ctx, func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		cancel()
		return retryableForTest(errors.New("x"))
	}, nil)
	if err == nil || calls > 2 {
		t.Fatalf("err=%v calls=%d, retry loop must stop promptly after cancel", err, calls)
	}
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"wrapped deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), false},
		{"not exist", os.ErrNotExist, false},
		{"permission", os.ErrPermission, false},
		{"closed", os.ErrClosed, false},
		{"exist", os.ErrExist, false},
		{"path error ENOENT", &fs.PathError{Op: "open", Path: "x", Err: syscall.ENOENT}, false},
		{"ENOSPC", syscall.ENOSPC, false},
		{"EROFS", syscall.EROFS, false},
		{"EACCES", syscall.EACCES, false},
		{"ECONNREFUSED", syscall.ECONNREFUSED, false},
		{"ETIMEDOUT", syscall.ETIMEDOUT, false},
		{"dns", &net.DNSError{Err: "no such host", Name: "h"}, false},
		{"read-only text", errors.New("write failed: read-only file system"), false},
		{"refused text", errors.New("dial tcp: connection refused"), false},
		{"unreachable text", errors.New("network is unreachable"), false},
		{"timeout text", errors.New("read: i/o timeout"), false},
		{"wrapped errno", fmt.Errorf("blob: %w", syscall.ENOSPC), false},
		{"generic", errors.New("something transient"), true},
		{"EAGAIN", syscall.EAGAIN, true},
		{"EINTR", syscall.EINTR, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldRetry(tt.err); got != tt.want {
				t.Fatalf("ShouldRetry(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// retryableForTest marks err as transient, the way production callers must for
// Retry to try again; a plain error ends the loop at once.
func retryableForTest(err error) error { return retry.RetryableError(err) }

func TestRetry_PlainErrorIsNotRetried(t *testing.T) {
	fastRetry(t)
	var calls int32
	err := Retry(context.Background(), func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("permanent")
	}, nil)
	if err == nil || calls != 1 {
		t.Fatalf("err=%v calls=%d, want an error after exactly one call", err, calls)
	}
}
