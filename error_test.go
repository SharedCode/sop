package sop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestError_ErrorString(t *testing.T) {
	e := Error{Code: FileIOError, Err: errors.New("disk gone"), UserData: "blob-7"}
	want := "error code: 2, user data: blob-7, details: disk gone"
	if got := e.Error(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestError_NilWrappedErrorHasNoFormatVerbNoise(t *testing.T) {
	got := Error{Code: Unknown}.Error()
	if strings.Contains(got, "%!") {
		t.Fatalf("message leaks a fmt error verb: %q", got)
	}
}

func TestError_UnwrapExposesCause(t *testing.T) {
	cause := errors.New("root cause")
	var err error = Error{Code: FileIOError, Err: cause}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is must reach the wrapped error")
	}
	wrapped := fmt.Errorf("commit: %w", Error{Code: LockAcquisitionFailure, Err: context.DeadlineExceeded})
	if !errors.Is(wrapped, context.DeadlineExceeded) {
		t.Fatal("errors.Is must reach a context error through sop.Error")
	}
	var se Error
	if !errors.As(wrapped, &se) || se.Code != LockAcquisitionFailure {
		t.Fatal("errors.As must still find the sop.Error")
	}
}

func TestErrorCodeValues(t *testing.T) {
	// These values cross package boundaries and are persisted in logs; pin them.
	pins := map[string]struct{ got, want ErrorCode }{
		"Unknown":                          {Unknown, 0},
		"LockAcquisitionFailure":           {LockAcquisitionFailure, 1},
		"FileIOError":                      {FileIOError, 2},
		"FailoverQualifiedError":           {FailoverQualifiedError, 80},
		"FileIOErrorFailoverQualified":     {FileIOErrorFailoverQualified, 81},
		"RestoreRegistryFileSectorFailure": {RestoreRegistryFileSectorFailure, 82},
	}
	for name, p := range pins {
		if p.got != p.want {
			t.Errorf("%s = %d, want %d", name, p.got, p.want)
		}
	}
}

func TestErrTimeout(t *testing.T) {
	plain := ErrTimeout{Name: "transaction", MaxTime: time.Second}
	if got, want := plain.Error(), "transaction timed out (maxTime=1s)"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if plain.Unwrap() != nil {
		t.Fatal("no cause means nothing to unwrap")
	}

	withCause := ErrTimeout{Name: "lock", MaxTime: time.Minute, Cause: context.DeadlineExceeded}
	if !strings.Contains(withCause.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("message must include the cause: %q", withCause.Error())
	}
	if !errors.Is(withCause, context.DeadlineExceeded) {
		t.Fatal("errors.Is must see the context cause")
	}
}

func TestTimedOut(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000)
	fixedNow(t, start.Add(10*time.Second))

	if err := TimedOut(context.Background(), "op", start, time.Minute); err != nil {
		t.Fatalf("within budget, got %v", err)
	}

	var te ErrTimeout
	err := TimedOut(context.Background(), "op", start, 10*time.Second)
	if !errors.As(err, &te) || te.Cause != nil || te.Name != "op" {
		t.Fatalf("elapsed >= max must yield a causeless ErrTimeout, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = TimedOut(ctx, "op", start, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx must surface context.Canceled, got %v", err)
	}
}

func TestHandleLockAcquisitionFailure(t *testing.T) {
	lockID := NewUUID()
	newErr := func() (*LockKey, error) {
		lk := &LockKey{LockID: lockID}
		return lk, Error{Code: LockAcquisitionFailure, UserData: lk}
	}
	noRollback := func(context.Context, UUID) error { t.Fatal("rollback must not run"); return nil }
	noUnlock := func(context.Context, []*LockKey) error { t.Fatal("unlock must not run"); return nil }

	t.Run("rollback and unlock succeed", func(t *testing.T) {
		lk, err := newErr()
		var rolled UUID
		var unlocked []*LockKey
		got := HandleLockAcquisitionFailure(context.Background(), err,
			func(_ context.Context, id UUID) error { rolled = id; return nil },
			func(_ context.Context, keys []*LockKey) error { unlocked = keys; return nil })
		if got != nil {
			t.Fatalf("got %v, want nil", got)
		}
		if rolled != lockID || len(unlocked) != 1 || unlocked[0] != lk || !lk.IsLockOwner {
			t.Fatalf("expected takeover of lock %v, rolled=%v unlocked=%v owner=%v", lockID, rolled, unlocked, lk.IsLockOwner)
		}
	})

	t.Run("unlock failure is returned", func(t *testing.T) {
		_, err := newErr()
		boom := errors.New("unlock failed")
		got := HandleLockAcquisitionFailure(context.Background(), err,
			func(context.Context, UUID) error { return nil },
			func(context.Context, []*LockKey) error { return boom })
		if !errors.Is(got, boom) {
			t.Fatalf("got %v, want %v", got, boom)
		}
	})

	t.Run("rollback failure returns original error", func(t *testing.T) {
		_, err := newErr()
		got := HandleLockAcquisitionFailure(context.Background(), err,
			func(context.Context, UUID) error { return errors.New("nope") }, noUnlock)
		if got == nil || got.Error() != err.Error() {
			t.Fatalf("got %v, want original %v", got, err)
		}
	})

	t.Run("other codes pass through", func(t *testing.T) {
		err := Error{Code: FileIOError, UserData: &LockKey{LockID: lockID}}
		if got := HandleLockAcquisitionFailure(context.Background(), err, noRollback, noUnlock); got == nil {
			t.Fatal("non lock-acquisition error must be returned unchanged")
		}
	})

	t.Run("nil lock id passes through", func(t *testing.T) {
		err := Error{Code: LockAcquisitionFailure, UserData: &LockKey{}}
		if got := HandleLockAcquisitionFailure(context.Background(), err, noRollback, noUnlock); got == nil {
			t.Fatal("nil lock id cannot be rolled back, error must be returned")
		}
	})

	t.Run("non sop error passes through", func(t *testing.T) {
		plain := errors.New("plain")
		if got := HandleLockAcquisitionFailure(context.Background(), plain, noRollback, noUnlock); got != plain {
			t.Fatalf("got %v", got)
		}
	})
}
