package sop

import (
    "context"
    "errors"
    "syscall"
    "testing"
)

func Test_IsFailoverQualifiedIOError_Core(t *testing.T) {
    cases := []struct {
        name string
        in   error
        want bool
    }{
        {"nil", nil, false},
    {"context canceled", context.Canceled, false},
    {"deadline exceeded", context.DeadlineExceeded, false},
        {"EIO", syscall.EIO, true},
        {"ENODEV", syscall.ENODEV, true},
        {"ENXIO", syscall.ENXIO, true},
        {"EROFS", syscall.EROFS, true},
        {"ENOSPC", syscall.ENOSPC, true},
        {"EDQUOT", syscall.EDQUOT, true},
        {"wrapped EIO", errors.Join(syscall.EIO), true},
        {"EACCES not failover", syscall.EACCES, false},
        {"EPERM not failover", syscall.EPERM, false},
        {"EMFILE not failover", syscall.EMFILE, false},
        {"ENFILE not failover", syscall.ENFILE, false},
        {"os.ErrNotExist not failover", syscall.ENOENT, false},
    }
    for _, tt := range cases {
        if got := IsFailoverQualifiedIOError(tt.in); got != tt.want {
            t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
        }
    }
}

func Test_IsFailoverQualifiedIOError_LinuxSpecific(t *testing.T) {
    // Use raw errno values so the test compiles everywhere; will only match
    // when the platform error matches these numeric codes.
    const (
        EREMOTEIO  = syscall.Errno(121)
        EUCLEAN    = syscall.Errno(117)
        ENOMEDIUM  = syscall.Errno(123)
        EMEDIUMTYPE = syscall.Errno(124)
    )

    cases := []struct {
        name string
        in   error
        want bool
    }{
        {"EREMOTEIO", EREMOTEIO, true},
        {"EUCLEAN", EUCLEAN, true},
        {"ENOMEDIUM", ENOMEDIUM, true},
        {"EMEDIUMTYPE", EMEDIUMTYPE, true},
    }
    for _, tt := range cases {
        if got := IsFailoverQualifiedIOError(tt.in); got != tt.want {
            t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
        }
    }
}
