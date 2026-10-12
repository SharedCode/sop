package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	log "log/slog"
)

// shuttingDown flips to true when the process is asked to stop. /api/ready
// then answers 503, so the platform stops sending new requests while the ones
// already running finish.
var shuttingDown atomic.Bool

const (
	// drainDelay gives the platform time to notice the failing readiness check
	// before the listener closes.
	drainDelay = 5 * time.Second
	// shutdownTimeout bounds how long in-flight requests get to finish. It is
	// under the 30 seconds Container Apps allows after SIGTERM.
	shutdownTimeout = 20 * time.Second
)

// checkDataPath reports whether the data directory is there and is a
// directory. On Container Apps that is the mounted file share, so a missing
// mount shows up as not ready instead of as errors on every request.
func checkDataPath(path string) error {
	if path == "" {
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("data path is not a directory")
	}
	return nil
}

// handleReady is the readiness check. /api/health stays a plain liveness
// answer, so a missing data mount takes the instance out of rotation without
// causing a restart loop.
func handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	status, body := http.StatusOK, "ready"
	switch {
	case shuttingDown.Load():
		status, body = http.StatusServiceUnavailable, "shutting_down"
	case checkDataPath(config.DatabasePath) != nil:
		status, body = http.StatusServiceUnavailable, "data_unavailable"
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"status": body})
}

// serveUntilDone serves on ln until ctx is cancelled, then drains: it marks
// the instance not ready, waits drain, and gives in-flight requests up to
// timeout to finish. It returns nil after a clean stop and the serve error
// otherwise.
func serveUntilDone(ctx context.Context, srv *http.Server, ln net.Listener, drain, timeout time.Duration) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	log.Info("shutdown requested, draining")
	shuttingDown.Store(true)
	select {
	case <-time.After(drain):
	case err := <-errc:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		srv.Close()
		return err
	}
	log.Info("shutdown complete")
	return nil
}

// securityHeaders sets headers that cost nothing and cannot break the API:
// browsers must not guess content types, and the referrer is trimmed to the
// origin on cross-origin requests.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}
