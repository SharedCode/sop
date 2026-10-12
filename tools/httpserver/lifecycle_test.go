package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func withDatabasePath(t *testing.T, path string) {
	t.Helper()
	old := config.DatabasePath
	config.DatabasePath = path
	t.Cleanup(func() { config.DatabasePath = old })
}

func readyStatus(t *testing.T) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	handleReady(rec, httptest.NewRequest(http.MethodGet, "/api/ready", nil))
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return rec.Code, body["status"]
}

func TestReadyReflectsTheDataDirectoryAndShutdown(t *testing.T) {
	t.Cleanup(func() { shuttingDown.Store(false) })

	withDatabasePath(t, t.TempDir())
	if code, status := readyStatus(t); code != 200 || status != "ready" {
		t.Errorf("with a data directory: %d %q", code, status)
	}

	withDatabasePath(t, filepath.Join(t.TempDir(), "missing"))
	if code, status := readyStatus(t); code != 503 || status != "data_unavailable" {
		t.Errorf("with the mount missing: %d %q", code, status)
	}

	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o600)
	withDatabasePath(t, file)
	if code, _ := readyStatus(t); code != 503 {
		t.Errorf("a file instead of a directory must not be ready, got %d", code)
	}

	withDatabasePath(t, "")
	if code, _ := readyStatus(t); code != 200 {
		t.Errorf("with no data path configured there is nothing to check, got %d", code)
	}

	shuttingDown.Store(true)
	if code, status := readyStatus(t); code != 503 || status != "shutting_down" {
		t.Errorf("while shutting down: %d %q", code, status)
	}
}

func TestHealthStaysLivenessOnly(t *testing.T) {
	withDatabasePath(t, filepath.Join(t.TempDir(), "missing"))
	rec := httptest.NewRecorder()
	handleHealth(rec, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if rec.Code != 200 {
		t.Errorf("a missing data mount must not fail liveness, or the platform would restart-loop: %d", rec.Code)
	}
}

func TestServeUntilDoneFinishesRequestsInFlight(t *testing.T) {
	t.Cleanup(func() { shuttingDown.Store(false) })
	withDatabasePath(t, t.TempDir())

	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ready", handleReady)
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(400 * time.Millisecond)
		w.Write([]byte("finished"))
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveUntilDone(ctx, &http.Server{Handler: mux}, ln, 300*time.Millisecond, 5*time.Second)
	}()

	type result struct {
		body string
		err  error
	}
	slow := make(chan result, 1)
	go func() {
		resp, err := http.Get(base + "/slow")
		if err != nil {
			slow <- result{err: err}
			return
		}
		defer resp.Body.Close()
		var sb strings.Builder
		buf := make([]byte, 64)
		for {
			n, rerr := resp.Body.Read(buf)
			sb.Write(buf[:n])
			if rerr != nil {
				break
			}
		}
		slow <- result{body: sb.String()}
	}()

	<-started
	cancel()

	// During the drain the server still answers, and says it is not ready.
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Get(base + "/api/ready")
	if err != nil {
		t.Fatalf("the listener should stay open during the drain: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("readiness during the drain: got %d, want 503", resp.StatusCode)
	}

	if r := <-slow; r.err != nil || r.body != "finished" {
		t.Errorf("the request in flight must complete: body %q err %v", r.body, r.err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("clean shutdown returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveUntilDone did not return")
	}
	if _, err := http.Get(base + "/api/ready"); err == nil {
		t.Error("the port should be closed after shutdown")
	}
}

func TestServeUntilDoneReturnsAServeFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	err = serveUntilDone(context.Background(), &http.Server{Handler: http.NewServeMux()}, ln, 0, time.Second)
	if err == nil {
		t.Error("a listener that fails must be reported, so the process exits non-zero")
	}
}

func TestSecurityHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	securityHeaders(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("Referrer-Policy") == "" {
		t.Errorf("headers missing: %v", rec.Header())
	}
}
