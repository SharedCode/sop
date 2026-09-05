package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBodySizeLimitMiddleware_RejectsOversizedBody confirms that a request
// body larger than maxRequestBodySize is rejected with a 413. Before this
// middleware was added, every handler in tools/httpserver read r.Body with
// no cap at all, so a single POST could force the process to allocate
// arbitrary amounts of memory.
func TestBodySizeLimitMiddleware_RejectsOversizedBody(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, maxRequestBodySize+1)
		_, err := r.Body.Read(buf)
		if err == nil {
			t.Fatal("expected an error reading past the body size limit, got nil")
		}
		// http.MaxBytesReader returns a *http.MaxBytesError once the limit
		// is exceeded. Any non-nil error here means the cap is working.
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
	})

	handler := bodySizeLimitMiddleware(inner)

	// Build a request whose body is just over the limit.
	oversized := strings.Repeat("X", maxRequestBodySize+1024)
	req := httptest.NewRequest(http.MethodPost, "/api/store/add", strings.NewReader(oversized))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for an oversized body, got %d", rec.Code)
	}
}

// TestBodySizeLimitMiddleware_AllowsNormalBody confirms that a request body
// within the size limit passes through to the handler without interference.
func TestBodySizeLimitMiddleware_AllowsNormalBody(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 256)
		n, _ := r.Body.Read(buf)
		if n == 0 {
			t.Fatal("expected to read the small body, got 0 bytes")
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := bodySizeLimitMiddleware(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/store/add", strings.NewReader(`{"name":"test"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a normal-sized body, got %d", rec.Code)
	}
}
