package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleMetricsFormat(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handleMetrics(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, metric := range []string{
		"sop_build_info",
		"sop_uptime_seconds",
		"sop_http_requests_total",
		"sop_http_responses_total",
		"sop_http_requests_in_flight",
		"sop_goroutines",
		"sop_heap_alloc_bytes",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics output missing %s", metric)
		}
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("unexpected content type %q", ct)
	}
}

func TestMetricsMiddlewareCountsResponses(t *testing.T) {
	before2xx := metrics.responses2xx.Load()
	before5xx := metrics.responses5xx.Load()
	beforeTotal := metrics.requests.Load()

	okHandler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	failHandler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	okHandler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	failHandler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))

	if got := metrics.requests.Load() - beforeTotal; got != 2 {
		t.Errorf("expected 2 requests counted, got %d", got)
	}
	if got := metrics.responses2xx.Load() - before2xx; got != 1 {
		t.Errorf("expected 1 2xx counted, got %d", got)
	}
	if got := metrics.responses5xx.Load() - before5xx; got != 1 {
		t.Errorf("expected 1 5xx counted, got %d", got)
	}
	if inFlight := metrics.inFlight.Load(); inFlight != 0 {
		t.Errorf("expected 0 in-flight after completion, got %d", inFlight)
	}
}
