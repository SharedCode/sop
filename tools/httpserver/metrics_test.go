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

func TestMetricsExposeRequestDurationHistogram(t *testing.T) {
	h := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fast", nil))

	rec := httptest.NewRecorder()
	handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE sop_http_request_duration_seconds histogram",
		`sop_http_request_duration_seconds_bucket{le="10"}`,
		`sop_http_request_duration_seconds_bucket{le="+Inf"}`,
		"sop_http_request_duration_seconds_sum ",
		"sop_http_request_duration_seconds_count ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if metrics.durationCount.Load() < 1 || metrics.durationBuckets[len(requestBuckets)-1].Load() < 1 {
		t.Error("a fast request should be counted in the top bucket")
	}
}
