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

func TestProbeTrafficIsKeptOutOfTheRequestMetrics(t *testing.T) {
	h := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	beforeReq, beforeDur, beforeProbe := metrics.requests.Load(), metrics.durationCount.Load(), metrics.probeRequests.Load()

	for _, path := range []string{"/api/health", "/api/ready", "/metrics"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	if got := metrics.probeRequests.Load() - beforeProbe; got != 3 {
		t.Errorf("3 probes should be counted as probes, got %d", got)
	}
	if metrics.requests.Load() != beforeReq || metrics.durationCount.Load() != beforeDur {
		t.Error("probes must not change the request total or the latency histogram")
	}

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/billing/plan", nil))
	if metrics.requests.Load() != beforeReq+1 || metrics.durationCount.Load() != beforeDur+1 {
		t.Error("a real request must still be counted")
	}

	rec := httptest.NewRecorder()
	handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), "sop_http_probe_requests_total ") {
		t.Error("the probe counter must be exposed")
	}
}

func TestMetricsTokenIsOptionalAndEnforcedWhenSet(t *testing.T) {
	get := func(auth string) int {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		handleMetrics(rec, req)
		return rec.Code
	}

	t.Setenv("JOLTRIN_METRICS_TOKEN", "")
	if got := get(""); got != 200 {
		t.Errorf("with no token configured /metrics stays open, got %d", got)
	}

	t.Setenv("JOLTRIN_METRICS_TOKEN", "scrape-secret")
	if got := get(""); got != 401 {
		t.Errorf("no token: got %d, want 401", got)
	}
	if got := get("Bearer wrong"); got != 401 {
		t.Errorf("wrong token: got %d, want 401", got)
	}
	if got := get("Bearer scrape-secret"); got != 200 {
		t.Errorf("right token: got %d, want 200", got)
	}
}
