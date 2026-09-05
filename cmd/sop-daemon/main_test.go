package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testConfig() *config {
	return &config{
		allowedOrigins: parseOrigins("http://localhost:8080,http://127.0.0.1:8080"),
		commandTimeout: 10 * time.Second,
	}
}

func post(t *testing.T, cfg *config, origin, token, command string) *httptest.ResponseRecorder {
	t.Helper()
	body := strings.NewReader(`{"command":` + jsonString(command) + `}`)
	req := httptest.NewRequest(http.MethodPost, "/api/execute", body)
	req.Header.Set("Content-Type", "application/json")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if token != "" {
		req.Header.Set(tokenHeader, token)
	}
	rec := httptest.NewRecorder()
	cfg.guard(executeHandler(cfg))(rec, req)
	return rec
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// Test_DriveByOriginIsRejected is the regression test for the critical
// finding: with Access-Control-Allow-Origin: * and no Origin check, any web
// page the user visited could POST here and run shell commands. A hostile
// origin must now be refused before the command ever reaches a shell.
func Test_DriveByOriginIsRejected(t *testing.T) {
	cfg := testConfig()
	rec := post(t, cfg, "https://evil.example", "", "echo pwned")

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for a disallowed origin, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "pwned") {
		t.Fatal("command executed for a disallowed origin")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("must not send CORS approval to a disallowed origin, got %q", got)
	}
}

// Test_NoWildcardCORS pins the specific header that made the drive-by
// possible: the response must echo one allowed origin, never "*".
func Test_NoWildcardCORS(t *testing.T) {
	cfg := testConfig()
	rec := post(t, cfg, "http://localhost:8080", "", "echo ok")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8080" {
		t.Fatalf("expected the allowed origin echoed back, got %q", got)
	}
	if rec.Header().Get("Vary") != "Origin" {
		t.Fatal("expected Vary: Origin so caches don't cross-serve the CORS decision")
	}
}

// Test_AllowedOriginStillWorks confirms the fix doesn't break the SOP UI,
// which posts here from the httpserver page on localhost:8080.
func Test_AllowedOriginStillWorks(t *testing.T) {
	cfg := testConfig()
	rec := post(t, cfg, "http://127.0.0.1:8080", "", "echo hello-from-daemon")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for an allowed origin, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(resp.Stdout, "hello-from-daemon") {
		t.Fatalf("expected the command to run, got stdout %q err %q", resp.Stdout, resp.Error)
	}
}

// Test_TokenEnforcedWhenSet covers the defense-in-depth path for callers
// that send no Origin at all (curl, other local tooling).
func Test_TokenEnforcedWhenSet(t *testing.T) {
	cfg := testConfig()
	cfg.token = "s3cret"

	if rec := post(t, cfg, "", "", "echo nope"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a token, got %d", rec.Code)
	}
	if rec := post(t, cfg, "", "wrong", "echo nope"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with a wrong token, got %d", rec.Code)
	}
	if rec := post(t, cfg, "", "s3cret", "echo yes"); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with the right token, got %d", rec.Code)
	}
}

// Test_OversizedBodyRejected confirms the MaxBytesReader cap: an unbounded
// JSON body was previously decoded straight into memory.
func Test_OversizedBodyRejected(t *testing.T) {
	cfg := testConfig()
	rec := post(t, cfg, "http://localhost:8080", "", strings.Repeat("A", maxRequestBody+1024))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an oversized body, got %d", rec.Code)
	}
}
