package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig(t *testing.T) *config {
	t.Helper()
	executables, err := resolveExecutables("echo," + os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	return &config{
		allowedOrigins:     parseOrigins("http://localhost:8080,http://127.0.0.1:8080"),
		commandTimeout:     10 * time.Second,
		allowedExecutables: executables,
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
	cfg := testConfig(t)
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
	cfg := testConfig(t)
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
	cfg := testConfig(t)
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
	cfg := testConfig(t)
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
	cfg := testConfig(t)
	rec := post(t, cfg, "http://localhost:8080", "", strings.Repeat("A", maxRequestBody+1024))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an oversized body, got %d", rec.Code)
	}
}

// The daemon re-runs this test binary as the command, so these tests need no
// shell and behave the same on every platform. HELPER selects what it does.
func TestMain(m *testing.M) {
	switch os.Getenv("SOP_DAEMON_TEST_HELPER") {
	case "flood":
		// Write far more than any response should carry.
		chunk := bytes.Repeat([]byte("x"), 64<<10)
		for i := 0; i < 160; i++ { // about 10 MiB
			os.Stdout.Write(chunk)
			os.Stderr.Write(chunk[:1024])
		}
		os.Exit(0)
	case "env":
		fmt.Print(os.Getenv("SOP_DAEMON_TOKEN"), "|", os.Getenv("KEEP_ME"))
		os.Exit(0)
	case "argv0":
		fmt.Print(os.Args[0])
		os.Exit(0)
	case "orphan":
		// Start a child that holds the output pipe open and outlives this process,
		// then wait to be killed by the timeout.
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), "SOP_DAEMON_TEST_HELPER=sleep")
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		_ = cmd.Start()
		time.Sleep(time.Minute)
		os.Exit(0)
	case "sleep":
		time.Sleep(20 * time.Second)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func postExec(t *testing.T, cfg *config, env map[string]string) ExecuteResponse {
	t.Helper()
	body, _ := json.Marshal(ExecuteRequest{Executable: os.Args[0]})
	req := httptest.NewRequest(http.MethodPost, "/api/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	for k, v := range env {
		t.Setenv(k, v)
	}
	cfg.guard(executeHandler(cfg))(rec, req)
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body.String())
	}
	return resp
}

// A command that prints without end must not be able to fill the daemon's
// memory or produce a response of the same size.
func Test_OutputIsCapped(t *testing.T) {
	resp := postExec(t, testConfig(t), map[string]string{"SOP_DAEMON_TEST_HELPER": "flood"})
	if len(resp.Stdout) > maxCapturedOutput || len(resp.Stderr) > maxCapturedOutput {
		t.Fatalf("captured %d bytes of stdout and %d of stderr, cap is %d", len(resp.Stdout), len(resp.Stderr), maxCapturedOutput)
	}
	if !resp.StdoutTruncated {
		t.Error("the response should say stdout was cut off")
	}
	if resp.ExitCode != 0 {
		t.Errorf("a command that finished normally should still report exit 0, got %d (%s)", resp.ExitCode, resp.Error)
	}
	if !strings.HasPrefix(resp.Stdout, "xxxx") {
		t.Error("the start of the output should be kept")
	}
}

// Output under the cap is returned whole and is not marked truncated.
func Test_ShortOutputIsNotTruncated(t *testing.T) {
	rec := post(t, testConfig(t), "", "", "echo small")
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StdoutTruncated || resp.StderrTruncated || !strings.Contains(resp.Stdout, "small") {
		t.Errorf("unexpected response: %+v", resp)
	}
}

// The shared secret that protects the daemon must not be handed to the
// commands it runs.
func Test_TokenIsNotPassedToCommands(t *testing.T) {
	cfg := testConfig(t)
	resp := postExec(t, cfg, map[string]string{
		"SOP_DAEMON_TEST_HELPER": "env",
		"SOP_DAEMON_TOKEN":       "do-not-leak",
		"KEEP_ME":                "kept",
	})
	if strings.Contains(resp.Stdout, "do-not-leak") {
		t.Fatalf("the token reached the command: %q", resp.Stdout)
	}
	if !strings.Contains(resp.Stdout, "|kept") {
		t.Errorf("other variables should still pass through: %q", resp.Stdout)
	}
}

// A command that leaves a child holding its output open used to keep the
// request waiting long after the timeout killed the command.
func Test_TimeoutReturnsEvenIfAChildHoldsTheOutputOpen(t *testing.T) {
	cfg := testConfig(t)
	cfg.commandTimeout = 300 * time.Millisecond
	start := time.Now()
	resp := postExec(t, cfg, map[string]string{"SOP_DAEMON_TEST_HELPER": "orphan"})
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("the request took %s, the timeout was %s", elapsed, cfg.commandTimeout)
	}
	if !strings.Contains(resp.Error, "exceeded") {
		t.Errorf("want a timeout error, got %+v", resp)
	}
}

func Test_AllowlistBlocksOtherExecutables(t *testing.T) {
	cfg := testConfig(t)

	if rec := post(t, cfg, "", "", "echo hi"); rec.Code != http.StatusOK {
		t.Fatalf("allowed program should run, got %d: %s", rec.Code, rec.Body.String())
	}
	rec := post(t, cfg, "", "", "/bin/echo hi")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("path form of an allowed name must be refused, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := post(t, cfg, "", "", "ls"); rec.Code != http.StatusForbidden {
		t.Fatalf("program outside the list must be refused, got %d", rec.Code)
	}
}

func Test_MissingExecutableAllowlistFailsClosed(t *testing.T) {
	for _, body := range []string{`{"command":"echo must-not-run"}`, `{"executable":"echo","args":["must-not-run"]}`} {
		for _, configured := range []map[string]configuredExecutable{nil, {}} {
			cfg := testConfig(t)
			cfg.allowedExecutables = configured
			req := httptest.NewRequest(http.MethodPost, "/api/execute", strings.NewReader(body))
			rec := httptest.NewRecorder()
			cfg.guard(executeHandler(cfg))(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Errorf("missing allowlist must deny %s, got %d: %s", body, rec.Code, rec.Body.String())
			}
		}
	}
}

func Test_ConfiguredProgramIgnoresLaterPATHChanges(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("PATH", t.TempDir())
	rec := post(t, cfg, "", "", "echo pinned-program")
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" || !strings.Contains(resp.Stdout, "pinned-program") {
		t.Fatalf("configured executable must remain available after PATH changes: %+v", resp)
	}
}

func Test_ResolveExecutablesRejectsMissingOrUnavailablePrograms(t *testing.T) {
	for _, input := range []string{"", " , ", filepath.Join(t.TempDir(), "missing-program"), "echo," + filepath.Join(t.TempDir(), "missing-program")} {
		if programs, err := resolveExecutables(input); err == nil || len(programs) != 0 {
			t.Errorf("invalid configuration %q must fail entirely: programs=%v err=%v", input, programs, err)
		}
	}
}

func Test_ExecutableRequestsUseTheConfiguredProgram(t *testing.T) {
	cfg := testConfig(t)
	for _, executable := range []string{"echo", cfg.allowedExecutables["echo"].path} {
		body, err := json.Marshal(ExecuteRequest{Executable: executable, Args: []string{"literal; $(no-shell)"}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/execute", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		cfg.guard(executeHandler(cfg))(rec, req)
		if executable != "echo" {
			if rec.Code != http.StatusForbidden {
				t.Fatalf("unconfigured absolute alias must be denied, got %d: %s", rec.Code, rec.Body.String())
			}
			continue
		}
		var resp ExecuteResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if rec.Code != http.StatusOK || resp.Error != "" || strings.TrimSpace(resp.Stdout) != "literal; $(no-shell)" {
			t.Fatalf("configured program must receive literal arguments, got %d: %+v", rec.Code, resp)
		}
	}
}

func Test_ConfiguredSymlinkIsResolvedBeforeRequests(t *testing.T) {
	echo, err := exec.LookPath("echo")
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "configured-echo"+filepath.Ext(echo))
	if err := os.Symlink(echo, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	cfg := testConfig(t)
	cfg.allowedExecutables, err = resolveExecutables(alias)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(os.Args[0], alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SOP_DAEMON_TEST_HELPER", "env")
	rec := post(t, cfg, "", "", fmt.Sprintf("%q pinned-symlink", alias))
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || resp.Error != "" || strings.TrimSpace(resp.Stdout) != "pinned-symlink" {
		t.Fatalf("replacing the configured symlink must not select another executable: %+v", resp)
	}
}

func Test_ConfiguredSymlinkPreservesArgv0AndPinnedPath(t *testing.T) {
	alias := filepath.Join(t.TempDir(), "configured-helper"+filepath.Ext(os.Args[0]))
	if err := os.Symlink(os.Args[0], alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	cfg := testConfig(t)
	var err error
	cfg.allowedExecutables, err = resolveExecutables(alias)
	if err != nil {
		t.Fatal(err)
	}
	// Repoint the configured name at an invalid program after startup. The
	// original helper must still run with its configured invocation name.
	replacement := filepath.Join(t.TempDir(), "replacement")
	if err := os.WriteFile(replacement, []byte("invalid executable"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(replacement, alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SOP_DAEMON_TEST_HELPER", "argv0")
	rec := post(t, cfg, "", "", fmt.Sprintf("%q", alias))
	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || resp.Error != "" || resp.Stdout != alias {
		t.Fatalf("configured invocation name and executable must both be preserved: %+v", resp)
	}
}
