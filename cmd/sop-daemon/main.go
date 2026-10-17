// Command sop-daemon runs a loopback-only helper that executes configured
// commands on behalf of the SOP web UI (tools/httpserver serves a page that
// posts here after a human approves a proposed command).
//
// Executable names are explicitly configured and resolved at startup. Requests
// can select those entries and supply arguments, but cannot introduce another
// executable or change its path. An allowed interpreter can still execute code
// supplied through its arguments, so operators must trust callers to use each
// configured program's full capabilities. See docs/DAEMON.md.
//
// Cross-origin browser requests are gated by validating the Origin allowlist
// and never answering with a wildcard CORS header; a browser attaches Origin
// to a cross-origin POST, so a page on evil.example cannot reach this
// endpoint even though it is reachable at 127.0.0.1. Requests with no
// Origin at all (curl, tests, other local tooling) still require a configured
// executable and can additionally be gated with -token.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

const (
	maxRequestBody = 64 << 10 // 64 KiB: a command line, not a file upload.
	// maxCapturedOutput caps what is kept of each of a command's stdout and
	// stderr. Without it a command that prints without end fills memory until
	// the timeout, and the response is as large as what it printed.
	maxCapturedOutput = 1 << 20
	// killWait is how long the daemon waits, after a command is killed, for
	// its output pipes to close. A child the command started can hold them open
	// and would otherwise keep the request waiting indefinitely.
	killWait    = 2 * time.Second
	tokenHeader = "X-SOP-Daemon-Token" //nolint:gosec // header name, not a credential
)

type ExecuteRequest struct {
	// Command is the legacy field: a single string split into argv by
	// splitCommand and exec'd directly, with no shell involved. Pipes,
	// &&/||, redirects, globs, and env expansion are no longer
	// interpreted; they land as literal argument text. Callers that need
	// those should prefer Executable+Args, or pipe/chain client-side.
	Command    string   `json:"command"`
	Executable string   `json:"executable"`
	Args       []string `json:"args"`
}

type ExecuteResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
	// StdoutTruncated and StderrTruncated are set when the output was longer
	// than maxCapturedOutput and only the start of it is returned.
	StdoutTruncated bool `json:"stdout_truncated,omitempty"`
	StderrTruncated bool `json:"stderr_truncated,omitempty"`
}

// cappedBuffer keeps the first limit bytes written to it and counts the rest as
// dropped. Write always reports success, so the command is never slowed or
// failed by a full buffer, it just stops being recorded.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.limit - c.buf.Len(); room > 0 {
		if len(p) <= room {
			c.buf.Write(p)
			return len(p), nil
		}
		c.buf.Write(p[:room])
	}
	c.truncated = true
	return len(p), nil
}

// childEnv is the daemon's environment without its own shared secret, so a
// command it runs cannot read the token that protects the daemon.
func childEnv() []string {
	env := os.Environ()
	out := env[:0:0]
	for _, kv := range env {
		if strings.HasPrefix(kv, "SOP_DAEMON_TOKEN=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// config holds the resolved security settings for one daemon run.
type config struct {
	allowedOrigins map[string]bool
	token          string
	commandTimeout time.Duration
	// allowedExecutables maps exact configured request names to executable paths
	// and invocation names resolved at startup. An empty map permits no execution.
	// Requests may select entries but never supply their paths or invocation names.
	allowedExecutables map[string]configuredExecutable
}

type configuredExecutable struct {
	path  string
	argv0 string
}

func main() {
	port := flag.Int("port", 9090, "Port to run the local SOP daemon on")
	origins := flag.String("allowed-origins", "http://localhost:8080,http://127.0.0.1:8080",
		"Comma-separated origins allowed to call this daemon from a browser. A request carrying any other Origin is rejected.")
	token := flag.String("token", os.Getenv("SOP_DAEMON_TOKEN"),
		"Optional shared secret; when set, callers must send it in the "+tokenHeader+" header. Defaults to $SOP_DAEMON_TOKEN.")
	allow := flag.String("allow-executables", os.Getenv("SOP_DAEMON_ALLOW_EXECUTABLES"),
		"Required comma-separated list of programs the daemon may run, matched exactly and resolved at startup (for example kubectl,terraform). Defaults to $SOP_DAEMON_ALLOW_EXECUTABLES.")
	timeout := flag.Duration("command-timeout", 60*time.Second, "Maximum wall-clock time a single command may run before it is killed.")
	flag.Parse()

	executables, err := resolveExecutables(*allow)
	if err != nil {
		log.Fatalf("Invalid executable allowlist: %v", err)
	}

	cfg := &config{
		allowedOrigins:     parseOrigins(*origins),
		token:              *token,
		commandTimeout:     *timeout,
		allowedExecutables: executables,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/execute", cfg.guard(executeHandler(cfg)))

	// Loopback only: this must never be reachable from the network.
	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("Starting SOP Desktop Daemon on %s", addr)
	log.Printf("Configured executables: %v. Allowed browser origins: %s", cfg.allowedExecutables, strings.Join(sortedKeys(cfg.allowedOrigins), ", "))
	if cfg.token == "" {
		log.Printf("No -token set: any local process can post commands here (it could also just run them directly). Set -token to require a shared secret.")
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// resolveExecutables accepts operator configuration, never request data. Resolve
// PATH entries and symlinks now so later requests cannot select another binary
// by changing PATH or the configured symlink. The operator must still protect
// the resolved executable and its directories from replacement.
func resolveExecutables(csv string) (map[string]configuredExecutable, error) {
	out := make(map[string]configuredExecutable)
	for _, name := range strings.Split(csv, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		path, err := exec.LookPath(name)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve %q: %w", name, err)
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve absolute path for %q: %w", name, err)
		}
		path, err = filepath.EvalSymlinks(path)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve symlinks for %q: %w", name, err)
		}
		out[name] = configuredExecutable{path: path, argv0: name}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("set -allow-executables or SOP_DAEMON_ALLOW_EXECUTABLES to the programs callers may run")
	}
	return out, nil
}

func parseOrigins(csv string) map[string]bool {
	out := make(map[string]bool)
	for _, o := range strings.Split(csv, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out[o] = true
		}
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// guard applies the Origin allowlist, the optional token check, and the
// CORS response headers. It deliberately never emits
// Access-Control-Allow-Origin: *, since that wildcard is what would let any page
// on the internet drive this endpoint.
func (c *config) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		// Vary matters for any cache sitting in front of this: the response
		// differs per Origin.
		w.Header().Set("Vary", "Origin")

		if origin != "" {
			if !c.allowedOrigins[origin] {
				log.Printf("rejected request from disallowed origin %q", origin)
				http.Error(w, "Forbidden: origin not allowed", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, "+tokenHeader)
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		if c.token != "" {
			given := r.Header.Get(tokenHeader)
			if subtle.ConstantTimeCompare([]byte(given), []byte(c.token)) != 1 {
				http.Error(w, "Unauthorized: bad or missing "+tokenHeader, http.StatusUnauthorized)
				return
			}
		}

		next(w, r)
	}
}

func executeHandler(cfg *config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ExecuteRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody)).Decode(&req); err != nil {
			respondJSON(w, http.StatusBadRequest, ExecuteResponse{Error: "Invalid JSON body: " + err.Error()})
			return
		}

		if req.Executable == "" && req.Command == "" {
			respondJSON(w, http.StatusBadRequest, ExecuteResponse{Error: "Either 'executable' or 'command' must be provided"})
			return
		}

		var program string
		var args []string

		if req.Executable != "" {
			program = req.Executable
			args = req.Args
		} else {
			// Legacy path: split the command string into program + args
			// to avoid passing raw input through a shell interpreter.
			parts, err := splitCommand(req.Command)
			if err != nil || len(parts) == 0 {
				respondJSON(w, http.StatusBadRequest, ExecuteResponse{Error: "Failed to parse command"})
				return
			}
			program = parts[0]
			args = parts[1:]
		}

		trustedProgram, allowed := cfg.allowedExecutables[program]
		if !allowed || trustedProgram.path == "" || trustedProgram.argv0 == "" {
			respondJSON(w, http.StatusForbidden, ExecuteResponse{Error: "Executable not in the allowlist: " + sanitizeLog(program)})
			return
		}

		log.Printf("Executing configured program: %s %v", sanitizeLog(trustedProgram.path), sanitizeLogArgs(args))

		// Bound the run: a command that never exits would otherwise pin this
		// handler (and its goroutine) forever.
		ctx, cancel := context.WithTimeout(r.Context(), cfg.commandTimeout)
		defer cancel()

		cmd := exec.CommandContext(ctx, trustedProgram.path, args...)
		// Multicall executables such as BusyBox select an applet using argv[0].
		// Preserve the configured invocation while Cmd.Path stays pinned to the
		// resolved binary; neither value is taken from the request.
		cmd.Args[0] = trustedProgram.argv0
		cmd.Env = childEnv()
		cmd.WaitDelay = killWait

		stdout := &cappedBuffer{limit: maxCapturedOutput}
		stderr := &cappedBuffer{limit: maxCapturedOutput}
		cmd.Stdout = stdout
		cmd.Stderr = stderr

		err := cmd.Run()

		resp := ExecuteResponse{
			Stdout:          stdout.buf.String(),
			Stderr:          stderr.buf.String(),
			StdoutTruncated: stdout.truncated,
			StderrTruncated: stderr.truncated,
		}

		if err != nil {
			resp.Error = err.Error()
			if exitError, ok := err.(*exec.ExitError); ok {
				resp.ExitCode = exitError.ExitCode()
			} else {
				resp.ExitCode = -1
			}
			if ctx.Err() != nil {
				resp.Error = fmt.Sprintf("command exceeded %s and was killed", cfg.commandTimeout)
			}
			log.Printf("Command finished with error: %v", err)
		} else {
			resp.ExitCode = 0
			log.Printf("Command executed successfully")
		}

		respondJSON(w, http.StatusOK, resp)
	}
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

// sanitizeLog strips characters a caller could use to forge extra log lines
// (newlines, carriage returns) out of a value before it's logged.
func sanitizeLog(s string) string {
	r := strings.NewReplacer("\n", "\\n", "\r", "\\r")
	return r.Replace(s)
}

func sanitizeLogArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = sanitizeLog(a)
	}
	return out
}

// splitCommand splits a shell command string into words, respecting single
// quotes, double quotes, and backslash escapes. This lets callers pass a
// single "command" string without routing it through sh -c.
func splitCommand(s string) ([]string, error) {
	var args []string
	var current []rune
	var quote rune // 0 = unquoted, '\'' or '"' = inside that quote type
	escaped := false

	for _, r := range s {
		if escaped {
			current = append(current, r)
			escaped = false
			continue
		}
		if r == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				current = append(current, r)
			}
			continue
		}
		switch {
		case r == '\'' || r == '"':
			quote = r
		case unicode.IsSpace(r):
			if len(current) > 0 {
				args = append(args, string(current))
				current = current[:0]
			}
		default:
			current = append(current, r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unclosed quote")
	}
	if len(current) > 0 {
		args = append(args, string(current))
	}
	return args, nil
}
