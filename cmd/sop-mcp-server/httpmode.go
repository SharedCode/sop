package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"
)

// The http command serves the same barrier over MCP streamable HTTP, for a
// team that wants one server near its agents instead of a process per agent.
//
// It is single tenant. Everyone who holds the token shares one runbook set and
// one trace namespace, so two callers that use the same trace_id are working
// in the same trace. Give each team its own server and token.

const (
	defaultHTTPAddr = "127.0.0.1:8088"
	httpEndpoint    = "/mcp"
	maxRequestBody  = 1 << 20
	minRemoteToken  = 16
)

type httpConfig struct {
	addr  string
	token string
}

// parseHTTPArgs reads --addr and SOP_MCP_TOKEN. A server that listens past
// loopback must have a token of at least minRemoteToken characters, so it
// cannot be started open to the network by leaving one out.
func parseHTTPArgs(args []string, getenv func(string) string, errw io.Writer) (httpConfig, error) {
	fs := flag.NewFlagSet("http", flag.ContinueOnError)
	fs.SetOutput(errw)
	addr := fs.String("addr", defaultHTTPAddr, "address to listen on")
	if err := fs.Parse(args); err != nil {
		return httpConfig{}, err
	}
	cfg := httpConfig{addr: *addr, token: getenv("SOP_MCP_TOKEN")}
	host, _, err := net.SplitHostPort(cfg.addr)
	if err != nil {
		return httpConfig{}, fmt.Errorf("--addr %q: %w", cfg.addr, err)
	}
	if !isLoopbackHost(host) && len(cfg.token) < minRemoteToken {
		return httpConfig{}, fmt.Errorf("listening on %s needs SOP_MCP_TOKEN of at least %d characters, so the server is not open to the network", cfg.addr, minRemoteToken)
	}
	return cfg, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// requireBearer rejects any request without the right bearer token. With no
// token configured, which is only allowed on loopback, it lets requests through.
// The comparison is on SHA-256 digests so it takes the same time for any input.
func requireBearer(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := sha256.Sum256([]byte("Bearer " + token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(r.Header.Get("Authorization")))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// newHTTPHandler serves /healthz without a token, so a probe needs no secret,
// and the MCP endpoint behind the token, with request bodies capped.
func newHTTPHandler(mcp *server.MCPServer, token string) (http.Handler, *server.StreamableHTTPServer) {
	// Stateless: no session is held in memory between requests. The runbook
	// state that matters lives in the store.
	streamable := server.NewStreamableHTTPServer(mcp,
		server.WithEndpointPath(httpEndpoint),
		server.WithStateLess(true),
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "ok\n")
	})
	mux.Handle(httpEndpoint, requireBearer(token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
		streamable.ServeHTTP(w, r)
	})))
	return mux, streamable
}

// serveHTTP serves until ctx is cancelled, then stops accepting requests and
// gives the ones in flight a few seconds to finish.
func serveHTTP(ctx context.Context, ln net.Listener, mcp *server.MCPServer, token string, errw io.Writer) error {
	handler, streamable := newHTTPHandler(mcp, token)
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	fmt.Fprintf(errw, "sop-mcp-server: MCP over HTTP at http://%s%s\n", ln.Addr(), httpEndpoint)

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	streamable.Shutdown(shutdownCtx)
	return err
}

// runHTTP is what main calls for "sop-mcp-server http".
func runHTTP(mcp *server.MCPServer, args []string) int {
	cfg, err := parseHTTPArgs(args, os.Getenv, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sop-mcp-server:", err)
		return 2
	}
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sop-mcp-server:", err)
		return 1
	}
	if cfg.token == "" {
		fmt.Fprintln(os.Stderr, "sop-mcp-server: no SOP_MCP_TOKEN set, so any process on this machine can call the barrier")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serveHTTP(ctx, ln, mcp, cfg.token, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "sop-mcp-server:", err)
		return 1
	}
	return 0
}
