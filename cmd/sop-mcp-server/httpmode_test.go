package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/sharedcode/joltrin/v5/tools/mcpserver"
)

const testToken = "a-test-token-of-enough-length"

func httpTestServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	store, _, err := newStore("")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := newHTTPHandler(mcpserver.New(store), token)
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func mcpClient(t *testing.T, ts *httptest.Server, token string) *client.Client {
	t.Helper()
	var opts []transport.StreamableHTTPCOption
	if token != "" {
		opts = append(opts, transport.WithHTTPHeaders(map[string]string{"Authorization": "Bearer " + token}))
	}
	c, err := client.NewStreamableHttpClient(ts.URL+httpEndpoint, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func initialize(ctx context.Context, c *client.Client) error {
	if err := c.Start(ctx); err != nil {
		return err
	}
	_, err := c.Initialize(ctx, mcp.InitializeRequest{Params: mcp.InitializeParams{
		ProtocolVersion: mcp.LATEST_PROTOCOL_VERSION,
		ClientInfo:      mcp.Implementation{Name: "t", Version: "0"},
	}})
	return err
}

func executed(t *testing.T, c *client.Client, trace, step string) bool {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = "execute_step"
	req.Params.Arguments = map[string]any{"workflow": "db-maintenance", "trace_id": trace, "step": step}
	res, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := res.StructuredContent.(map[string]any)
	return m["executed"] == true
}

func TestHTTPModeRefusesToListenOpenOnTheNetwork(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	var errw bytes.Buffer

	if _, err := parseHTTPArgs([]string{"--addr", "0.0.0.0:8088"}, get(nil), &errw); err == nil {
		t.Error("a non-loopback address with no token must be refused")
	}
	if _, err := parseHTTPArgs([]string{"--addr", "0.0.0.0:8088"}, get(map[string]string{"SOP_MCP_TOKEN": "short"}), &errw); err == nil {
		t.Error("a short token must be refused on a non-loopback address")
	}
	if _, err := parseHTTPArgs([]string{"--addr", "0.0.0.0:8088"}, get(map[string]string{"SOP_MCP_TOKEN": testToken}), &errw); err != nil {
		t.Errorf("a long token on a non-loopback address should be accepted: %v", err)
	}
	cfg, err := parseHTTPArgs(nil, get(nil), &errw)
	if err != nil || cfg.addr != defaultHTTPAddr {
		t.Errorf("the default must be loopback and need no token: %+v %v", cfg, err)
	}
	if _, err := parseHTTPArgs([]string{"--addr", "nonsense"}, get(nil), &errw); err == nil {
		t.Error("a malformed address must be refused")
	}
}

func TestHTTPModeRequiresTheToken(t *testing.T) {
	ts := httpTestServer(t, testToken)

	for name, header := range map[string]string{"missing": "", "wrong": "Bearer not-the-token", "wrong scheme": testToken} {
		req, _ := http.NewRequest("POST", ts.URL+httpEndpoint, strings.NewReader("{}"))
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s token: got %d, want 401", name, resp.StatusCode)
		}
	}

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("/healthz needs no token, got %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := initialize(ctx, mcpClient(t, ts, "not-the-token")); err == nil {
		t.Error("an MCP client with the wrong token must not be able to initialize")
	}
}

func TestHTTPModeBlocksThenAllowsOverTheWire(t *testing.T) {
	ts := httpTestServer(t, testToken)
	c := mcpClient(t, ts, testToken)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := initialize(ctx, c); err != nil {
		t.Fatal(err)
	}

	if executed(t, c, "t1", "drop_prod_db") {
		t.Fatal("the drop must be blocked before a backup is validated")
	}
	for _, step := range []string{"take_backup", "validate_backup", "drop_prod_db"} {
		if !executed(t, c, "t1", step) {
			t.Fatalf("%s should be allowed in order", step)
		}
	}
}

// The server is single tenant by design: one token, one trace namespace. This
// documents what that means, so a change to per-client traces has to change
// this test on purpose.
func TestHTTPModeIsSingleTenant(t *testing.T) {
	ts := httpTestServer(t, testToken)
	a, b := mcpClient(t, ts, testToken), mcpClient(t, ts, testToken)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, c := range []*client.Client{a, b} {
		if err := initialize(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	executed(t, a, "shared", "take_backup")
	executed(t, a, "shared", "validate_backup")
	if !executed(t, b, "shared", "drop_prod_db") {
		t.Error("two callers holding the same token and trace_id share one trace, so b sees a's progress")
	}
	if executed(t, b, "other", "drop_prod_db") {
		t.Error("a different trace_id starts with nothing established")
	}
}

func TestHTTPModeCapsRequestBodies(t *testing.T) {
	ts := httpTestServer(t, testToken)
	req, _ := http.NewRequest("POST", ts.URL+httpEndpoint, strings.NewReader(strings.Repeat("x", 2<<20)))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Errorf("a 2 MiB body should be refused, got %d", resp.StatusCode)
	}
}

func TestServeHTTPStopsGracefullyWhenCancelled(t *testing.T) {
	store, _, err := newStore("")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var errw bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, ln, mcpserver.New(store), "", &errw) }()

	url := "http://" + ln.Addr().String() + "/healthz"
	for i := 0; ; i++ {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			break
		}
		if i > 50 {
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("shutdown returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serveHTTP did not return after the context was cancelled")
	}
	if _, err := http.Get(url); err == nil {
		t.Error("the port should be closed after shutdown")
	}
}
