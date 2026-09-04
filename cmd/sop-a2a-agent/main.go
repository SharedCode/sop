// Command sop-a2a-agent serves tools/a2aagent over HTTP, registering the
// example db-maintenance runbook (tools/runbookstore.DBMaintenanceWorkflow)
// so an A2A client has something real to delegate a task against out of the
// box. Serves the agent card at /.well-known/agent-card.json and the
// JSON-RPC transport at /a2a/invoke.
//
//	go run ./cmd/sop-a2a-agent
//	curl localhost:8087/.well-known/agent-card.json
package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/sharedcode/sop/tools/a2aagent"
	"github.com/sharedcode/sop/tools/runbookstore"
)

const (
	addr       = ":8087"
	invokePath = "/a2a/invoke"
)

func main() {
	wf, err := runbookstore.DBMaintenanceWorkflow()
	if err != nil {
		fmt.Fprintln(os.Stderr, "sop-a2a-agent:", err)
		os.Exit(1)
	}

	store := runbookstore.New()
	if err := store.RegisterWorkflow("db-maintenance", wf); err != nil {
		fmt.Fprintln(os.Stderr, "sop-a2a-agent:", err)
		os.Exit(1)
	}

	mux := a2aagent.NewMux(store, "http://localhost"+addr, invokePath)
	fmt.Printf("sop-a2a-agent listening on %s (agent card: /.well-known/agent-card.json, invoke: %s)\n", addr, invokePath)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, "sop-a2a-agent:", err)
		os.Exit(1)
	}
}
