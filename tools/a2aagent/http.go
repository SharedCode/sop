package a2aagent

import (
	"net/http"

	"github.com/a2aproject/a2a-go/a2asrv"

	"github.com/sharedcode/sop/tools/runbookstore"
)

// NewMux wires this agent's card and JSON-RPC endpoint onto a fresh
// http.ServeMux: the agent card at a2asrv.WellKnownAgentCardPath
// (/.well-known/agent-card.json per the SDK's actual spec-derived
// constant), and the JSON-RPC transport at invokePath.
func NewMux(store *runbookstore.Store, baseURL, invokePath string) *http.ServeMux {
	card := AgentCard(baseURL)
	handler := a2asrv.NewHandler(NewExecutor(store))

	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle(invokePath, a2asrv.NewJSONRPCHandler(handler))
	return mux
}
