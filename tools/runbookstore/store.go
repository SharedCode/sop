// Package runbookstore holds the registered ai/verify.Workflow runbooks and
// their in-progress execution traces, shared by every protocol front-end
// this repo exposes them through (tools/mcpserver, tools/a2aagent). Sharing
// one Store is what makes the two protocols actually interoperable rather
// than two isolated demos: a step executed via A2A advances the same trace
// an MCP client can inspect with validate_step, and vice versa.
package runbookstore

import (
	"fmt"
	"sync"

	"github.com/sharedcode/sop/ai/verify"
)

// Store holds the runbooks this server can validate and execute steps
// against, plus one execution Trace per trace_id. Safe for concurrent use.
type Store struct {
	mu        sync.RWMutex
	workflows map[string]*verify.Workflow
	traces    map[string]*verify.Trace
}

// New returns an empty Store. Use RegisterWorkflow to add runbooks before
// serving.
func New() *Store {
	return &Store{
		workflows: make(map[string]*verify.Workflow),
		traces:    make(map[string]*verify.Trace),
	}
}

// RegisterWorkflow makes a runbook available under name. It runs
// VerifyReachability against the workflow immediately and returns an error
// if that fails: a runbook whose rollback path can dead-end should never be
// registered in the first place, catching that at registration time is
// strictly better than discovering it mid-incident.
func (s *Store) RegisterWorkflow(name string, wf *verify.Workflow) error {
	if err := wf.VerifyReachability(); err != nil {
		return fmt.Errorf("runbookstore: refusing to register workflow %q: %w", name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[name] = wf
	return nil
}

// Workflow returns the registered workflow by name, if any.
func (s *Store) Workflow(name string) (*verify.Workflow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[name]
	return wf, ok
}

// TraceFor returns the Trace for traceID, creating a fresh one on first use.
func (s *Store) TraceFor(traceID string) *verify.Trace {
	s.mu.Lock()
	defer s.mu.Unlock()
	tr, ok := s.traces[traceID]
	if !ok {
		tr = verify.NewTrace()
		s.traces[traceID] = tr
	}
	return tr
}
