// Package k8sgate is a Kubernetes validating admission webhook that puts the
// verify barrier in front of destructive cluster operations.
package k8sgate

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/sharedcode/joltrin/v5/tools/runbookstore"
	"github.com/sharedcode/joltrin/v5/verify"
)

const (
	// maxBodyBytes stays above the 1.5 MiB the API server accepts for one
	// object, so a real review is never cut off.
	maxBodyBytes   = 2 << 20
	maxPolicyBytes = 1 << 20
)

// TraceAnnotation names the annotation that carries a trace id.
const TraceAnnotation = "joltrin.dev/trace-id"

// Rule maps one Kubernetes operation to a runbook step.
type Rule struct {
	Group     string `json:"group"`
	Resource  string `json:"resource"`
	Operation string `json:"operation"`
	Workflow  string `json:"workflow"`
	Step      string `json:"step"`
}

// Policy is the set of rules the gate enforces.
type Policy struct {
	Rules []Rule `json:"rules"`
}

// Gate serves the admission and commit endpoints.
type Gate struct {
	store       *runbookstore.Store
	policy      Policy
	commitToken string
	warnOnly    bool
}

// Option configures a Gate.
type Option func(*Gate)

// WithWarnOnly makes the gate allow every request and report what it would
// have blocked as an admission warning.
func WithWarnOnly() Option { return func(g *Gate) { g.warnOnly = true } }

// WithCommitToken requires this bearer token on the commit endpoint.
func WithCommitToken(token string) Option { return func(g *Gate) { g.commitToken = token } }

// New builds a Gate over the runbooks in store.
func New(store *runbookstore.Store, p Policy, opts ...Option) (*Gate, error) {
	if len(p.Rules) == 0 {
		return nil, errors.New("k8sgate: the policy has no rules")
	}
	for i, r := range p.Rules {
		if r.Resource == "" || r.Operation == "" {
			return nil, fmt.Errorf("k8sgate: rule %d needs both a resource and an operation", i)
		}
		wf, ok := store.Workflow(r.Workflow)
		if !ok {
			return nil, fmt.Errorf("k8sgate: rule %d names workflow %q, which is not registered", i, r.Workflow)
		}
		if _, ok := wf.Steps[verify.StepID(r.Step)]; !ok {
			return nil, fmt.Errorf("k8sgate: rule %d names step %q, which workflow %q does not have", i, r.Step, r.Workflow)
		}
	}
	g := &Gate{store: store, policy: p}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// Handler returns the HTTP handler for the gate.
func (g *Gate) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	mux.HandleFunc("POST /validate", g.validate)
	mux.HandleFunc("POST /commit", g.commit)
	return mux
}

type admissionReview struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Request    *admissionRequest `json:"request,omitempty"`
	Response   *admissionResult  `json:"response,omitempty"`
}

type admissionRequest struct {
	UID       string `json:"uid"`
	Operation string `json:"operation"`
	Resource  struct {
		Group    string `json:"group"`
		Resource string `json:"resource"`
	} `json:"resource"`
	OldObject struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	} `json:"oldObject"`
}

type admissionResult struct {
	UID      string   `json:"uid"`
	Allowed  bool     `json:"allowed"`
	Warnings []string `json:"warnings,omitempty"`
	Status   *struct {
		Message string `json:"message"`
	} `json:"status,omitempty"`
}

func (g *Gate) validate(w http.ResponseWriter, r *http.Request) {
	var in admissionReview
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Request == nil {
		http.Error(w, "not an AdmissionReview", http.StatusBadRequest)
		return
	}
	req := in.Request
	res := &admissionResult{UID: req.UID, Allowed: true}
	rule, ok := g.match(req)
	if !ok {
		g.respond(w, in, res)
		return
	}
	traceID := req.OldObject.Metadata.Annotations[TraceAnnotation]
	if traceID == "" {
		g.deny(res, "the object has no "+TraceAnnotation+" annotation, so the gate cannot tell which run this "+req.Operation+" belongs to")
		g.respond(w, in, res)
		return
	}
	wf, _ := g.store.Workflow(rule.Workflow)
	trace := g.store.TraceFor(traceID)
	if err := wf.CheckSafety(trace, verify.StepID(rule.Step)); err != nil {
		g.deny(res, err.Error())
	}
	g.respond(w, in, res)
}

func (g *Gate) deny(res *admissionResult, msg string) {
	if g.warnOnly {
		res.Warnings = append(res.Warnings, "joltrin would block this: "+msg)
		return
	}
	res.Allowed = false
	res.Status = &struct {
		Message string `json:"message"`
	}{Message: msg}
}

func (g *Gate) respond(w http.ResponseWriter, in admissionReview, res *admissionResult) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(admissionReview{APIVersion: in.APIVersion, Kind: in.Kind, Response: res})
}

func (g *Gate) match(req *admissionRequest) (Rule, bool) {
	for _, r := range g.policy.Rules {
		if r.Group == req.Resource.Group && r.Resource == req.Resource.Resource && r.Operation == req.Operation {
			return r, true
		}
	}
	return Rule{}, false
}

type commitRequest struct {
	Workflow string `json:"workflow"`
	TraceID  string `json:"trace_id"`
	Step     string `json:"step"`
}

func (g *Gate) commit(w http.ResponseWriter, r *http.Request) {
	if g.commitToken == "" {
		http.Error(w, "the commit endpoint is disabled: no commit token is configured", http.StatusForbidden)
		return
	}
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(got), []byte(g.commitToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var in commitRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if in.TraceID == "" {
		http.Error(w, "trace_id is required", http.StatusBadRequest)
		return
	}
	wf, ok := g.store.Workflow(in.Workflow)
	if !ok {
		http.Error(w, "unknown workflow "+strconv.Quote(in.Workflow), http.StatusNotFound)
		return
	}
	if _, ok := wf.Steps[verify.StepID(in.Step)]; !ok {
		http.Error(w, "unknown step "+strconv.Quote(in.Step), http.StatusNotFound)
		return
	}
	trace := g.store.TraceFor(in.TraceID)
	if err := wf.CheckAndCommit(trace, verify.StepID(in.Step)); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"executed": true, "trace": trace.ExecutedSteps()})
}

// LoadPolicy reads a policy from JSON. Unknown fields and trailing content
// are errors, so a misspelled key is caught instead of ignored.
func LoadPolicy(r io.Reader) (Policy, error) {
	dec := json.NewDecoder(io.LimitReader(r, maxPolicyBytes))
	dec.DisallowUnknownFields()
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("k8sgate: invalid policy: %w", err)
	}
	if dec.More() {
		return Policy{}, errors.New("k8sgate: unexpected content after the policy")
	}
	return p, nil
}

// Files names the inputs of a gate. Runbooks is optional and defaults to the
// built-in db-maintenance runbook. CommitTokenFile is optional and, when empty,
// leaves the commit endpoint disabled.
type Files struct {
	Runbooks        string
	Policy          string
	CommitTokenFile string
	WarnOnly        bool
}

// FromFiles builds a Gate from files.
func FromFiles(f Files) (*Gate, error) {
	if f.Policy == "" {
		return nil, errors.New("k8sgate: a policy file is required")
	}
	store := runbookstore.New()
	if f.Runbooks == "" {
		wf, err := runbookstore.DBMaintenanceWorkflow()
		if err != nil {
			return nil, err
		}
		if err := store.RegisterWorkflow("db-maintenance", wf); err != nil {
			return nil, err
		}
	} else if _, err := runbookstore.LoadFile(store, f.Runbooks); err != nil {
		return nil, err
	}
	pf, err := os.Open(f.Policy)
	if err != nil {
		return nil, fmt.Errorf("k8sgate: %w", err)
	}
	defer pf.Close()
	policy, err := LoadPolicy(pf)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Policy, err)
	}
	var opts []Option
	if f.CommitTokenFile != "" {
		raw, err := os.ReadFile(f.CommitTokenFile)
		if err != nil {
			return nil, fmt.Errorf("k8sgate: %w", err)
		}
		token := strings.TrimSpace(string(raw))
		if token == "" {
			return nil, fmt.Errorf("k8sgate: the commit token file %s is empty", f.CommitTokenFile)
		}
		opts = append(opts, WithCommitToken(token))
	}
	if f.WarnOnly {
		opts = append(opts, WithWarnOnly())
	}
	return New(store, policy, opts...)
}
