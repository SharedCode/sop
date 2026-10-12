package k8sgate

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sharedcode/joltrin/v5/tools/runbookstore"
)

func dbStore(t *testing.T) *runbookstore.Store {
	t.Helper()
	wf, err := runbookstore.DBMaintenanceWorkflow()
	if err != nil {
		t.Fatal(err)
	}
	s := runbookstore.New()
	if err := s.RegisterWorkflow("db-maintenance", wf); err != nil {
		t.Fatal(err)
	}
	return s
}

var pvcDelete = Rule{Resource: "persistentvolumeclaims", Operation: "DELETE", Workflow: "db-maintenance", Step: "drop_prod_db"}

func newGate(t *testing.T, opts ...Option) (*Gate, *runbookstore.Store) {
	t.Helper()
	s := dbStore(t)
	g, err := New(s, Policy{Rules: []Rule{pvcDelete}}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return g, s
}

// review builds an AdmissionReview body for a request on the given resource.
func review(op, group, resource string, annotations map[string]string) string {
	obj := map[string]any{"metadata": map[string]any{"annotations": annotations}}
	r := map[string]any{
		"apiVersion": "admission.k8s.io/v1",
		"kind":       "AdmissionReview",
		"request": map[string]any{
			"uid":       "uid-1",
			"operation": op,
			"name":      "orders-db-data",
			"namespace": "prod",
			"resource":  map[string]any{"group": group, "version": "v1", "resource": resource},
			"oldObject": obj,
		},
	}
	b, _ := json.Marshal(r)
	return string(b)
}

type verdict struct {
	Code     int
	Allowed  bool
	UID      string
	Message  string
	Warnings []string
}

func validate(t *testing.T, g *Gate, body string) verdict {
	t.Helper()
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/validate", strings.NewReader(body)))
	v := verdict{Code: rec.Code}
	if rec.Code != http.StatusOK {
		return v
	}
	var out struct {
		Response struct {
			UID     string `json:"uid"`
			Allowed bool   `json:"allowed"`
			Status  struct {
				Message string `json:"message"`
			} `json:"status"`
			Warnings []string `json:"warnings"`
		} `json:"response"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not an AdmissionReview: %v\n%s", err, rec.Body.String())
	}
	v.Allowed, v.UID, v.Message, v.Warnings = out.Response.Allowed, out.Response.UID, out.Response.Status.Message, out.Response.Warnings
	return v
}

func TestDeleteIsDeniedWhenPrerequisitesNotRecorded(t *testing.T) {
	g, _ := newGate(t)
	v := validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))
	if v.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", v.Code)
	}
	if v.Allowed {
		t.Fatal("DELETE was allowed with no backup recorded")
	}
	if v.UID != "uid-1" {
		t.Errorf("uid = %q, want it echoed back", v.UID)
	}
	if !strings.Contains(v.Message, "backup_validated") {
		t.Errorf("message %q should name the missing state", v.Message)
	}
}

func commit(t *testing.T, g *Gate, token, workflow, trace, step string) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"workflow": workflow, "trace_id": trace, "step": step})
	req := httptest.NewRequest(http.MethodPost, "/commit", strings.NewReader(string(body)))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestDeleteIsAllowedOnceStepsAreCommitted(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	for _, step := range []string{"take_backup", "validate_backup"} {
		if code, out := commit(t, g, "s3cret", "db-maintenance", "t1", step); code != http.StatusOK {
			t.Fatalf("commit %s = %d %v, want 200", step, code, out)
		}
	}
	v := validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))
	if !v.Allowed {
		t.Fatalf("DELETE denied after backup was validated: %s", v.Message)
	}
}

func TestCommitRejectsMissingOrWrongToken(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	for name, token := range map[string]string{"missing": "", "wrong": "nope"} {
		if code, _ := commit(t, g, token, "db-maintenance", "t1", "take_backup"); code != http.StatusUnauthorized {
			t.Errorf("%s token: status = %d, want 401", name, code)
		}
	}
	// The rejected calls must not have recorded anything: a later valid
	// validate_backup still needs take_backup first.
	if code, _ := commit(t, g, "s3cret", "db-maintenance", "t1", "validate_backup"); code != http.StatusConflict {
		t.Errorf("validate_backup after rejected take_backup = %d, want 409", code)
	}
}

func TestCommitIsDisabledWithoutAConfiguredToken(t *testing.T) {
	g, _ := newGate(t)
	if code, _ := commit(t, g, "anything", "db-maintenance", "t1", "take_backup"); code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 when no commit token is configured", code)
	}
}

func TestRequestsNoRuleCoversAreAllowed(t *testing.T) {
	g, _ := newGate(t)
	ann := map[string]string{"joltrin.dev/trace-id": "t1"}
	for name, body := range map[string]string{
		"other resource":  review("DELETE", "", "configmaps", ann),
		"other operation": review("UPDATE", "", "persistentvolumeclaims", ann),
		"other group":     review("DELETE", "apps", "persistentvolumeclaims", ann),
	} {
		if v := validate(t, g, body); !v.Allowed {
			t.Errorf("%s: denied (%s), want allowed because no rule covers it", name, v.Message)
		}
	}
}

func TestMatchedRequestWithoutTraceIDIsDenied(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	v := validate(t, g, review("DELETE", "", "persistentvolumeclaims", nil))
	if v.Allowed {
		t.Fatal("DELETE was allowed with no trace id annotation")
	}
	if !strings.Contains(v.Message, "joltrin.dev/trace-id") {
		t.Errorf("message %q should say which annotation is missing", v.Message)
	}
}

func TestCommitWithoutTraceIDIsRejected(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	if code, _ := commit(t, g, "s3cret", "db-maintenance", "", "take_backup"); code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", code)
	}
}

func TestCommitOfUnknownWorkflowOrStepIsNotFound(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	if code, _ := commit(t, g, "s3cret", "no-such-runbook", "t1", "take_backup"); code != http.StatusNotFound {
		t.Errorf("unknown workflow: status = %d, want 404", code)
	}
	if code, _ := commit(t, g, "s3cret", "db-maintenance", "t1", "no_such_step"); code != http.StatusNotFound {
		t.Errorf("unknown step: status = %d, want 404", code)
	}
}

func TestNewRejectsRulesThatReferenceNothing(t *testing.T) {
	s := dbStore(t)
	bad := map[string]Rule{
		"unknown workflow": {Resource: "persistentvolumeclaims", Operation: "DELETE", Workflow: "nope", Step: "drop_prod_db"},
		"unknown step":     {Resource: "persistentvolumeclaims", Operation: "DELETE", Workflow: "db-maintenance", Step: "nope"},
		"no resource":      {Operation: "DELETE", Workflow: "db-maintenance", Step: "drop_prod_db"},
		"no operation":     {Resource: "persistentvolumeclaims", Workflow: "db-maintenance", Step: "drop_prod_db"},
	}
	for name, r := range bad {
		if _, err := New(s, Policy{Rules: []Rule{r}}); err == nil {
			t.Errorf("%s: New accepted the rule", name)
		}
	}
	if _, err := New(s, Policy{}); err == nil {
		t.Error("New accepted a policy with no rules")
	}
}

func TestWarnModeAllowsButSaysWhatWouldHaveBeenBlocked(t *testing.T) {
	g, _ := newGate(t, WithWarnOnly())
	v := validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))
	if !v.Allowed {
		t.Fatalf("warn mode denied the request: %s", v.Message)
	}
	if len(v.Warnings) != 1 || !strings.Contains(v.Warnings[0], "backup_validated") {
		t.Errorf("warnings = %v, want one naming backup_validated", v.Warnings)
	}
}

func TestLoadPolicyReadsRulesAndRejectsTypos(t *testing.T) {
	p, err := LoadPolicy(strings.NewReader(`{"rules":[{"resource":"persistentvolumeclaims","operation":"DELETE","workflow":"db-maintenance","step":"drop_prod_db"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rules) != 1 || p.Rules[0].Step != "drop_prod_db" {
		t.Errorf("policy = %+v", p)
	}
	if _, err := LoadPolicy(strings.NewReader(`{"rules":[{"resorce":"x"}]}`)); err == nil {
		t.Error("a misspelled key was accepted")
	}
	if _, err := LoadPolicy(strings.NewReader(`{"rules":[]} {"rules":[]}`)); err == nil {
		t.Error("trailing content was accepted")
	}
}

func TestOversizedRequestBodyIsRejected(t *testing.T) {
	g, _ := newGate(t)
	// A well-formed review, so only its size can be the reason to refuse it.
	big := review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": strings.Repeat("a", 2<<20)})
	if v := validate(t, g, big); v.Code != http.StatusBadRequest && v.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 400 or 413", v.Code)
	}
}

func TestHealthz(t *testing.T) {
	g, _ := newGate(t)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
