package k8sgate

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func scrape(t *testing.T, g *Gate) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	return rec.Body.String()
}

func wantLine(t *testing.T, body, line string) {
	t.Helper()
	for _, l := range strings.Split(body, "\n") {
		if l == line {
			return
		}
	}
	t.Errorf("metrics missing line %q in:\n%s", line, body)
}

func TestMetricsCountAdmissionDecisions(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	trace := map[string]string{"joltrin.dev/trace-id": "t1"}
	validate(t, g, review("DELETE", "", "persistentvolumeclaims", trace)) // denied
	commit(t, g, "s3cret", "db-maintenance", "t1", "take_backup")
	commit(t, g, "s3cret", "db-maintenance", "t1", "validate_backup")
	validate(t, g, review("DELETE", "", "persistentvolumeclaims", trace)) // allowed
	validate(t, g, review("CREATE", "", "pods", nil))                     // no rule covers it

	body := scrape(t, g)
	wantLine(t, body, `joltrin_k8sgate_admission_total{operation="DELETE",resource="persistentvolumeclaims",result="denied"} 1`)
	wantLine(t, body, `joltrin_k8sgate_admission_total{operation="DELETE",resource="persistentvolumeclaims",result="allowed"} 1`)
	wantLine(t, body, `joltrin_k8sgate_admission_total{operation="CREATE",resource="pods",result="unmatched"} 1`)
}

func TestMetricsCountWarnOnlyAsWarned(t *testing.T) {
	g, _ := newGate(t, WithWarnOnly())
	validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))
	wantLine(t, scrape(t, g), `joltrin_k8sgate_admission_total{operation="DELETE",resource="persistentvolumeclaims",result="warned"} 1`)
}

func TestMetricsCountCommitOutcomes(t *testing.T) {
	g, _ := newGate(t, WithCommitToken("s3cret"))
	commit(t, g, "s3cret", "db-maintenance", "t1", "take_backup")  // ok
	commit(t, g, "wrong", "db-maintenance", "t1", "take_backup")   // unauthorized
	commit(t, g, "s3cret", "db-maintenance", "t2", "drop_prod_db") // conflict: prerequisites missing

	body := scrape(t, g)
	wantLine(t, body, `joltrin_k8sgate_commit_total{result="ok"} 1`)
	wantLine(t, body, `joltrin_k8sgate_commit_total{result="unauthorized"} 1`)
	wantLine(t, body, `joltrin_k8sgate_commit_total{result="conflict"} 1`)
}

func TestMetricLabelsCannotBeInjectedByRequestContent(t *testing.T) {
	g, _ := newGate(t)
	validate(t, g, review("DELETE\"} 99\n#", "", "pods\"x", nil))
	for _, l := range strings.Split(scrape(t, g), "\n") {
		if strings.HasPrefix(l, "#") || l == "" || strings.HasPrefix(l, "joltrin_k8sgate_") {
			continue
		}
		t.Fatalf("request content produced a foreign metrics line: %q", l)
	}
}

func TestAdmissionDecisionsAreLoggedAsStructuredAuditRecords(t *testing.T) {
	var buf bytes.Buffer
	g, _ := newGate(t, WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
	validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
	}
	for k, want := range map[string]string{
		"msg": "admission", "result": "denied", "operation": "DELETE",
		"resource": "persistentvolumeclaims", "trace_id": "t1", "uid": "uid-1",
	} {
		if rec[k] != want {
			t.Errorf("log field %s = %v, want %q", k, rec[k], want)
		}
	}
	if reason, _ := rec["reason"].(string); !strings.Contains(reason, "backup_validated") {
		t.Errorf("log reason = %q, want it to name the missing state", reason)
	}
}

func TestFromFilesUsesTheConfiguredLogger(t *testing.T) {
	var buf bytes.Buffer
	dir := t.TempDir()
	g, err := FromFiles(Files{Policy: write(t, dir, "policy.json", policyJSON), Logger: slog.New(slog.NewJSONHandler(&buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{"joltrin.dev/trace-id": "t1"}))
	if !strings.Contains(buf.String(), `"msg":"admission"`) {
		t.Errorf("no audit record in %q", buf.String())
	}
}
