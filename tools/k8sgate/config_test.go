package k8sgate

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const policyJSON = `{"rules":[{"resource":"persistentvolumeclaims","operation":"DELETE","workflow":"db-maintenance","step":"drop_prod_db"}]}`

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFromFilesBuildsAWorkingGateWithTheBuiltInRunbook(t *testing.T) {
	dir := t.TempDir()
	g, err := FromFiles(Files{Policy: write(t, dir, "policy.json", policyJSON), CommitTokenFile: write(t, dir, "token", "s3cret\n")})
	if err != nil {
		t.Fatal(err)
	}
	// The trailing newline of a mounted secret must not become part of the token.
	if code, out := commit(t, g, "s3cret", "db-maintenance", "t1", "take_backup"); code != http.StatusOK {
		t.Fatalf("commit = %d %v", code, out)
	}
}

func TestFromFilesLoadsACustomRunbookFile(t *testing.T) {
	dir := t.TempDir()
	rb := write(t, dir, "runbooks.json", `{"workflows":{"pvc-retire":{"steps":[
		{"id":"snapshot","establishes":["snapshotted"]},
		{"id":"delete_pvc","requires":["snapshotted"],"establishes":["pvc_deleted"]},
		{"id":"restore","requires":["snapshotted"],"establishes":["restored"]},
		{"id":"restore_after_delete","requires":["pvc_deleted"],"establishes":["restored"]}],
		"safety":[{"name":"no-delete-without-snapshot","forbidden":"pvc_deleted","requires":"snapshotted"}],
		"reachability":[{"name":"restorable","target":"restored"}]}}}`)
	pol := write(t, dir, "policy.json", `{"rules":[{"resource":"persistentvolumeclaims","operation":"DELETE","workflow":"pvc-retire","step":"delete_pvc"}]}`)
	g, err := FromFiles(Files{Runbooks: rb, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	v := validate(t, g, review("DELETE", "", "persistentvolumeclaims", map[string]string{TraceAnnotation: "t1"}))
	if v.Allowed || !strings.Contains(v.Message, "snapshotted") {
		t.Errorf("allowed=%v message=%q, want a denial naming snapshotted", v.Allowed, v.Message)
	}
}

func TestFromFilesRefusesBrokenInput(t *testing.T) {
	dir := t.TempDir()
	good := write(t, dir, "policy.json", policyJSON)
	cases := map[string]Files{
		"missing policy":       {Policy: filepath.Join(dir, "none.json")},
		"no policy path":       {},
		"empty token file":     {Policy: good, CommitTokenFile: write(t, dir, "empty", "\n")},
		"missing token file":   {Policy: good, CommitTokenFile: filepath.Join(dir, "none")},
		"missing runbook file": {Policy: good, Runbooks: filepath.Join(dir, "none.json")},
	}
	for name, f := range cases {
		if _, err := FromFiles(f); err == nil {
			t.Errorf("%s: FromFiles succeeded", name)
		}
	}
}
