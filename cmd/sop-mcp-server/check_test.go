package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/joltrin/v5/verify"
)

const goodRunbook = `{
  "workflows": {
    "deploy": {
      "steps": [
        {"id": "run_tests",    "establishes": ["tests_passed"]},
        {"id": "get_approval", "requires": ["tests_passed"], "establishes": ["approved"]},
        {"id": "deploy_prod",  "requires": ["tests_passed", "approved"], "establishes": ["deployed"]}
      ],
      "safety": [{"name": "no-deploy-without-approval", "forbidden": "deployed", "requires": "approved"}]
    }
  }
}`

const badRunbook = `{"workflows":{"deploy":{"steps":[{"id":"a","requires":["typo"]}]}}}`

func writeRunbook(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "runbooks.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func check(t *testing.T, args ...string) (code int, out, errw string) {
	t.Helper()
	var o, e bytes.Buffer
	code = runCheck(args, &o, &e)
	return code, o.String(), e.String()
}

func TestCheckShowsWhatTheBarrierWillEnforce(t *testing.T) {
	code, out, errw := check(t, writeRunbook(t, goodRunbook))
	if code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, errw)
	}
	for _, want := range []string{
		"1 workflow",
		"deploy",
		"run_tests",
		"can run first",
		"deploy_prod",
		"needs approved, tests_passed",
		"no-deploy-without-approval",
		`"deployed" cannot be reached unless "approved" is established first`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestCheckRefusesABadRunbookWithTheLoadersError(t *testing.T) {
	code, out, errw := check(t, writeRunbook(t, badRunbook))
	if code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if !strings.Contains(errw, `requires state "typo"`) {
		t.Errorf("stderr should carry the loader's error:\n%s", errw)
	}
	if strings.Contains(out, "valid") {
		t.Errorf("a bad file must not be called valid:\n%s", out)
	}
}

func TestCheckNeedsAFileThatExists(t *testing.T) {
	if code, _, errw := check(t); code != 2 || !strings.Contains(errw, "usage") {
		t.Errorf("no argument: exit %d, stderr %q", code, errw)
	}
	if code, _, _ := check(t, filepath.Join(t.TempDir(), "missing.json")); code != 1 {
		t.Errorf("missing file: exit %d, want 1", code)
	}
}

func TestSetupRefusesARunbookFileThatWillNotLoad(t *testing.T) {
	rec := &recorder{have: map[string]bool{"claude": true}}
	code, out, errw := setup(t, "/bin/sop-mcp-server", rec, "--apply", "--runbooks", writeRunbook(t, badRunbook))
	if code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if len(rec.calls) != 0 {
		t.Errorf("nothing may be registered with a file the server cannot load, calls = %v", rec.calls)
	}
	if !strings.Contains(errw, `requires state "typo"`) {
		t.Errorf("the reason should be shown:\nstdout:\n%s\nstderr:\n%s", out, errw)
	}
}

func TestSetupAcceptsARunbookFileThatLoads(t *testing.T) {
	rec := &recorder{have: map[string]bool{"claude": true}}
	code, out, _ := setup(t, "/bin/sop-mcp-server", rec, "--apply", "--runbooks", writeRunbook(t, goodRunbook))
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if len(rec.calls) == 0 {
		t.Error("a valid file should still be registered")
	}
}

// Each state has a producer, so this file loads, but neither step can start.
const cyclicRunbook = `{"workflows":{"cycle":{"steps":[
  {"id":"a","requires":["b_done"],"establishes":["a_done"]},
  {"id":"b","requires":["a_done"],"establishes":["b_done"]}
]}}}`

func TestCheckDiagnoseFindsDependencyCycle(t *testing.T) {
	p := writeRunbook(t, cyclicRunbook)
	if code, _, errw := check(t, p); code != 0 {
		t.Fatalf("load check: %d %s", code, errw)
	}
	code, out, errw := check(t, "--diagnose", p)
	if code != 1 {
		t.Fatalf("diagnose exit %d, want 1; out=%s err=%s", code, out, errw)
	}
	for _, want := range []string{"BLOCKED", "a", "b", "b_done", "run first: b"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q: %s", want, out)
		}
	}
}

func TestCheckDiagnoseFindsRunnableOrder(t *testing.T) {
	code, out, errw := check(t, "--json", "--diagnose", writeRunbook(t, goodRunbook))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errw)
	}
	var result struct {
		Valid     bool `json:"valid"`
		Workflows []struct {
			Diagnostics struct {
				RunnableOrder []string          `json:"runnable_order"`
				Blocked       []json.RawMessage `json:"blocked"`
			} `json:"diagnostics"`
		} `json:"workflows"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Valid || len(result.Workflows) != 1 {
		t.Fatalf("unexpected result: %s", out)
	}
	d := result.Workflows[0].Diagnostics
	if strings.Join(d.RunnableOrder, ",") != "run_tests,get_approval,deploy_prod" || len(d.Blocked) != 0 {
		t.Fatalf("unexpected diagnostics: %s", out)
	}
}

func TestCheckDiagnoseHonorsSafetyRules(t *testing.T) {
	p := writeRunbook(t, `{"workflows":{"unsafe":{"steps":[
 {"id":"approve_and_deploy","establishes":["approved","deployed"]}
 ],"safety":[{"name":"approval-first","forbidden":"deployed","requires":"approved"}]}}}`)
	code, out, errw := check(t, p, "--diagnose", "--json")
	if code != 1 || !strings.Contains(out, "approval-first") || !strings.Contains(out, "approve_and_deploy") {
		t.Fatalf("exit %d out=%s err=%s", code, out, errw)
	}
	var res map[string]any
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatal(err)
	}
	if res["valid"] != true {
		t.Fatalf("load validity changed: %s", out)
	}
}

func TestCheckDiagnosticsStopsAtItsBudget(t *testing.T) {
	wf, err := verify.NewWorkflow([]verify.Step{
		{ID: "first", Establishes: []verify.State{"ready"}},
		{ID: "second", Requires: []verify.State{"ready"}},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	d := diagnoseWorkflow(wf, 1)
	if !strings.Contains(d.Error, "limit reached after 1 checks") || len(d.RunnableOrder) != 1 || len(d.Blocked) != 0 {
		t.Fatalf("incomplete diagnostics must not claim steps are blocked: %+v", d)
	}
	// Diagnostics must not retain state between invocations.
	complete := diagnoseWorkflow(wf, 10)
	if complete.Error != "" || strings.Join(complete.RunnableOrder, ",") != "first,second" {
		t.Fatalf("%+v", complete)
	}
}

func TestCheckDiagnosticsRetriesEarlierBlockedSteps(t *testing.T) {
	p := writeRunbook(t, `{"workflows":{"deploy":{"steps":[
 {"id":"start","establishes":["started"]},
 {"id":"a_finish","requires":["ready"],"establishes":["finished"]},
 {"id":"z_prepare","requires":["started"],"establishes":["ready"]}
 ]}}}`)
	code, out, errw := check(t, p, "--diagnose")
	if code != 0 || !strings.Contains(out, "runnable order: start -> z_prepare -> a_finish") {
		t.Fatalf("exit %d out=%s err=%s", code, out, errw)
	}
}

func TestCheckDiagnosticsNextActionMatchesBarrier(t *testing.T) {
	for _, producer := range []bool{false, true} {
		steps := []verify.Step{{ID: "blocked", Requires: []verify.State{"missing"}, Establishes: []verify.State{"waiting"}}}
		if producer {
			steps = append(steps, verify.Step{ID: "producer", Requires: []verify.State{"waiting"}, Establishes: []verify.State{"missing"}})
		}
		wf, err := verify.NewWorkflow(steps, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		d := diagnoseWorkflow(wf, 10)
		if len(d.Blocked) == 0 {
			t.Fatalf("expected blocked steps: %+v", d)
		}
		for _, st := range d.Blocked {
			missing := verify.State("missing")
			if st.Step == "producer" {
				missing = "waiting"
			}
			want := wf.NextAction(&verify.Violation{MissingState: missing})
			if st.Next != want {
				t.Errorf("producer=%v step=%s next=%s want=%s", producer, st.Step, st.Next, want)
			}
		}
	}
}

// A ring has no runnable first step. Diagnosing each missing prerequisite
// should reuse the producer index rather than rescan the full ring.
func BenchmarkCheckDiagnosticsCycle(b *testing.B) {
	for _, size := range []int{1000, 4000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			steps := make([]verify.Step, size)
			for i := range steps {
				steps[i] = verify.Step{ID: verify.StepID(fmt.Sprintf("step_%05d", i)), Requires: []verify.State{verify.State(fmt.Sprint((i + 1) % size))}, Establishes: []verify.State{verify.State(fmt.Sprint(i))}}
			}
			wf, err := verify.NewWorkflow(steps, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d := diagnoseWorkflow(wf, 100000)
				if d.Error != "" || len(d.Blocked) != size {
					b.Fatalf("incomplete ring diagnostics: blocked=%d error=%s", len(d.Blocked), d.Error)
				}
			}
		})
	}
}
