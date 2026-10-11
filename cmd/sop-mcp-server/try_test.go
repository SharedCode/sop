package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func try(t *testing.T, args ...string) (code int, out, errw string) {
	t.Helper()
	var o, e bytes.Buffer
	code = runTry(args, &o, &e)
	return code, o.String(), e.String()
}

func TestTryAllowsAnOrderedRunAndExitsZero(t *testing.T) {
	p := writeRunbook(t, goodRunbook)
	code, out, errw := try(t, p, "deploy", "run_tests", "get_approval", "deploy_prod")
	if code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, errw)
	}
	if !strings.Contains(out, "3 allowed, 0 blocked") || strings.Contains(out, "BLOCKED") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestTryBlocksOutOfOrderAndSaysWhatToRunFirst(t *testing.T) {
	p := writeRunbook(t, goodRunbook)
	code, out, _ := try(t, p, "deploy", "deploy_prod", "run_tests", "get_approval", "deploy_prod")
	if code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	for _, want := range []string{"deploy_prod", "BLOCKED", "run first: run_tests", "3 allowed, 1 blocked"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestTryBlockedStepDoesNotAdvanceTheTrace(t *testing.T) {
	p := writeRunbook(t, goodRunbook)
	_, out, _ := try(t, "--json", p, "deploy", "deploy_prod", "get_approval")
	var res tryResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("not json: %v\n%s", err, out)
	}
	if res.Blocked != 2 || len(res.Trace) != 0 {
		t.Errorf("blocked=%d trace=%v, want 2 blocked and an empty trace", res.Blocked, res.Trace)
	}
	if res.Steps[0].Next != "run_established_by_steps" || len(res.Steps[0].RunFirst) == 0 {
		t.Errorf("first step should point at steps to run: %+v", res.Steps[0])
	}
}

func TestTryRefusesUnknownWorkflowAndStep(t *testing.T) {
	p := writeRunbook(t, goodRunbook)
	code, _, errw := try(t, p, "nope", "run_tests")
	if code != 2 || !strings.Contains(errw, `no workflow "nope"`) || !strings.Contains(errw, "deploy") {
		t.Errorf("workflow: exit %d, stderr %q", code, errw)
	}
	code, _, errw = try(t, p, "deploy", "run_tests", "dpeloy_prod")
	if code != 2 || !strings.Contains(errw, "unknown step dpeloy_prod") || !strings.Contains(errw, "deploy_prod") {
		t.Errorf("step: exit %d, stderr %q", code, errw)
	}
}

func TestTryReportsAFileThatDoesNotLoad(t *testing.T) {
	code, _, errw := try(t, writeRunbook(t, badRunbook), "deploy", "a")
	if code != 1 || !strings.Contains(errw, "try:") {
		t.Errorf("exit %d, stderr %q", code, errw)
	}
}

func TestTryUsageAndDispatch(t *testing.T) {
	if code, _, errw := try(t, "only-a-file"); code != 2 || !strings.Contains(errw, "usage:") {
		t.Errorf("exit %d, stderr %q", code, errw)
	}
	var o, e bytes.Buffer
	if code, serve := dispatch([]string{"try", "--help"}, &o, &e); code != 0 || serve || !strings.Contains(o.String(), "usage:") {
		t.Errorf("dispatch try --help: code %d serve %v out %q", code, serve, o.String())
	}
}
