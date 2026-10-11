package main

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/sharedcode/joltrin/v5/verify"
)

const tryUsage = "usage: sop-mcp-server try [--json] <runbooks.json> <workflow> <step>...\n"

// tryStep is one decision in "try --json".
type tryStep struct {
	Step     string   `json:"step"`
	Decision string   `json:"decision"` // "allowed" or "blocked"
	Reason   string   `json:"reason,omitempty"`
	Next     string   `json:"next,omitempty"`
	RunFirst []string `json:"run_first,omitempty"`
}

type tryResult struct {
	Workflow string    `json:"workflow"`
	Steps    []tryStep `json:"steps"`
	Trace    []string  `json:"trace"`
	Blocked  int       `json:"blocked"`
}

// tryRun replays steps against wf in one fresh trace, the way an agent would
// call execute_step, and records each decision. A blocked step changes nothing
// and the replay carries on with the next one.
func tryRun(name string, wf *verify.Workflow, steps []string) (tryResult, error) {
	trace := verify.NewTrace()
	res := tryResult{Workflow: name, Steps: []tryStep{}, Trace: []string{}}
	for _, s := range steps {
		err := wf.CheckAndCommit(trace, verify.StepID(s))
		if err == nil {
			res.Steps = append(res.Steps, tryStep{Step: s, Decision: "allowed"})
			continue
		}
		var v *verify.Violation
		if !errors.As(err, &v) {
			return tryResult{}, fmt.Errorf("%s: %w", s, err)
		}
		st := tryStep{Step: s, Decision: "blocked", Reason: v.Message, Next: wf.NextAction(v)}
		for _, id := range wf.StepsThatEstablish(v.MissingState) {
			st.RunFirst = append(st.RunFirst, string(id))
		}
		res.Steps = append(res.Steps, st)
		res.Blocked++
	}
	for _, s := range trace.ExecutedSteps() {
		res.Trace = append(res.Trace, string(s))
	}
	return res, nil
}

// runTry implements "sop-mcp-server try": it replays a list of steps against a
// runbook file and says which the barrier would allow, so an author can test a
// runbook without an agent. Exit codes: 0 when every step is allowed, 1 when
// any is blocked or the file does not load, 2 for a usage error or an unknown
// workflow or step.
func runTry(args []string, out, errw io.Writer) int {
	if isHelp(args) {
		fmt.Fprint(out, tryUsage)
		return 0
	}
	args, asJSON := popFlag(args, "--json")
	if len(args) < 3 {
		fmt.Fprint(errw, tryUsage)
		return 2
	}
	store, names, err := loadRunbooks(args[0])
	if err != nil {
		fmt.Fprintln(errw, "try:", err)
		return 1
	}
	name, steps := args[1], args[2:]
	wf, ok := store.Workflow(name)
	if !ok {
		fmt.Fprintf(errw, "try: no workflow %q in %s. It has: %s\n", name, args[0], strings.Join(names, ", "))
		return 2
	}
	var unknown []string
	for _, s := range steps {
		if _, ok := wf.Steps[verify.StepID(s)]; !ok {
			unknown = append(unknown, s)
		}
	}
	if len(unknown) > 0 {
		known := make([]string, 0, len(wf.Steps))
		for id := range wf.Steps {
			known = append(known, string(id))
		}
		sort.Strings(known)
		fmt.Fprintf(errw, "try: unknown step %s in %q. It has: %s\n", strings.Join(unknown, ", "), name, strings.Join(known, ", "))
		return 2
	}

	res, err := tryRun(name, wf, steps)
	if err != nil {
		fmt.Fprintln(errw, "try:", err)
		return 1
	}
	code := 0
	if res.Blocked > 0 {
		code = 1
	}
	if asJSON {
		if werr := writeJSON(out, res); werr != nil {
			fmt.Fprintln(errw, "try: could not write the result:", werr)
			return 1
		}
		return code
	}
	for i, s := range res.Steps {
		if s.Decision == "allowed" {
			fmt.Fprintf(out, "%2d. %-24s allowed\n", i+1, s.Step)
			continue
		}
		fmt.Fprintf(out, "%2d. %-24s BLOCKED  %s\n", i+1, s.Step, s.Reason)
		if len(s.RunFirst) > 0 {
			fmt.Fprintf(out, "    run first: %s\n", strings.Join(s.RunFirst, ", "))
		}
	}
	fmt.Fprintf(out, "\n%d allowed, %d blocked\n", len(res.Steps)-res.Blocked, res.Blocked)
	return code
}
