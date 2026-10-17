package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/sharedcode/joltrin/v5/tools/runbookstore"
	"github.com/sharedcode/joltrin/v5/verify"
)

// loadRunbooks loads a runbook file with the loader the server uses, so a file
// that passes here is one the server will start with.
func loadRunbooks(path string) (*runbookstore.Store, []string, error) {
	store := runbookstore.New()
	names, err := runbookstore.LoadFile(store, path)
	return store, names, err
}

// checkResult is the machine-readable form of "check --json".
type checkResult struct {
	Valid     bool            `json:"valid"`
	File      string          `json:"file"`
	Error     string          `json:"error,omitempty"`
	Workflows []checkWorkflow `json:"workflows,omitempty"`
}

type checkWorkflow struct {
	Name         string            `json:"name"`
	Diagnostics  *checkDiagnostics `json:"diagnostics,omitempty"`
	Steps        []checkStep       `json:"steps"`
	Safety       []checkRule       `json:"safety"`
	Reachability []checkReach      `json:"reachability"`
}

type checkStep struct {
	ID          string   `json:"id"`
	Requires    []string `json:"requires"`
	Establishes []string `json:"establishes"`
}

type checkRule struct {
	Name      string `json:"name"`
	Forbidden string `json:"forbidden"`
	Requires  string `json:"requires"`
}

type checkReach struct {
	Name   string `json:"name"`
	Target string `json:"target"`
}

type checkDiagnostics struct {
	RunnableOrder []string  `json:"runnable_order"`
	Blocked       []tryStep `json:"blocked"`
	Error         string    `json:"error,omitempty"`
}

const checkUsage = "usage: sop-mcp-server check [--json] [--diagnose] <runbooks.json>\n"

// diagnoseWorkflow commits each step at most once in a fresh, in-memory trace.
// States only accumulate, so a stalled pass identifies steps that cannot run
// from an empty trace under the current preconditions and safety rules. This
// checks declared transitions; it never executes tools or proves external facts.
func diagnoseWorkflow(wf *verify.Workflow, maxChecks int) *checkDiagnostics {
	d := &checkDiagnostics{RunnableOrder: []string{}, Blocked: []tryStep{}}
	pending := orderedSteps(wf)
	// Index producers once: stalled steps may be revisited on many passes.
	establishers := map[verify.State][]string{}
	for _, step := range pending {
		seen := map[verify.State]bool{}
		for _, state := range step.Establishes {
			if !seen[state] {
				establishers[state] = append(establishers[state], string(step.ID))
				seen[state] = true
			}
		}
	}
	for _, ids := range establishers {
		sort.Strings(ids)
	}
	trace := verify.NewTrace()
	checks := 0
	for len(pending) > 0 {
		var next []verify.Step
		blocked := []tryStep{}
		for _, step := range pending {
			if checks >= maxChecks {
				d.Error = fmt.Sprintf("diagnostic limit reached after %d checks; runnable order is partial", checks)
				return d
			}
			checks++
			err := wf.CheckAndCommit(trace, step.ID)
			if err == nil {
				d.RunnableOrder = append(d.RunnableOrder, string(step.ID))
				continue
			}
			var v *verify.Violation
			if !errors.As(err, &v) {
				d.Error = err.Error()
				return d
			}
			st := tryStep{Step: string(step.ID), Decision: "blocked", Reason: v.Message, Next: verify.NextStopAndAsk}
			st.RunFirst = establishers[v.MissingState]
			if len(st.RunFirst) > 0 {
				st.Next = verify.NextRunEstablishingSteps
			}
			blocked = append(blocked, st)
			next = append(next, step)
		}
		if len(next) == len(pending) {
			d.Blocked = blocked
			break
		}
		pending = next
	}
	return d
}

// runCheck implements "sop-mcp-server check <file>": it loads the file and
// prints what the barrier will enforce. With --json it prints the same as one
// JSON document, including the error for a file that does not load, so a script
// can read the reason. It returns the process exit code.
func runCheck(args []string, out, errw io.Writer) int {
	if isHelp(args) {
		fmt.Fprint(out, checkUsage)
		return 0
	}
	args, asJSON := popFlag(args, "--json")
	args, diagnose := popFlag(args, "--diagnose")
	if len(args) != 1 {
		fmt.Fprint(errw, checkUsage)
		return 2
	}
	store, names, err := loadRunbooks(args[0])
	if err != nil {
		if asJSON {
			emitJSON(out, errw, checkResult{Valid: false, File: args[0], Error: err.Error()}, 1)
			return 1
		}
		fmt.Fprintln(errw, "check:", err)
		return 1
	}

	diagnostics := map[string]*checkDiagnostics{}
	code := 0
	if diagnose {
		for _, name := range names {
			wf, _ := store.Workflow(name)
			d := diagnoseWorkflow(wf, 100000)
			diagnostics[name] = d
			if len(d.Blocked) > 0 || d.Error != "" {
				code = 1
			}
		}
	}

	if asJSON {
		res := checkResult{Valid: true, File: args[0]}
		for _, name := range names {
			wf, _ := store.Workflow(name)
			cw := checkWorkflow{Name: name, Diagnostics: diagnostics[name], Steps: []checkStep{}, Safety: []checkRule{}, Reachability: []checkReach{}}
			for _, s := range orderedSteps(wf) {
				cw.Steps = append(cw.Steps, checkStep{ID: string(s.ID), Requires: sortedStates(s.Requires), Establishes: sortedStates(s.Establishes)})
			}
			for _, r := range wf.Safety {
				cw.Safety = append(cw.Safety, checkRule{Name: r.Name, Forbidden: string(r.Forbidden), Requires: string(r.Requires)})
			}
			for _, r := range wf.Reachability {
				cw.Reachability = append(cw.Reachability, checkReach{Name: r.Name, Target: string(r.Target)})
			}
			res.Workflows = append(res.Workflows, cw)
		}
		return emitJSON(out, errw, res, code)
	}

	noun := "workflows"
	if len(names) == 1 {
		noun = "workflow"
	}
	fmt.Fprintf(out, "%s loads: %d %s.\n", args[0], len(names), noun)
	for _, name := range names {
		wf, _ := store.Workflow(name)
		fmt.Fprintf(out, "\n%s\n", name)
		for _, s := range orderedSteps(wf) {
			fmt.Fprintf(out, "  %-24s %s\n", s.ID, needs(s))
		}
		for _, r := range wf.Safety {
			fmt.Fprintf(out, "  rule %s: %q cannot be reached unless %q is established first\n", r.Name, r.Forbidden, r.Requires)
		}
		for _, r := range wf.Reachability {
			fmt.Fprintf(out, "  reachability %s: %q must stay reachable from every state\n", r.Name, r.Target)
		}

		if d := diagnostics[name]; d != nil {
			fmt.Fprintf(out, "  runnable order: %s\n", strings.Join(d.RunnableOrder, " -> "))
			for _, st := range d.Blocked {
				fmt.Fprintf(out, "  BLOCKED %s: %s\n", st.Step, st.Reason)
				if len(st.RunFirst) > 0 {
					fmt.Fprintf(out, "    run first: %s (also blocked)\n", strings.Join(st.RunFirst, ", "))
				}
			}
			if d.Error != "" {
				fmt.Fprintf(out, "  diagnostics incomplete: %s\n", d.Error)
			}
		}
	}
	return code
}

// sortedStates returns states as sorted strings, never nil, so the JSON shows
// an empty list rather than null.
func sortedStates(in []verify.State) []string {
	out := make([]string, len(in))
	for i, st := range in {
		out[i] = string(st)
	}
	sort.Strings(out)
	return out
}

// writeJSON writes v as indented JSON and returns the write error, so a closed
// pipe or a full disk is a failed command and not silent, truncated output.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// emitJSON is writeJSON for a command: it reports a write failure on errw and
// turns it into the exit code. ok is the code to return when the write works.
func emitJSON(out, errw io.Writer, v any, ok int) int {
	if err := writeJSON(out, v); err != nil {
		fmt.Fprintln(errw, "check: could not write the result:", err)
		return 1
	}
	return ok
}

// orderedSteps lists the steps that need nothing first, then the rest, each
// group by ID, so the output does not depend on map order.
func orderedSteps(wf *verify.Workflow) []verify.Step {
	steps := make([]verify.Step, 0, len(wf.Steps))
	for _, s := range wf.Steps {
		steps = append(steps, s)
	}
	sort.Slice(steps, func(i, j int) bool {
		a, b := steps[i], steps[j]
		if (len(a.Requires) == 0) != (len(b.Requires) == 0) {
			return len(a.Requires) == 0
		}
		return a.ID < b.ID
	})
	return steps
}

func needs(s verify.Step) string {
	if len(s.Requires) == 0 {
		return "can run first"
	}
	req := make([]string, len(s.Requires))
	for i, r := range s.Requires {
		req[i] = string(r)
	}
	sort.Strings(req)
	return "needs " + strings.Join(req, ", ")
}
