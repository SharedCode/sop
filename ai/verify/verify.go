// Package verify checks a finite operational workflow (a runbook: a set of
// steps with preconditions and postconditions) against two classes of
// property before letting an agent execute a step:
//
//   - Safety (precedence): a forbidden state must never occur unless a
//     required state occurred first. Example: "the production database can
//     never be dropped unless a backup was already validated."
//   - Reachability: a target state must remain reachable from wherever
//     execution currently stands. Example: "a rollback path must always
//     exist, even after a partial failure."
//
// What this is: an explicit-state graph checker over a finite workflow.
// Safety checking is the "P is preceded by Q" pattern from Dwyer, Avrunin,
// and Corbett's property specification patterns (1999), the same
// specification family most LTL model checkers verify against; here it's
// checked directly against an accumulated state set rather than compiled
// into a temporal-logic formula and run through a model checker. Reachability
// checking is plain graph reachability (BFS) over the step graph.
//
// What this is not: a general-purpose LTL/CTL model checker. There is no
// temporal-operator parser, no Büchi automaton construction, no support for
// infinite-trace properties or probabilistic model checking. For a finite
// runbook with an enumerable set of states, that generality isn't needed to
// get a correct answer, an explicit precondition/postcondition graph is
// exactly the right level of formalism, and a much smaller trusted
// computing base to verify by reading the source.
package verify

import "fmt"

// State is a named fact that becomes true once some step establishes it.
// States are opaque strings from this package's point of view, e.g.
// "backup_validated" or "prod_db_dropped".
type State string

// StepID identifies one step in a Workflow.
type StepID string

// Step is one unit of a workflow. It requires a set of States to already
// hold (its precondition) and, once it completes, establishes a set of new
// States (its postcondition).
type Step struct {
	ID          StepID
	Requires    []State
	Establishes []State
}

// SafetyRule is a precedence property: Forbidden must never become
// established unless Requires was already established first.
type SafetyRule struct {
	Name      string
	Forbidden State
	Requires  State
}

// ReachabilityRule asserts that Target must remain reachable, via some
// sequence of steps, from every state in the workflow graph. Used to verify
// properties like "rollback is always reachable."
type ReachabilityRule struct {
	Name   string
	Target State
}

// Workflow is the full transition graph for one runbook: its steps, plus
// the safety and reachability properties every execution trace must satisfy.
type Workflow struct {
	Steps        map[StepID]Step
	Safety       []SafetyRule
	Reachability []ReachabilityRule
}

// NewWorkflow builds a Workflow from a step list, indexing steps by ID.
// Returns an error if two steps share an ID.
func NewWorkflow(steps []Step, safety []SafetyRule, reachability []ReachabilityRule) (*Workflow, error) {
	index := make(map[StepID]Step, len(steps))
	for _, s := range steps {
		if _, exists := index[s.ID]; exists {
			return nil, fmt.Errorf("verify: duplicate step ID %q", s.ID)
		}
		index[s.ID] = s
	}
	return &Workflow{Steps: index, Safety: safety, Reachability: reachability}, nil
}

// Trace is the ordered record of steps executed so far in one run of a
// Workflow, plus the accumulated set of States those steps established.
type Trace struct {
	Executed []StepID
	Holds    map[State]bool
}

// NewTrace starts an empty execution trace.
func NewTrace() *Trace {
	return &Trace{Holds: make(map[State]bool)}
}

// violation is returned by CheckSafety, giving the caller (an MCP tool
// handler, in this repo's case) a specific, actionable reason a step was
// blocked instead of a bare boolean.
type violation struct {
	Rule    string
	Message string
}

func (v *violation) Error() string { return v.Message }

// CheckSafety is the barrier certificate: before executing `next`, verify
// that doing so would not establish a Forbidden state without its Requires
// state already holding. Returns nil if `next` is safe to execute, or an
// error identifying which SafetyRule it would violate and why.
func (w *Workflow) CheckSafety(trace *Trace, next StepID) error {
	step, ok := w.Steps[next]
	if !ok {
		return fmt.Errorf("verify: unknown step %q", next)
	}

	// Precondition check: every state this step requires must already hold.
	for _, req := range step.Requires {
		if !trace.Holds[req] {
			return &violation{
				Rule:    "precondition",
				Message: fmt.Sprintf("step %q requires state %q, which has not been established in this trace", next, req),
			}
		}
	}

	// Safety check: for every state this step would establish, verify no
	// SafetyRule forbids establishing it without its Requires state already
	// holding.
	for _, est := range step.Establishes {
		for _, rule := range w.Safety {
			if rule.Forbidden != est {
				continue
			}
			if !trace.Holds[rule.Requires] {
				return &violation{
					Rule: rule.Name,
					Message: fmt.Sprintf(
						"step %q would establish forbidden state %q without required state %q first (rule: %s)",
						next, est, rule.Requires, rule.Name,
					),
				}
			}
		}
	}

	return nil
}

// Commit records that `next` executed successfully, advancing the trace:
// its established states now hold. Callers must call CheckSafety(trace,
// next) and get a nil error before calling Commit; Commit itself does not
// re-check safety, it assumes the caller already gated execution on it,
// exactly the barrier-certificate pattern: verify, then act, never act then
// verify.
func (w *Workflow) Commit(trace *Trace, next StepID) error {
	step, ok := w.Steps[next]
	if !ok {
		return fmt.Errorf("verify: unknown step %q", next)
	}
	trace.Executed = append(trace.Executed, next)
	for _, est := range step.Establishes {
		trace.Holds[est] = true
	}
	return nil
}
