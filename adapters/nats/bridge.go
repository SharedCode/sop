package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/sharedcode/joltrin/v5/verify"
)

// BarrierDecision is the structured event VerifyBridge publishes to NATS
// every time it runs a barrier check. It carries enough detail for an
// external subscriber to reconstruct what happened without re-deriving it
// from the workflow graph: which step, which workflow, whether it was
// allowed, and if not, which rule and which missing state blocked it.
type BarrierDecision struct {
	Time     time.Time     `json:"time"`
	Workflow string        `json:"workflow,omitempty"`
	Step     verify.StepID `json:"step"`
	// Allowed is true when the step passed the barrier (CheckSafety
	// returned nil). It is false both for a blocked *verify.Violation and
	// for a malformed request (an unknown step), Rule/MissingState
	// distinguish the two: a Violation always sets Rule, a malformed
	// request never does.
	Allowed bool `json:"allowed"`
	// Replayed is true when the decision came from
	// CheckAndCommitIdempotent's idempotency cache rather than a fresh
	// check, i.e. this event describes a retried call, not a new one.
	Replayed bool `json:"replayed,omitempty"`
	// Rule is the SafetyRule.Name that blocked this step, or the literal
	// "precondition" when the step's own precondition was not met. Empty
	// when Allowed is true or the request was malformed.
	Rule string `json:"rule,omitempty"`
	// MissingState is the State that still needs to be established before
	// this step can pass, taken from verify.Violation.MissingState. Empty
	// when Allowed is true or the request was malformed.
	MissingState verify.State `json:"missing_state,omitempty"`
	// Reason is the blocking error's message, verbatim, for a human
	// reading the event directly. Empty when Allowed is true.
	Reason string `json:"reason,omitempty"`
}

// DefaultSubject is the NATS subject a VerifyBridge publishes to when
// constructed with an empty workflow name.
const DefaultSubject = "joltrin.verify.decision"

// SubjectFor returns the NATS subject a VerifyBridge publishes
// BarrierDecision events to for the given workflow name. An empty name
// returns DefaultSubject.
func SubjectFor(workflow string) string {
	if workflow == "" {
		return DefaultSubject
	}
	return fmt.Sprintf("joltrin.verify.%s.decision", workflow)
}

// VerifyBridge wraps a *verify.Workflow and publishes a BarrierDecision
// event to NATS after every barrier check it runs. It does not change
// verify's behavior: CheckSafety, CheckAndCommit, and
// CheckAndCommitIdempotent each call straight through to the identically
// named *verify.Workflow method and return its exact result unchanged; the
// NATS publish is a side effect on the way out. A publish failure is never
// allowed to change or block the barrier's own decision, see publish
// below.
//
// A VerifyBridge is only ever a decorator a caller opts into at its own
// call sites. Nothing in verify, tools/mcpserver, or tools/a2aagent
// constructs or requires one.
type VerifyBridge struct {
	wf       *verify.Workflow
	nc       *nats.Conn
	workflow string
	subject  string

	// onPublishError, if set, is called with any error returned by the
	// underlying NATS publish. Optional: a VerifyBridge with no handler set
	// simply drops a publish failure, exactly like fire-and-forget metrics
	// or logging, rather than letting it surface as a barrier error.
	onPublishError func(error)

	// js is set only by WithJetStream. When nil, publish uses core NATS
	// exactly as before.
	js jetstream.JetStream
}

// NewVerifyBridge returns a VerifyBridge that runs barrier checks against
// wf and publishes a BarrierDecision for each one to nc, on the subject
// SubjectFor(workflowName). workflowName is a label only, used to build the
// subject and to tag published events; it does not have to match any name
// wf is registered under in a tools/runbookstore.Store.
func NewVerifyBridge(wf *verify.Workflow, nc *nats.Conn, workflowName string) *VerifyBridge {
	return &VerifyBridge{
		wf:       wf,
		nc:       nc,
		workflow: workflowName,
		subject:  SubjectFor(workflowName),
	}
}

// OnPublishError sets a handler called with any error the underlying NATS
// publish returns, and returns the bridge for chaining. Optional; without
// one, a publish failure is dropped silently.
func (b *VerifyBridge) OnPublishError(fn func(error)) *VerifyBridge {
	b.onPublishError = fn
	return b
}

// Subject returns the NATS subject this bridge publishes to.
func (b *VerifyBridge) Subject() string {
	return b.subject
}

// CheckSafety runs wf.CheckSafety(trace, next), publishes the resulting
// BarrierDecision, and returns wf.CheckSafety's exact result.
func (b *VerifyBridge) CheckSafety(trace *verify.Trace, next verify.StepID) error {
	err := b.wf.CheckSafety(trace, next)
	b.publish(next, false, "", err)
	return err
}

// CheckAndCommit runs wf.CheckAndCommit(trace, next), publishes the
// resulting BarrierDecision, and returns wf.CheckAndCommit's exact result.
func (b *VerifyBridge) CheckAndCommit(trace *verify.Trace, next verify.StepID) error {
	err := b.wf.CheckAndCommit(trace, next)
	b.publish(next, false, "", err)
	return err
}

// CheckAndCommitIdempotent runs
// wf.CheckAndCommitIdempotent(trace, next, idempotencyKey), publishes the
// resulting BarrierDecision (with Replayed set from the same call), and
// returns wf.CheckAndCommitIdempotent's exact result.
func (b *VerifyBridge) CheckAndCommitIdempotent(trace *verify.Trace, next verify.StepID, idempotencyKey string) (replayed bool, err error) {
	replayed, err = b.wf.CheckAndCommitIdempotent(trace, next, idempotencyKey)
	b.publish(next, replayed, idempotencyKey, err)
	return replayed, err
}

func (b *VerifyBridge) publish(step verify.StepID, replayed bool, idempotencyKey string, err error) {
	ev := BarrierDecision{
		Time:     time.Now().UTC(),
		Workflow: b.workflow,
		Step:     step,
		Allowed:  err == nil,
		Replayed: replayed,
	}
	if err != nil {
		ev.Reason = err.Error()
		var violation *verify.Violation
		if errors.As(err, &violation) {
			ev.Rule = violation.Rule
			ev.MissingState = violation.MissingState
		}
	}

	data, err := json.Marshal(ev)
	if err != nil {
		// A BarrierDecision is a fixed, JSON-safe shape; this would only
		// fail if that shape changes to include something unmarshalable.
		// Report it the same way a publish failure is reported, rather than
		// silently dropping a bug.
		if b.onPublishError != nil {
			b.onPublishError(fmt.Errorf("nats: marshal barrier decision: %w", err))
		}
		return
	}

	if b.js != nil {
		b.publishJetStream(data, idempotencyKey)
		return
	}

	if err := b.nc.Publish(b.subject, data); err != nil && b.onPublishError != nil {
		b.onPublishError(fmt.Errorf("nats: publish barrier decision: %w", err))
	}
}

// DefaultStream is the JetStream stream WithJetStream uses when
// JetStreamOptions.Stream is empty. It captures every subject a
// VerifyBridge publishes to, whatever the workflow name.
const DefaultStream = "JOLTRIN_VERIFY"

// defaultStreamSubjects is the subject filter DefaultStream is created with.
const defaultStreamSubjects = "joltrin.verify.>"

// JetStreamOptions configures the stream WithJetStream creates or checks.
// The zero value is valid.
type JetStreamOptions struct {
	// Stream is the stream name. Empty means DefaultStream.
	Stream string
	// Replicas is the replica count used only when the stream is created.
	// Zero means 1.
	Replicas int
	// MaxAge, if set, is how long the stream keeps a decision. Used only
	// when the stream is created. Zero keeps decisions until removed.
	MaxAge time.Duration
	// Duplicates is the window in which a repeated Nats-Msg-Id is stored
	// once. Used only when the stream is created. Zero means the server
	// default (two minutes).
	Duplicates time.Duration
}

// WithJetStream makes the bridge publish each BarrierDecision to a
// JetStream stream instead of plain NATS, so decisions are stored and a
// consumer that was offline can read them later (see SubscribeJetStream).
// It returns the bridge for chaining.
//
// The stream is created if it does not exist. If it exists, it is checked
// to capture the bridge's subject and is otherwise left untouched; a
// mismatch is an error rather than a silent change to someone else's
// stream.
//
// Publishing stays off the barrier's path. The decision is made first and
// returned unchanged, and the publish is asynchronous: a failure, including
// a missing stream or a lost acknowledgement, is reported through
// OnPublishError and never to the caller of CheckSafety, CheckAndCommit, or
// CheckAndCommitIdempotent. Call Flush before shutdown to wait for pending
// acknowledgements.
//
// CheckAndCommitIdempotent publishes with a Nats-Msg-Id built from the
// workflow, step, and idempotency key, so a retry is stored once inside the
// stream's duplicate window. CheckSafety and CheckAndCommit have no
// caller-supplied identity, so their decisions are published without an ID
// and are not de-duplicated.
func (b *VerifyBridge) WithJetStream(ctx context.Context, opts JetStreamOptions) (*VerifyBridge, error) {
	js, err := jetstream.New(b.nc, jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, _ *nats.Msg, err error) {
		if b.onPublishError != nil {
			b.onPublishError(fmt.Errorf("nats: jetstream publish barrier decision: %w", err))
		}
	}))
	if err != nil {
		return nil, fmt.Errorf("nats: open jetstream: %w", err)
	}

	name := opts.Stream
	if name == "" {
		name = DefaultStream
	}

	stream, err := js.Stream(ctx, name)
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		replicas := opts.Replicas
		if replicas == 0 {
			replicas = 1
		}
		subjects := []string{defaultStreamSubjects}
		if !subjectCovered(subjects, b.subject) {
			subjects = []string{b.subject}
		}
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       name,
			Subjects:   subjects,
			Replicas:   replicas,
			MaxAge:     opts.MaxAge,
			Duplicates: opts.Duplicates,
		}); err != nil {
			return nil, fmt.Errorf("nats: create stream %q: %w", name, err)
		}
	case err != nil:
		return nil, fmt.Errorf("nats: look up stream %q: %w", name, err)
	default:
		if !subjectCovered(stream.CachedInfo().Config.Subjects, b.subject) {
			return nil, fmt.Errorf("nats: stream %q does not capture subject %q", name, b.subject)
		}
	}

	b.js = js
	return b, nil
}

// Flush waits until every asynchronous JetStream publish has been
// acknowledged or has failed, or ctx is done. It does nothing for a bridge
// without WithJetStream.
func (b *VerifyBridge) Flush(ctx context.Context) error {
	if b.js == nil {
		return nil
	}
	select {
	case <-b.js.PublishAsyncComplete():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *VerifyBridge) publishJetStream(data []byte, idempotencyKey string) {
	var opts []jetstream.PublishOpt
	if idempotencyKey != "" {
		opts = append(opts, jetstream.WithMsgID(b.workflow+"/"+idempotencyKey))
	}
	if _, err := b.js.PublishMsgAsync(&nats.Msg{Subject: b.subject, Data: data}, opts...); err != nil && b.onPublishError != nil {
		b.onPublishError(fmt.Errorf("nats: jetstream publish barrier decision: %w", err))
	}
}

// subjectCovered reports whether subject matches any of the stream subject
// patterns, using NATS wildcards: "*" for one token and a trailing ">" for
// one or more.
func subjectCovered(patterns []string, subject string) bool {
	subjTokens := strings.Split(subject, ".")
	for _, p := range patterns {
		pt := strings.Split(p, ".")
		match := true
		for i, tok := range pt {
			if tok == ">" {
				match = i < len(subjTokens)
				break
			}
			if i >= len(subjTokens) || (tok != "*" && tok != subjTokens[i]) {
				match = false
				break
			}
			if i == len(pt)-1 && len(subjTokens) != len(pt) {
				match = false
			}
		}
		if match {
			return true
		}
	}
	return false
}
