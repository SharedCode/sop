// Package nats is an optional, opt-in bridge that publishes verify
// barrier decisions to a NATS subject so an external system (a dashboard,
// a SIEM, another service in a team's stack that already runs NATS) can
// observe them without joltrin's embedded core ever depending on NATS or
// requiring it to function.
//
// This package changes nothing about verify. VerifyBridge wraps a
// *verify.Workflow and calls straight through to the same-named method on
// it; the barrier's decision (allow or block) is exactly what
// verify.Workflow would have returned on its own. Publishing to NATS is
// a side effect on the way out, not a precondition for the barrier to
// work: a caller that never imports this package, or that imports it but
// never wires it in, sees no behavior change and no new dependency.
//
// Nothing here touches storage. VerifyBridge only ever calls verify
// methods (CheckSafety, CheckAndCommit, CheckAndCommitIdempotent), which
// hold an in-memory *verify.Trace lock and do not read or write the
// B-Tree. There is no code path from this package into btree, inmemory,
// fs, or any transaction path.
//
// Typical use: a server that already owns a *verify.Workflow and a
// *nats.Conn (tools/mcpserver and tools/a2aagent are two examples in this
// repo, see their use of verify directly) wraps the workflow once at
// startup:
//
//	nc, _ := natsgo.Connect(natsgo.DefaultURL)
//	bridge := nats.NewVerifyBridge(wf, nc, "prod-db-rollout")
//	// use bridge.CheckAndCommit in place of wf.CheckAndCommit
//
// and every barrier decision made through bridge is also published as a
// BarrierDecision event to bridge's subject. See examples/verify_barrier_nats
// for a full runnable demonstration, including a subscriber.
//
// # JetStream
//
// Plain NATS drops an event nobody is listening for. For decisions that must
// be kept and replayed, opt in with WithJetStream, which stores each decision
// in a stream, and read them back with SubscribeJetStream, a durable pull
// consumer with explicit acknowledgement:
//
//	bridge, err := nats.NewVerifyBridge(wf, nc, "prod-db-rollout").
//		WithJetStream(ctx, nats.JetStreamOptions{})
//	// later, possibly after a restart:
//	err = nats.SubscribeJetStream(ctx, nc, bridge.Subject(),
//		nats.ConsumerOptions{Durable: "audit"}, func(ev nats.BarrierDecision) error { ... })
//
// The decision is made and returned before any publish, and a JetStream
// failure is reported only through OnPublishError. A caller that never calls
// WithJetStream gets the plain NATS behavior described above, unchanged.
package nats
