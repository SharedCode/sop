package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Subscribe subscribes to subject on nc and calls handler with each
// BarrierDecision received. It is a thin convenience wrapper around
// nc.Subscribe for the common case of consuming VerifyBridge's own output;
// nothing in this package requires a caller to use it, a NATS subscriber
// in any other language or client can consume SubjectFor's subject
// directly since BarrierDecision is plain JSON.
//
// A message that fails to unmarshal as a BarrierDecision is dropped rather
// than passed to handler or returned as an error, since one malformed
// message on a shared subject should not stop delivery of the rest; wrap
// nc.Subscribe directly instead if that behavior does not fit a caller's
// needs.
func Subscribe(nc *nats.Conn, subject string, handler func(BarrierDecision)) (*nats.Subscription, error) {
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var ev BarrierDecision
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			return
		}
		handler(ev)
	})
	if err != nil {
		return nil, fmt.Errorf("nats: subscribe to %q: %w", subject, err)
	}
	return sub, nil
}

// ConsumerOptions configures SubscribeJetStream. Durable is required; the
// other fields have defaults.
type ConsumerOptions struct {
	// Stream is the stream to read. Empty means DefaultStream.
	Stream string
	// Durable is the consumer name. The server remembers its position, so
	// a consumer that restarts with the same name resumes where it left
	// off.
	Durable string
	// AckWait is how long the server waits for an acknowledgement before
	// redelivering. Zero means 30 seconds.
	AckWait time.Duration
	// MaxDeliver caps delivery attempts per message. Zero means 5.
	MaxDeliver int
	// Batch is how many messages to request at a time. Zero means 10.
	Batch int
}

// SubscribeJetStream reads BarrierDecision events for subject from a
// JetStream stream through a durable pull consumer with explicit
// acknowledgement, and calls handler for each. It blocks until ctx is done
// and returns ctx.Err() then, or an earlier setup or fetch error.
//
// A message is acknowledged when handler returns nil. If handler returns an
// error the message is left unacknowledged, so the server redelivers it
// after AckWait, up to MaxDeliver attempts. A message that does not
// unmarshal as a BarrierDecision is terminated rather than redelivered,
// matching Subscribe's rule that one malformed message must not stop the
// rest.
//
// Because the consumer is durable, decisions published while no consumer
// was running are delivered when one starts.
func SubscribeJetStream(ctx context.Context, nc *nats.Conn, subject string, opts ConsumerOptions, handler func(BarrierDecision) error) error {
	if opts.Durable == "" {
		return errors.New("nats: SubscribeJetStream requires ConsumerOptions.Durable")
	}
	stream := opts.Stream
	if stream == "" {
		stream = DefaultStream
	}
	ackWait := opts.AckWait
	if ackWait == 0 {
		ackWait = 30 * time.Second
	}
	maxDeliver := opts.MaxDeliver
	if maxDeliver == 0 {
		maxDeliver = 5
	}
	batch := opts.Batch
	if batch == 0 {
		batch = 10
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("nats: open jetstream: %w", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       opts.Durable,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("nats: consumer %q on stream %q: %w", opts.Durable, stream, err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msgs, err := cons.Fetch(batch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("nats: fetch from consumer %q: %w", opts.Durable, err)
		}
		for msg := range msgs.Messages() {
			var ev BarrierDecision
			if err := json.Unmarshal(msg.Data(), &ev); err != nil {
				_ = msg.Term()
				continue
			}
			if err := handler(ev); err != nil {
				continue
			}
			_ = msg.Ack()
		}
		if err := msgs.Error(); err != nil && ctx.Err() == nil && !errors.Is(err, nats.ErrTimeout) {
			return fmt.Errorf("nats: fetch from consumer %q: %w", opts.Durable, err)
		}
	}
}
