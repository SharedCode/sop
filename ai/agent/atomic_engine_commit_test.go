package agent

import (
	"context"
	"testing"
)

type trackingCursor struct {
	items     []any
	index     int
	nextCalls int
	closed    bool
}

func (c *trackingCursor) Next(ctx context.Context) (any, bool, error) {
	c.nextCalls++
	if c.index >= len(c.items) {
		return nil, false, nil
	}
	item := c.items[c.index]
	c.index++
	return item, true, nil
}

func (c *trackingCursor) Close() error {
	c.closed = true
	return nil
}

func TestMaterializeCommitOutput_PrefersOutputCursorOnly(t *testing.T) {
	engine := &ScriptEngine{Context: NewScriptContext()}
	outputCursor := &trackingCursor{items: []any{map[string]any{"orders.total_amount": 831}}}
	intermediateCursor := &trackingCursor{items: []any{map[string]any{"users.first_name": "John"}}}

	engine.Context.Variables["output"] = outputCursor
	engine.Context.Variables["joined_orders"] = intermediateCursor
	engine.Context.LastUpdatedVar = "joined_orders"

	if err := engine.materializeCommitOutput(context.Background()); err != nil {
		t.Fatalf("materializeCommitOutput failed: %v", err)
	}

	materialized, ok := engine.Context.Variables["output"].([]any)
	if !ok || len(materialized) != 1 {
		t.Fatalf("expected output cursor to materialize, got %#v", engine.Context.Variables["output"])
	}
	if outputCursor.nextCalls == 0 || !outputCursor.closed {
		t.Fatalf("expected output cursor to be drained and closed, got nextCalls=%d closed=%v", outputCursor.nextCalls, outputCursor.closed)
	}
	if intermediateCursor.nextCalls != 0 || intermediateCursor.closed {
		t.Fatalf("expected intermediate cursor to remain untouched, got nextCalls=%d closed=%v", intermediateCursor.nextCalls, intermediateCursor.closed)
	}
}

func TestMaterializeCommitOutput_FallsBackToLastUpdatedCursor(t *testing.T) {
	engine := &ScriptEngine{Context: NewScriptContext()}
	joinedCursor := &trackingCursor{items: []any{map[string]any{"orders.total_amount": 831}}}

	engine.Context.Variables["joined_orders"] = joinedCursor
	engine.Context.LastUpdatedVar = "joined_orders"

	if err := engine.materializeCommitOutput(context.Background()); err != nil {
		t.Fatalf("materializeCommitOutput failed: %v", err)
	}

	materialized, ok := engine.Context.Variables["joined_orders"].([]any)
	if !ok || len(materialized) != 1 {
		t.Fatalf("expected last updated cursor to materialize, got %#v", engine.Context.Variables["joined_orders"])
	}
	if joinedCursor.nextCalls == 0 || !joinedCursor.closed {
		t.Fatalf("expected last updated cursor to be drained and closed, got nextCalls=%d closed=%v", joinedCursor.nextCalls, joinedCursor.closed)
	}
}
