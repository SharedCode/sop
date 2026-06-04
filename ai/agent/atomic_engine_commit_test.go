package agent

import (
	"context"
	"strings"
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

func TestStreamAndDrainCursors_PrefersOutputCursorOnly(t *testing.T) {
	engine := &ScriptEngine{Context: NewScriptContext()}
	outputCursor := &trackingCursor{items: []any{map[string]any{"orders.total_amount": 831}}}
	intermediateCursor := &trackingCursor{items: []any{map[string]any{"users.first_name": "John"}}}

	engine.Context.Variables["output"] = outputCursor
	engine.Context.Variables["joined_orders"] = intermediateCursor
	engine.Context.LastUpdatedVar = "joined_orders"

	// Add mock streamer to test streaming behavior
	var buf strings.Builder
	streamer := NewNDJSONStreamer(&buf)
	ctx := context.WithValue(context.Background(), CtxKeyJSONStreamer, streamer)

	if err := engine.streamAndDrainCursorsBeforeCommit(ctx); err != nil {
		t.Fatalf("streamAndDrainCursorsBeforeCommit failed: %v", err)
	}

	// After streaming, cursor is replaced with summary object
	summary, ok := engine.Context.Variables["output"].(map[string]any)
	if !ok {
		t.Fatalf("expected output cursor to be replaced with summary, got %#v", engine.Context.Variables["output"])
	}
	if summary["streamed"] != true || summary["rows"] != 1 {
		t.Fatalf("expected summary {streamed: true, rows: 1}, got %#v", summary)
	}
	if outputCursor.nextCalls == 0 || !outputCursor.closed {
		t.Fatalf("expected output cursor to be drained and closed, got nextCalls=%d closed=%v", outputCursor.nextCalls, outputCursor.closed)
	}
	if intermediateCursor.nextCalls != 0 || intermediateCursor.closed {
		t.Fatalf("expected intermediate cursor to remain untouched, got nextCalls=%d closed=%v", intermediateCursor.nextCalls, intermediateCursor.closed)
	}
}

func TestStreamAndDrainCursors_FallsBackToLastUpdatedCursor(t *testing.T) {
	engine := &ScriptEngine{Context: NewScriptContext()}
	joinedCursor := &trackingCursor{items: []any{map[string]any{"orders.total_amount": 831}}}

	engine.Context.Variables["joined_orders"] = joinedCursor
	engine.Context.LastUpdatedVar = "joined_orders"

	// Add mock streamer to test streaming behavior
	var buf strings.Builder
	streamer := NewNDJSONStreamer(&buf)
	ctx := context.WithValue(context.Background(), CtxKeyJSONStreamer, streamer)

	if err := engine.streamAndDrainCursorsBeforeCommit(ctx); err != nil {
		t.Fatalf("streamAndDrainCursorsBeforeCommit failed: %v", err)
	}

	// After streaming, cursor is replaced with summary object
	summary, ok := engine.Context.Variables["joined_orders"].(map[string]any)
	if !ok {
		t.Fatalf("expected last updated cursor to be replaced with summary, got %#v", engine.Context.Variables["joined_orders"])
	}
	if summary["streamed"] != true || summary["rows"] != 1 {
		t.Fatalf("expected summary {streamed: true, rows: 1}, got %#v", summary)
	}
	if joinedCursor.nextCalls == 0 || !joinedCursor.closed {
		t.Fatalf("expected last updated cursor to be drained and closed, got nextCalls=%d closed=%v", joinedCursor.nextCalls, joinedCursor.closed)
	}
}

func TestStreamAndDrainCursors_MaterializesToArrayWhenNoStreamer(t *testing.T) {
	engine := &ScriptEngine{Context: NewScriptContext()}
	cursor := &trackingCursor{items: []any{
		map[string]any{"id": 1, "name": "Alice"},
		map[string]any{"id": 2, "name": "Bob"},
	}}

	engine.Context.Variables["output"] = cursor

	// No streamer in context - should materialize to array
	ctx := context.Background()

	if err := engine.streamAndDrainCursorsBeforeCommit(ctx); err != nil {
		t.Fatalf("streamAndDrainCursorsBeforeCommit failed: %v", err)
	}

	// Without streamer, cursor is materialized to array (backward compatible)
	materialized, ok := engine.Context.Variables["output"].([]any)
	if !ok {
		t.Fatalf("expected output cursor to be materialized to array, got %#v", engine.Context.Variables["output"])
	}
	if len(materialized) != 2 {
		t.Fatalf("expected 2 items in array, got %d", len(materialized))
	}
	if cursor.nextCalls == 0 || !cursor.closed {
		t.Fatalf("expected cursor to be drained and closed, got nextCalls=%d closed=%v", cursor.nextCalls, cursor.closed)
	}
}
