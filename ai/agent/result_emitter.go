package agent

import (
	"context"
	"encoding/json"

	"github.com/sharedcode/sop/ai"
)

// ResultEmitter helps stream results to the UI or reduce them if no live streamer is present.
type ResultEmitter struct {
	streamer ai.ResultStreamer
	reducer  *resultReducer
}

func NewResultEmitter(ctx context.Context) *ResultEmitter {
	re := &ResultEmitter{}
	if sendEvent, ok := ctx.Value(ai.CtxKeyEventStreamer).(func(string, any)); ok && sendEvent != nil {
		streamer := NewEventResultStreamer(sendEvent)
		re.streamer = streamer
	} else if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok && s != nil {
		re.streamer = s
	} else {
		re.reducer = newResultReducer()
		re.streamer = &ReducingStreamer{reducer: re.reducer}
	}
	return re
}

func (re *ResultEmitter) Start() {
	re.streamer.BeginArray()
}

func (re *ResultEmitter) Emit(item any) {
	re.streamer.WriteItem(item)
}

func (re *ResultEmitter) SetColumns(cols []string) {
	re.streamer.SetMetadata(map[string]any{"columns": cols})
}

func (re *ResultEmitter) Finalize() string {
	re.streamer.EndArray()
	if re.reducer != nil {
		b, _ := json.MarshalIndent(re.reducer.Write(), "", "  ")
		return string(b)
	}
	return ""
}

// ReducingStreamer caps the buffered result payload as items are generated,
// which keeps the LLM-facing tool result bounded without holding the full row set in memory.
type ReducingStreamer struct {
	reducer *resultReducer
}

func (rs *ReducingStreamer) BeginArray()                     {}
func (rs *ReducingStreamer) EndArray()                       {}
func (rs *ReducingStreamer) SetMetadata(meta map[string]any) {}
func (rs *ReducingStreamer) WriteItem(item any) {
	rs.reducer.Append(item)
}

type FilteringStreamer struct {
	wrapped ai.ResultStreamer
	fields  []string
	limit   int
	count   int
}

func (fs *FilteringStreamer) BeginArray() {
	fs.wrapped.BeginArray()
}

func (fs *FilteringStreamer) SetMetadata(meta map[string]any) {
	fs.wrapped.SetMetadata(meta)
}

func (fs *FilteringStreamer) WriteItem(item any) {
	if fs.limit > 0 && fs.count >= fs.limit {
		return
	}

	var filtered any
	if len(fs.fields) > 0 {
		if mapItem, ok := item.(map[string]any); ok {
			filtered = filterFields(mapItem, fs.fields)
		} else {
			filtered = item
		}
	} else {
		filtered = item
	}

	fs.wrapped.WriteItem(filtered)
	fs.count++
}

func (fs *FilteringStreamer) EndArray() {
	fs.wrapped.EndArray()
}

// Stream writer that writes via callback function calls.

type EventResultStreamer struct {
	sendEvent func(string, any)
	closed    bool
}

func NewEventResultStreamer(sendEvent func(string, any)) *EventResultStreamer {
	return &EventResultStreamer{sendEvent: sendEvent}
}

func (s *EventResultStreamer) BeginArray() {
	if s == nil || s.sendEvent == nil {
		return
	}
	s.sendEvent("result_stream", map[string]any{"status": "start"})
}

func (s *EventResultStreamer) SetMetadata(meta map[string]any) {}

func (s *EventResultStreamer) WriteItem(item any) {
	if s == nil || s.sendEvent == nil {
		return
	}
	s.sendEvent("record", item)
}

func (s *EventResultStreamer) EndArray() {
	if s == nil || s.sendEvent == nil || s.closed {
		return
	}
	s.sendEvent("result_stream", map[string]any{"status": "done"})
	s.closed = true
}

func (s *EventResultStreamer) Close() {
	s.EndArray()
}
