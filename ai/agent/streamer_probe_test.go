package agent

import (
  "bytes"
  "context"
  "testing"
)

func TestStreamerProbe(t *testing.T) {
  buf := &bytes.Buffer{}
  streamer := NewNDJSONStreamer(buf)
  streamer.SetFlush(true)
  ctx := context.WithValue(context.Background(), CtxKeyJSONStreamer, streamer)
  res, err := serializeResult(ctx, &MockCursor{Items: []any{map[string]any{"id":1}}})
  if err != nil { t.Fatal(err) }
  t.Logf("RES=%s", res)
  t.Logf("BUF=%s", buf.String())
}
