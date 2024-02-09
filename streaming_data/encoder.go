package streaming_data

import (
	"context"
	"fmt"
	"encoding/json"

	"github.com/SharedCode/sop/btree"
)

// An Encoder writes JSON values to an output stream by delegating to JSON Encoder.
type Encoder[TK btree.Comparable] struct {
	jsonEncoder *json.Encoder
	w *writer[TK]
}

func newEncoder[TK btree.Comparable](ctx context.Context, w *writer[TK]) *Encoder[TK] {
	return &Encoder[TK]{
		jsonEncoder: json.NewEncoder(w),
		w: w,
	}
}

// Close is only useful for Update/UpdateCurrentItem. It allows StreamingDataStore
// to do any house cleanup if needed. Not necessary for Add/AddIfNotExists methods
// of StreamingDataStore.
//
// Example, on Update/UpdateCurrentItem, store will ensure to cleanup or delete
// any chunks that were not replaced by the encoder/writer.
func (e *Encoder[TK]) Close() error {
	// Don't do anything if in add mode.
	if e.w.addOrUpdate {
		return nil
	}
	for {
		found, err := e.w.btree.FindOne(e.w.ctx, streamingDataKey[TK]{key: e.w.key, chunkIndex: e.w.chunkIndex}, false)
		if err != nil {
			return err
		}
		if !found {
			break
		}
		// Delete if found because it means the chunk is of the previous record that got updated.
		ok, err := e.w.btree.RemoveCurrentItem(e.w.ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("Update's Close failed, key: %v, chunk: #%d did not remove in the backend Btree", e.w.key, e.w.chunkIndex)
		}
		e.w.chunkIndex++
	}
	return nil
}

// Encode writes the JSON encoding of v to the stream,
// followed by a newline character.
//
// See the documentation for Marshal for details about the
// conversion of Go values to JSON.
func (e *Encoder[TK]) Encode(v any) error {
	return e.jsonEncoder.Encode(v)
}

// SetIndent instructs the encoder to format each subsequent encoded
// value as if indented by the package-level function Indent(dst, src, prefix, indent).
// Calling SetIndent("", "") disables indentation.
func (e *Encoder[TK]) SetIndent(prefix, indent string) {
	e.jsonEncoder.SetIndent(prefix, indent)
}

// SetEscapeHTML specifies whether problematic HTML characters
// should be escaped inside JSON quoted strings.
// The default behavior is to escape &, <, and > to \u0026, \u003c, and \u003e
// to avoid certain safety problems that can arise when embedding JSON in HTML.
//
// In non-HTML settings where the escaping interferes with the readability
// of the output, SetEscapeHTML(false) disables this behavior.
func (e *Encoder[TK]) SetEscapeHTML(on bool) {
	e.jsonEncoder.SetEscapeHTML(on)
}
