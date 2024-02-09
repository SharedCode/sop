package streaming_data

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/SharedCode/sop/in_red_ck"
)

var ctx = context.Background()

func Test_StreamingDataStoreBasicUse(t *testing.T) {
	trans, _ := in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds := NewStreamingDataStore[string](ctx, "fooStore", trans)
	encoder, _ := sds.Add(ctx, "fooVideo")
	for i := 0; i < 10; i ++ {
		encoder.Encode("a huge chunk, about 10MB.")
	}
	trans.Commit(ctx)

	// Read back the data. Pass false on 2nd argument will toggle to a "reader" transaction.
	trans, _ = in_red_ck.NewMockTransaction(t, false, -1)
	trans.Begin()
	sds = NewStreamingDataStore[string](ctx, "fooStore", trans)

	ok, _ := sds.FindOne(ctx, "fooVideo", false)
	if !ok {
		t.Errorf("FindOne('fooVideo') failed, got not found, want found")
	}
	decoder, _ := sds.GetCurrentValue(ctx)
	var target string
	for i := 0; i < 10; i++ {
		if err := decoder.Decode(&target); err != nil && err != io.EOF{
			t.Error(err)
			break
		}
		fmt.Println(target)
	}
	// Commit on "reader" transaction will ensure that data you read did not change on entire
	// transaction session until commit time. If other transaction did change the data read,
	// Commit on the reader will return an error to reflect that data consistency conflict.
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Reader transaction commit failed, details: %v", err)
	}
}
