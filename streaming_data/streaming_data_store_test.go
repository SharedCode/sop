package streaming_data

import (
	"context"
	"testing"

	"github.com/SharedCode/sop/in_red_ck"
)

var ctx = context.Background()

func Test_StreamingDataStoreBasicUse(t *testing.T) {
	trans, _ := in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds := NewStreamingDataStore[string](ctx, "fooStore", trans)
	encoder, _ := sds.Add(ctx, "fooVideo")
	for i:= 0; i < 10; i ++ {
		encoder.Encode("a huge chunk, about 10MB.")
	}
	trans.Commit(ctx)
}
