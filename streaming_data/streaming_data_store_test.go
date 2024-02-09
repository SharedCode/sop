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
		encoder.Encode(fmt.Sprintf("%d. a huge chunk, about 10MB.", i))
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

func Test_StreamingDataStoreBigDataUpdate(t *testing.T) {
	// Upload the video.
	trans, _ := in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds := NewStreamingDataStore[string](ctx, "fooStore", trans)
	encoder, _ := sds.Add(ctx, "fooVideo2")
	for i := 0; i < 10; i ++ {
		encoder.Encode(fmt.Sprintf("%d. a huge chunk, about 10MB.", i))
	}
	trans.Commit(ctx)

	// Update the video.
	trans, _ = in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds = NewStreamingDataStore[string](ctx, "fooStore", trans)
	encoder, _ = sds.Update(ctx, "fooVideo2")
	chunkCount := 9
	for i := 0; i < chunkCount; i ++ {
		encoder.Encode(fmt.Sprintf("%d. a huge chunk, about 15MB.", i))
	}
	// Close the "update" encoder to cleanup mis-aligned chunks.
	encoder.Close()
	trans.Commit(ctx)

	// Read back the video.
	trans, _ = in_red_ck.NewMockTransaction(t, false, -1)
	trans.Begin()
	sds = NewStreamingDataStore[string](ctx, "fooStore", trans)

	ok, _ := sds.FindOne(ctx, "fooVideo2", false)
	if !ok {
		t.Errorf("FindOne('fooVideo') failed, got not found, want found")
	}
	decoder, _ := sds.GetCurrentValue(ctx)
	var target string

	for i := 0; i < 15; i++ {
		if i > chunkCount {
			t.Errorf("Failed decoding video, got %d want %d", i, chunkCount)
		}
		if err := decoder.Decode(&target); err != nil{
			if err == io.EOF {
				break
			}
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

func Test_StreamingDataStoreUpdateWithCountCheck(t *testing.T) {
	// Upload the video.
	trans, _ := in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds := NewStreamingDataStore[string](ctx, "fooStore2", trans)
	encoder, _ := sds.Add(ctx, "fooVideo1")
	encodeVideo(t, encoder, 50)
	trans.Commit(ctx)

	// Update the video.
	trans, _ = in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds = NewStreamingDataStore[string](ctx, "fooStore2", trans)
	encoder, _ = sds.Update(ctx, "fooVideo1")
	encodeVideo(t, encoder, 5)
	// Important to close the encoder, otherwise, cleanup will not happen.
	encoder.Close()

	if sds.Count() != 5 {
		t.Errorf("Failed Update, got %d, want %d", sds.Count(), 5)
	}
	trans.Commit(ctx)
}

func Test_StreamingDataStoreDelete(t *testing.T) {
	// Upload the video.
	trans, _ := in_red_ck.NewMockTransaction(t, true, -1)
	trans.Begin()
	sds := NewStreamingDataStore[string](ctx, "fooStore3", trans)

	encoder, _ := sds.Add(ctx, "fooVideo1")
	encodeVideo(t, encoder, 50)

	encoder, _ = sds.Add(ctx, "fooVideo2")
	encodeVideo(t, encoder, 5)

	encoder, _ = sds.Add(ctx, "fooVideo3")
	encodeVideo(t, encoder, 15)

	if ok, err := sds.Remove(ctx, "fooVideo2"); err != nil{
		if err != nil {
			t.Errorf("Failed Remove, details: %v", err)
		}
	} else if !ok {
		t.Error("Failed Remove, got false, want true")
	}

	if sds.Count() != 65 {
		t.Errorf("Failed Remove, got %d want %d", sds.Count(), 65)
	}

	trans.Commit(ctx)
}

func encodeVideo(t *testing.T, encoder *Encoder[string], count int) {
	for i := 0; i < count; i ++ {
		encoder.Encode("#%d. A huge chunk, about 20MB.")
	}
}