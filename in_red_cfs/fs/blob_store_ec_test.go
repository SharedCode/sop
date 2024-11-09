package fs

import (
	"bytes"
	"context"
	"fmt"

	"testing"

	"github.com/SharedCode/sop"
)

var ctx context.Context = context.Background()

func TestECAddThenRead(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	ba, err := bs.GetOne(ctx, "b1", id)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(ba, eba) {
		t.Errorf("got %v, expected %v", ba, eba)
	}
}

func TestECAddRemoveRead(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	bs.Remove(ctx, sop.BlobsPayload[sop.UUID]{
		BlobTable: "b1",
		Blobs:     []sop.UUID{id},
	})

	_, err := bs.GetOne(ctx, "b1", id)
	if err == nil {
		t.Error("GetOne succeeded, expected to fail")
	}
}

func TestECerrorOnAdd(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	id2 := sop.NewUUID()
	eba2 := []byte{1, 2, 3}
	fileIO.errorOnSuffixNumber = 1
	err := bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id2,
				Value: eba2,
			},
		},
	})
	if err == nil {
		t.Error("got nil, expected error")
	}
}

func TestECerrorOnRemove(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	id2 := sop.NewUUID()
	eba2 := []byte{1, 2, 3}
	//fileIO.errorOnSuffixNumber = 1
	err := bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id2,
				Value: eba2,
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	fileIO.errorOnSuffixNumber = 1
	err = bs.Remove(ctx, sop.BlobsPayload[sop.UUID]{
		BlobTable: "b1",
		Blobs:     []sop.UUID{id},
	})
	if err == nil {
		t.Error("got nil, expected error")
	}
}

func TestECerrorOnReadButReconstructed(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	id2 := sop.NewUUID()
	eba2 := []byte{1, 2, 3}
	err := bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id2,
				Value: eba2,
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	fileIO.errorOnSuffixNumber = 1
	ba, _ := bs.GetOne(ctx, "b1", id)
	if !bytes.Equal(ba, eba) {
		t.Errorf("got %v, expected %v", ba, eba)
	}
}

func TestECerrorOnReadNotReconstructed(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	id2 := sop.NewUUID()
	eba2 := []byte{1, 2, 3}
	err := bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id2,
				Value: eba2,
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	fileIO.errorOnSuffixNumber = 1
	fileIO.errorOnSuffixNumber2 = 0
	_, err = bs.GetOne(ctx, "b1", id)
	if err == nil {
		t.Error("got nil, expected error")
	}
}

func TestECerrorOnRepair(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
		RepairCorruptedShards: true,
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)
	id := sop.NewUUID()
	eba := []byte{1, 2, 3}
	bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id,
				Value: eba,
			},
		},
	})

	id2 := sop.NewUUID()
	eba2 := []byte{1, 2, 3}
	err := bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "b1",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{
				Key:   id2,
				Value: eba2,
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	fileIO.errorOnSuffixNumber = 1
	ba, err := bs.GetOne(ctx, "b1", id)
	// GetOne will still succeed, but warning should be logged.
	if err != nil {
		t.Error("got err, expected nil")
	}
	if !bytes.Equal(ba, eba) {
		t.Errorf("got %v, expected %v", ba, eba)
	}
}

func TestThreadedECerrorOnReadButReconstructed(t *testing.T) {
	ec := &ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"disk1",
			"disk2",
			"disk3",
		},
	}
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreExt(fileIO, ec)

	tr := sop.NewTaskRunner(ctx, 5)

	const iterations = 500

	task := func() error {
		fileIO.errorOnSuffixNumber = -1
		fileIO.errorOnSuffixNumber2 = -1
		id := sop.NewUUID()
		eba := []byte{1, 2, 3}
		bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
			BlobTable: "b1",
			Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
				{
					Key:   id,
					Value: eba,
				},
			},
		})

		id2 := sop.NewUUID()
		eba2 := []byte{1, 2, 3}
		bs.Add(ctx, sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
			BlobTable: "b1",
			Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
				{
					Key:   id2,
					Value: eba2,
				},
			},
		})

		fileIO.errorOnSuffixNumber = 1
		ba, _ := bs.GetOne(ctx, "b1", id)
		if !bytes.Equal(ba, eba) {
			err := fmt.Errorf("got %v, expected %v", ba, eba)
			return err
		}
		return nil
	}

	for i := 0; i < iterations; i++ {
		tr.Go(task)
	}

	if err := tr.Wait(); err != nil {
		t.Error(err)
	}
}