package erasure

import (
	"bytes"
	"testing"
)

func Test_Encode_Decode(t *testing.T) {
	e, _ := NewErasure(4, 2)
	d := []byte{1, 2, 3, 4, 5}
	shards, err := e.Encode(d)
	if err != nil {
		t.Error(err)
	}
	sm := make([][]byte, len(shards))
	var stuffZeroCount int
	for i := range shards {
		md, _ := e.ComputeShardMetadata(len(d), shards, 0)
		stuffZeroCount = int(md[0])
		sm[i] = md
	}
	if stuffZeroCount != 3 {
		t.Errorf("stuff 0 count got %d, expected 3", stuffZeroCount)
	}

	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Error(dr.Error)
	}

	if len(dr.DecodedData) != len(d) {
		t.Errorf("DecodedData got %d length, expected %d", len(dr.DecodedData), len(d))
	}
}

func Test_bitrot(t *testing.T) {
	e, _ := NewErasure(4, 2)
	d := []byte{1, 2, 3, 4, 5}
	shards, err := e.Encode(d)
	if err != nil {
		t.Error(err)
	}
	sm := make([][]byte, len(shards))
	for i := range shards {
		md, _ := e.ComputeShardMetadata(len(d), shards, i)
		sm[i] = md
	}

	// Change one byte to simulate bitrot on that location losing a byte.
	shards[1][1] = 0

	dr := e.Decode(shards, sm)
	if dr.Error != nil {
		t.Error(dr.Error)
	}
	if dr.ReconstructedShardsIndeces[0] != 1 {
		t.Errorf("ReconstructedShardsIndeces got %v, expected 1", dr.ReconstructedShardsIndeces[0])
	}

	d = []byte{1, 2, 3, 4, 5}
	if !bytes.Equal(dr.DecodedData, d) {
		t.Errorf("DecodedData got %v, expected %v", dr.DecodedData, d)
	}
}
