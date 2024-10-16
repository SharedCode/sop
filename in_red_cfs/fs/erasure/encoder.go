// The encoder encodes a simple file into a number of shards.
// To reverse the process see "decoder.go"
package erasure

import (
	"crypto/md5"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

type Erasure struct {
	dataShardsCount   int
	parityShardsCount int
	encoder           reedsolomon.Encoder
}

// NewErasure instantiates an erasure encoder.
func NewErasure(dataShards int, parityShards int) (*Erasure, error) {
	if (dataShards + parityShards) > 256 {
		return nil, fmt.Errorf("sum of data and parity shards cannot exceed 256")
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return &Erasure{
		dataShardsCount:   dataShards,
		parityShardsCount: parityShards,
		encoder:           enc,
	}, nil
}

// Encode erasure encodes data into a given set of data & parity shards & returns it,
// or an error if there is an error encountered.
func (e *Erasure) Encode(data []byte) ([][]byte, error) {

	// Create encoding matrix.

	// Split the file into equally sized shards.
	shards, err := e.encoder.Split(data)
	if err != nil {
		return nil, err
	}

	// Encode parity
	err = e.encoder.Encode(shards)
	if err != nil {
		return nil, err
	}

	return shards, nil
}

// ComputeShardMetadata returns a given shard's (computed) metadata.
// dataSize specifies the known data size,
// shardIndex is the index of the shard we need to compute metadata of.
func (e *Erasure) ComputeShardMetadata(dataSize int, shards [][]byte, shardIndex int) ([]byte, error) {
	if dataSize <= 0 {
		return nil, fmt.Errorf("dataSize(%d) is invalid", dataSize)
	}
	if shardIndex < 0 || shardIndex >= len(shards) {
		return nil, fmt.Errorf("shardIndex(%d) is invalid", shardIndex)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("shards can't be empty or nil")
	}
	checksum := md5.Sum(shards[shardIndex])
	r := make([]byte, 1+len(checksum))
	// Add the last shard stuffed zeroes count as 1st byte.
	if dataSize%e.dataShardsCount != 0 {
		r[0] = byte(e.dataShardsCount - dataSize%e.dataShardsCount)
	}
	// Add the checksum bytes.
	copy(r[1:], checksum[0:])

	return r, nil
}
