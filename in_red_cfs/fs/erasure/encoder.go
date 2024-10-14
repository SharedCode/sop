// The encoder encodes a simple file into a number of shards.
// To reverse the process see "decoder.go"
package erasure

import (
	"fmt"
	"crypto/md5"

	"github.com/klauspost/reedsolomon"
)

type erasure struct {
	dataShardsCount int
	parityShardsCount int
}

// NewErasure instantiates an erasure encoder.
func NewErasure(dataShards int, parityShards int) (erasure, error) {
	if (dataShards + parityShards) > 256 {
		return erasure{}, fmt.Errorf("sum of data and parity shards cannot exceed 256")
	}
	return erasure {
		dataShardsCount: dataShards,
		parityShardsCount: parityShards,
	}, nil
}

// Encode erasure encodes data into a given set of data & parity shards & returns it, 
// or an error if there is an error encountered.
func (e *erasure)Encode(data []byte) ([][]byte, error) {

	// Create encoding matrix.
	enc, err := reedsolomon.New(e.dataShardsCount, e.parityShardsCount)
	if err != nil {
		return nil, err
	}

	// Split the file into equally sized shards.
	shards, err := enc.Split(data)
	if err != nil {
		return nil, err
	}

	// Encode parity
	err = enc.Encode(shards)
	if err != nil {
		return nil, err
	}

	return shards, nil
}

// ComputeShardMetadata returns a given shard's (computed) metadata.
// dataSize specifies the known data size,
// shardIndex is the index of the shard we need to compute metadata of.
func (e *erasure)ComputeShardMetadata(dataSize int, shards [][]byte, shardIndex int) ([]byte, error) {
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
	r := make([]byte, 1 + len(checksum))
	// Add the last shard stuffed zeroes count as 1st byte.
	r[0] = byte(e.dataShardsCount - dataSize % e.dataShardsCount)
	// Add the checksum bytes.
	copy(r[1:], checksum[0:])

	return r, nil
}
