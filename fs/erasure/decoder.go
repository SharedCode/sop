// The decoder reverses the process done by "encoder.go"
package erasure

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	log "log/slog"
)

// DecodeResult is a structure containing the Decode function result.
type DecodeResult struct {
	DecodedData []byte
	// In case shard(s) are nil or corrupted but can get reconstructed then this array
	// hold the shard(s) indeces that were nil or corrupted and had to be reconstructed.
	// Useful for fixing the nil or corrupted shards, e.g. - save or overwrite them.
	ReconstructedShardsIndeces []int
	Error                      error
}

// Decode will reverse the erasure encode done on shards and returns the data together
// with other useful details like indices of detected corrupted shards but are able to
// reconstruct using "erasure encoding" or error if there is an error encountered.
func (e *Erasure) Decode(shards [][]byte, shardsMetaData [][]byte) *DecodeResult {
	if len(shards) == 0 {
		return &DecodeResult{
			Error: fmt.Errorf("shards can't be nil or empty"),
		}
	}

	r := &DecodeResult{}
	// Verify the shards.
	ok, _ := e.encoder.Verify(shards)
	if !ok {
		log.Info("Verification failed, reconstructing data.")
		r = e.reconstructMissingShards(shards)
		if r.Error != nil {
			return r
		}
		ok, _ = e.encoder.Verify(shards)
		if !ok {
			dr := e.detectBadShardsThenReconstruct(shards, shardsMetaData)
			if dr.Error != nil {
				return &DecodeResult{
					Error: fmt.Errorf("final attempt to reconstruct failed, error: %v", dr.Error),
				}
			}
			r = dr
		}
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	err := e.encoder.Join(w, shards, len(shards[0])*e.DataShardsCount)
	if err != nil {
		return &DecodeResult{
			Error: fmt.Errorf("encoder.Join failed, error: %v", err),
		}
	}
	// Truncate trailing zeroes from decoded data, if there are and package for return.
	w.Flush()
	ba := make([]byte, len(b.Bytes())-int(shardsMetaData[0][0]))
	copy(ba, b.Bytes())
	r.DecodedData = ba
	return r
}

func (e *Erasure) detectBadShardsThenReconstruct(shards [][]byte, shardsMetaData [][]byte) *DecodeResult {
	corruptedShardsIndices := make([]int, 0, 2)
	for i := range shards {
		expectedChecksum := shardsMetaData[i][1:]
		gotChecksum := md5.Sum(shards[i])
		if !bytes.Equal(expectedChecksum, gotChecksum[:]) {
			corruptedShardsIndices = append(corruptedShardsIndices, i)
			shards[i] = nil
		}
	}
	if len(corruptedShardsIndices) == 0 {
		return &DecodeResult{
			Error: fmt.Errorf("shards passed checksum check, should be good"),
		}
	}
	err := e.encoder.Reconstruct(shards)
	if err != nil {
		return &DecodeResult{
			Error: err,
		}
	}
	ok, err := e.encoder.Verify(shards)
	if !ok {
		return &DecodeResult{
			Error: err,
		}
	}

	// Just return the nullified shards' indices that made shards to get reconstructed.
	return &DecodeResult{
		ReconstructedShardsIndeces: corruptedShardsIndices,
	}
}
func (e *Erasure) reconstructMissingShards(shards [][]byte) *DecodeResult {
	r := DecodeResult{}
	requestReconstruction := make([]bool, len(shards))
	// Fill in the missing shards so it can get repaired by the caller, if it gets reconstructed on this level.
	for i := range shards {
		if shards[i] == nil {
			r.ReconstructedShardsIndeces = append(r.ReconstructedShardsIndeces, i)
			requestReconstruction[i] = true
		}
	}
	if err := e.encoder.ReconstructSome(shards, requestReconstruction); err != nil {
		r.Error = err
	}
	return &r
}
