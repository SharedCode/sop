package fs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// marshalData copies payload into block, pads with zeros, and appends CRC32.
// The block size is determined by len(block).
// The payload must fit in len(block) - 4.
func marshalData(payload []byte, block []byte) []byte {

	dataLen := len(block) - 4
	if dataLen < len(payload) {
		block = make([]byte, len(payload)+4)
		dataLen = len(payload)
	}

	// Copy payload
	copy(block, payload)
	// Zero-pad the rest of the data section
	for i := len(payload); i < dataLen; i++ {
		block[i] = 0
	}

	// Optimization: If the data section is all zeros, the checksum (0) is also zero.
	// We can skip the checksum calculation and write, leaving the block as pure zeros.
	// This matches the "Sparse Block" optimization in unmarshalBlock.
	if isZeroData(block[:dataLen]) {
		// Ensure the checksum area is also zeroed (it should be if block was clean, but let's be safe)
		binary.LittleEndian.PutUint32(block[dataLen:], 0)
		return block
	}

	// Calculate and write checksum
	checksum := crc32.ChecksumIEEE(block[:dataLen])
	binary.LittleEndian.PutUint32(block[dataLen:], checksum)

	return block
}

// unmarshalData validates the block and returns the payload.
// The block size is determined by len(block).
func unmarshalData(block []byte) ([]byte, error) {
	if len(block) < 4 {
		return nil, fmt.Errorf("block too small")
	}
	dataLen := len(block) - 4
	// Optimization: Valid if all zeros (sparse/unwritten block)
	if isZeroData(block) {
		return block[:dataLen], nil
	}

	// Validation: Checksum match
	checksum := crc32.ChecksumIEEE(block[:dataLen])
	savedChecksum := binary.LittleEndian.Uint32(block[dataLen:])
	if checksum != savedChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}
	return block[:dataLen], nil
}

func isZeroData(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
