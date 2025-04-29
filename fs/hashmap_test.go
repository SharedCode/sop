package fs

import (
	"fmt"
	"testing"

	"github.com/SharedCode/sop"
)

const hashModValue = 250000

func TestHashModDistribution(t *testing.T) {
	hashTable1 := make([][66]sop.UUID, hashModValue)
	//hashTable2 := make([][66]sop.UUID, 250000)
	//hashTable3 := make([][66]sop.UUID, 250000)
	collisionCount := 0
	for i := 0; i < 16000000; i++ {
		// Split UUID into high & low int64 parts.
		id := sop.NewUUID()
		bytes := id[:]

		var high int64
		for i := 0; i < 8; i++ {
			high = high<<8 | int64(bytes[i])
		}
		var low int64
		for i := 8; i < 16; i++ {
			low = low<<8 | int64(bytes[i])
		}

		if high < 0 {
			high = -high
		}
		bucket := high % hashModValue 
		if low < 0 {
			low = -low
		}
		bucketOffset := low%66

		if hashTable1[bucket][bucketOffset] != sop.NilUUID {
			foundASlot := false
			for ii := 0; ii < 66; ii++ {
				if hashTable1[bucket][ii] == sop.NilUUID {
					hashTable1[bucket][ii] = id
					foundASlot = true
					break
				}
			}
			if !foundASlot {
				collisionCount++
				// fmt.Printf("collision count: %d, current: %v, new: %v, Bucket: %d, Offset: %d\n",
				// 	collisionCount, hashTable1[bucket][bucketOffset], id, bucket, bucketOffset)
				continue
			}
		}
		hashTable1[bucket][bucketOffset] = id
	}

	notFoundCount := 0

	for i := 0; i < len(hashTable1); i++ {
		for ii := 0; ii < 66; ii++ {
			if !findItem(hashTable1, hashTable1[i][ii]) {
				notFoundCount++
				fmt.Printf("item with UUID: %v not found\n", hashTable1[i][ii])
			}
		}
	}
	fmt.Printf("not found count: %v, collision count: %v\n", notFoundCount, collisionCount)
}

func findItem(ht [][66]sop.UUID, id sop.UUID) bool {
	bytes := id[:]
	var high int64
	for i := 0; i < 8; i++ {
		high = high<<8 | int64(bytes[i])
	}
	var low int64
	for i := 8; i < 16; i++ {
		low = low<<8 | int64(bytes[i])
	}

	if high < 0 {
		high = -high
	}
	bucket := high % hashModValue 
	if low < 0 {
		low = -low
	}
	bucketOffset := low%66

	if ht[bucket][bucketOffset] == id {
		return true
	}
	for i := 0; i < 66; i++ {
		if ht[bucket][i] == id {
			return true
		}
	}
	return false
}
