package fs

import (
	"fmt"
	"testing"
	"time"

	"github.com/SharedCode/sop"
)

const (
	hashModValue = 500000
	blockItemCount = 66
)
// This hashing algorithm tend to be denser as more data segment file is used. At two, it can fill around 66% avg.
// At one segment file, it fills up around 55%. SOP b-tree (w/ load distribution)
// can fill up around 55%-67%, so, this is at par. BUT better because each Handle is a very small sized data (record).
// At 4, it should be able to fill 75%.

func TestHashModDistribution(t *testing.T) {
	hashTable1 := make([][blockItemCount]sop.UUID, hashModValue)
	//hashTable2 := make([][66]sop.UUID, hashModValue)
	collisionCount := 0
	fmt.Printf("Start %v", time.Now())
	for i := 0; i < 23000000; i++ {
		// Split UUID into high & low int64 parts.
		id := sop.NewUUID()
		bytes := id[:]

		var high uint64
		for i := 0; i < 8; i++ {
			high = high<<8 | uint64(bytes[i])
		}
		var low uint64
		for i := 8; i < 16; i++ {
			low = low<<8 | uint64(bytes[i])
		}

		bucket := high % hashModValue 
		bucketOffset := low%blockItemCount

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
				// if hashTable2[bucket][bucketOffset] != sop.NilUUID {
				// 	for ii := 0; ii < 66; ii++ {
				// 		if hashTable2[bucket][ii] == sop.NilUUID {
				// 			hashTable2[bucket][ii] = id
				// 			foundASlot = true
				// 			break
				// 		}
				// 	}
				// } else {
				// 	hashTable2[bucket][bucketOffset] = id
				// 	continue
				// }
				if !foundASlot {
					collisionCount++
					// fmt.Printf("collision count: %d, current: %v, new: %v, Bucket: %d, Offset: %d\n",
					// 	collisionCount, hashTable1[bucket][bucketOffset], id, bucket, bucketOffset)
				}
			}
			continue
		}
		hashTable1[bucket][bucketOffset] = id
	}

	notUsedSlot := 0
	notFoundCount := 0

	for i := 0; i < len(hashTable1); i++ {
		for ii := 0; ii < blockItemCount; ii++ {
			if hashTable1[i][ii] == sop.NilUUID {
				notUsedSlot++
				continue
			}
			if !findItem(hashTable1, hashTable1[i][ii]) {
				notFoundCount++
				// fmt.Printf("item with UUID: %v not found\n", hashTable1[i][ii])
			}
		}
	}
	// for i := 0; i < len(hashTable2); i++ {
	// 	for ii := 0; ii < 66; ii++ {
	// 		if hashTable2[i][ii] == sop.NilUUID {
	// 			notUsedSlot++
	// 			continue
	// 		}
	// 		if !findItem(hashTable2, hashTable2[i][ii]) {
	// 			notFoundCount++
	// 		}
	// 	}
	// }
	fmt.Printf("not found count: %v, collision count: %v, not used slot: %v\n", notFoundCount, collisionCount, notUsedSlot)
	fmt.Printf("End %v", time.Now())
}

func findItem(ht [][blockItemCount]sop.UUID, id sop.UUID) bool {
	bytes := id[:]
	var high uint64
	for i := 0; i < 8; i++ {
		high = high<<8 | uint64(bytes[i])
	}
	var low uint64
	for i := 8; i < 16; i++ {
		low = low<<8 | uint64(bytes[i])
	}

	bucket := high % hashModValue 
	bucketOffset := low%blockItemCount

	if ht[bucket][bucketOffset] == id {
		return true
	}
	for i := 0; i < blockItemCount; i++ {
		if ht[bucket][i] == id {
			return true
		}
	}
	return false
}
