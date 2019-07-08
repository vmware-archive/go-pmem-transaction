package transaction

import (
	"log"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	bitsPerByte = 8
)

type bitmap struct {
	bitArray []uint32
	// cachedIndex is a hint as to where to begin the next search for an unset bit
	cachedIndex int
}

// Create a new bitmap datastructure with space allocated for the backing array.
func newBitmap(length int) *bitmap {
	bitArray := make([]uint32, length)
	return &bitmap{bitArray, 0}
}

// changeBit atomically tries to change the bit at index 'b' from old to new. It
// returns true if this was successful, and false in all other cases.
func (bm *bitmap) changeBit(b int, old, new uint32) bool {
	bitAddr := (*uint32)(unsafe.Pointer(&bm.bitArray[b]))
	return atomic.CompareAndSwapUint32(bitAddr, old, new)
}

// Returns the next index in the bitmap that is unset.
func (bm *bitmap) nextAvailable() int {
	ciAddr := (*int64)(unsafe.Pointer(&bm.cachedIndex))

	for {
		ind := int(atomic.LoadInt64(ciAddr))
		ln := len(bm.bitArray)
		for i := 0; i < ln; i++ {
			b := (ind + i) % ln
			if bm.changeBit(b, 0, 1) {
				return b
			}
		}
		// No unset bit at this time. Let some other goroutine (if available)
		// run instead.
		runtime.Gosched()
	}
}

// clearBit clears the bit at index 'b''
func (bm *bitmap) clearBit(b int) {
	if !bm.changeBit(b, 1, 0) {
		log.Fatal("Bit already unset")
	}
	ciAddr := (*int64)(unsafe.Pointer(&bm.cachedIndex))
	atomic.StoreInt64(ciAddr, int64(b))
}
