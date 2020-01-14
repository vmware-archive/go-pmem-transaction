package transaction

import (
	"runtime"
	"unsafe"
)

var cacheLineSz = uintptr(runtime.FLUSH_ALIGN)

type (
	flushSt struct {
		//data *Tree
		data map[unsafe.Pointer]uintptr
	}
)

// set bits i to j in num as 1. i, j inclusive and start from 0
func setBits(num, i, j uintptr) uintptr {
	if j > 63 {
		j = 63
	}
	// calculating a number 'n' having set
	// bits in the range from i to j and all other
	// bits as 0 (or unset).
	n := (((1 << i) - 1) ^ ((1 << (j + 1)) - 1))
	return num | uintptr(n)
}

// return if bits i to j in num are all set to 1. i, j inclusive and start from 0
func getBits(num, i, j uintptr) bool {
	if j > 63 {
		j = 63
	}
	n := (uintptr)(((1 << (i)) - 1) ^ ((1 << (j + 1)) - 1))
	m := num & n
	if m == n {
		return true
	}
	return false
}

func (f *flushSt) insert(start, size uintptr) bool {
	if f.data == nil {
		//f.data = NewRbTree()
		// This is kept as a map of unsafe pointers so that GC walks the map
		f.data = make(map[unsafe.Pointer]uintptr)
	}
	exists := true
	alignedAddr := start &^ (cacheLineSz - 1)
	lower8Bits := start & (cacheLineSz - 1)
	//f.data[alignedAddr] = 1
	//return

	//if f.data.Get(alignedAddr) {
	//	return // already exists
	//}
	// We only care about cacheline aligned addresses

	sizeRemain := size
	for alignedAddr < start+size {
		currMask, ok := f.data[unsafe.Pointer(alignedAddr)]
		if !ok {
			//fmt.Println("No map entry")
			//fmt.Println("start = ", start, "sizeRemain = ", sizeRemain, "lower8Bits =", lower8Bits, "newmask = ", setBits(0, lower8Bits, lower8Bits+sizeRemain))
			exists = false
			f.data[unsafe.Pointer(alignedAddr)] = setBits(0, lower8Bits, lower8Bits+sizeRemain)
		} else {
			if getBits(currMask, lower8Bits, lower8Bits+sizeRemain) == false {
				//fmt.Println("Found map entry updating mask")
				//fmt.Println("start = ", start, "sizeRemain = ", sizeRemain, "lower8Bits =", lower8Bits, "oldmask =", currMask, "newmask = ", setBits(currMask, lower8Bits, lower8Bits+sizeRemain))
				exists = false
				f.data[unsafe.Pointer(alignedAddr)] = setBits(currMask, lower8Bits, lower8Bits+sizeRemain)
			} else {
				//fmt.Println("Exact match")
				//fmt.Println("start = ", start, "sizeRemain = ", sizeRemain, "mask =", currMask)
			}
		}
		alignedAddr += cacheLineSz
		sizeRemain -= cacheLineSz
	}
	return exists
}

func (f *flushSt) flushAndDestroy() {
	if f.data != nil {
		//flushRbTree(f.data.Root)
		flushRbTreeMap(f.data)
		runtime.Fence()
		f.data = nil
	}
}

// only destroys
func (f *flushSt) Destroy() {
	f.data = nil
}

func flushRbTreeMap(m map[unsafe.Pointer]uintptr) {
	for k := range m {
		runtime.FlushRange(k, cacheLineSz)
	}
}

func flushRbTree(n *Node) {
	if n == nil {
		return
	}
	flushRbTree(n.Left)
	runtime.FlushRange(unsafe.Pointer(n.Key), cacheLineSz)
	flushRbTree(n.Right)
}
