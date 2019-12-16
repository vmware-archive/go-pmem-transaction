package transaction

import (
	"runtime"
	"unsafe"
)

var cacheLineSz = uintptr(runtime.FLUSH_ALIGN)

type (
	flushSt struct {
		//data *Tree
		data map[unsafe.Pointer]int
	}
)

func (f *flushSt) insert(start, size uintptr) {
	if f.data == nil {
		//f.data = NewRbTree()
		// This is kept as a map of unsafe pointers so that GC walks the map
		f.data = make(map[unsafe.Pointer]int)
	}

	alignedAddr := start &^ (cacheLineSz - 1)
	//f.data[alignedAddr] = 1
	//return

	//if f.data.Get(alignedAddr) {
	//	return // already exists
	//}
	// We only care about cacheline aligned addresses
	for alignedAddr < start+size {
		//f.data.Put(alignedAddr)
		f.data[unsafe.Pointer(alignedAddr)] = 1
		alignedAddr += cacheLineSz
	}
}

func (f *flushSt) flushAndDestroy() {
	if f.data != nil {
		//flushRbTree(f.data.Root)
		flushRbTreeMap(f.data)
		runtime.Fence()
		f.data = nil
	}
}

func flushRbTreeMap(m map[unsafe.Pointer]int) {
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
