package transaction

import (
	"runtime"
	"unsafe"
)

var cacheLineSz = uintptr(runtime.FLUSH_ALIGN)

type (
	flushSt struct {
		data *Tree
	}
)

func (f *flushSt) insert(start, size uintptr) {
	if f.data == nil {
		f.data = NewRbTree()
	}
	alignedAddr := start &^ (cacheLineSz - 1)
	if f.data.Get(alignedAddr) {
		return // already exists
	}
	// We only care about cacheline aligned addresses
	for alignedAddr < start+size {
		f.data.Put(alignedAddr)
		alignedAddr += cacheLineSz
	}
}

func (f *flushSt) flushAndDestroy() {
	if f.data != nil {
		flushRbTree(f.data.Root)
		f.data = nil
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
