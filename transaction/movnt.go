// +build amd64
package transaction

import (
	"fmt"
	"runtime"
	"unsafe"
)

func movnt4x64b(dst, src uintptr) // 256 bytes
func movnt2x64b(dst, src uintptr) // 128 bytes
func movnt1x64b(dst, src uintptr) // 64 bytes
func movnt1x32b(dst, src uintptr) // 32 bytes
func movnt1x16b(dst, src uintptr) // 16 bytes
func movnt1x8b(dst, src uintptr)  // 8 bytes
func movnt1x4b(dst, src uintptr)  // 4 bytes

// issue clwb, but no fence
func memmove_small_clwb(dst, src, len uintptr) {
	if len == 0 {
		return
	}
	srcByte := (*[maxInt]byte)(unsafe.Pointer(src))
	dstByte := (*[maxInt]byte)(unsafe.Pointer(dst))
	copy(dstByte[:len], srcByte[:len])
	runtime.FlushRange(unsafe.Pointer(dst), len)
	return
}

// issue movnt if we can, else fall back to clwb
func memmove_small(dst, src, len uintptr) {
	if len > 15 {
		panic(fmt.Sprintf("[movnt.go] [memmove_small] len is %d should be using movnt first", len))
	}
	align := len & 7
	if len != 0 && align == 0 {
		movnt1x8b(dst, src)
		dst += 8
		src += 8
		len -= 8
	}
	align = len & 3
	for len != 0 && align == 0 {
		movnt1x4b(dst, src)
		dst += 4
		src += 4
		len -= 4
	}
	// We cannot issue movnt because either the data structure is < 4B at this
	// point or it is unaligned. Let's issue clwb.
	memmove_small_clwb(dst, src, len)
}

// caller needs to put memory barrier to ensure ordering of stores
func movnt(dst0, src0 unsafe.Pointer, len uintptr) {
	dst := uintptr(dst0)
	src := uintptr(src0)
	if len <= 15 {
		memmove_small(dst, src, len)
		return
	}

	align := dst & 15 // Make sure we start with 16B align
	if align > 0 {
		memmove_small(dst, src, align)
		dst += align
		src += align
		len -= align
	}
	for len >= 16 {
		movnt1x16b(dst, src)
		dst += 16
		src += 16
		len -= 16
	}
	memmove_small(dst, src, len)
}
