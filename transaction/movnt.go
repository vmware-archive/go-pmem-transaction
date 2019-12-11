// +build amd64
package transaction

import (
	"fmt"
	"log"
	"runtime"
	"unsafe"
)

func movnt128b(dst, src uintptr)
func movnt64b(dst, src uintptr)
func movnt32b(dst, src uintptr)

func movnt4x64b(dest, src uintptr)

func MOVNT(dst, src unsafe.Pointer, len uintptr) {
	movnt(dst, src, len)
}

// issue clwb, but no fence
func memmove_small_clwb(dst, src, len uintptr) {
	// Check len before coming to this fn
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
		movnt64b(dst, src)
		dst += 8
		src += 8
		len -= 8
	}
	align = len & 3
	for len != 0 && align == 0 {
		movnt32b(dst, src)
		dst += 4
		src += 4
		len -= 4
	}
	// We cannot issue movnt because either the data structure is < 4B at this
	// point or it is unaligned. Let's issue clwb.
	if len > 0 {
		memmove_small_clwb(dst, src, len)
	}
}

// caller needs to put memory barrier to ensure ordering of stores
func movnt(dst0, src0 unsafe.Pointer, len uintptr) {
	dst := uintptr(dst0)
	src := uintptr(src0)

	if len%64 != 0 {
		log.Fatal("Movnt optimized only for 64 byte moves")
	}

	if dst&63 != 0 || src&63 != 0 {
		log.Fatal("dst or src not 64-byte aligned")
	}

	for len >= 256 {
		movnt4x64b(dst, src)
		dst += 256
		src += 256
		len -= 256
	}

	for len > 0 {
		movnt128b(dst, src)
		dst += 16
		src += 16
		len -= 16
	}
}
