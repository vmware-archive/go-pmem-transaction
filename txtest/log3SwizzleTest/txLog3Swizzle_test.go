// +build swizzleTest

///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

// This test requires to be run twice to check undo log abort works correctly
// when pointers are swizzled in the recovery path.

// Hence this test is not run by default and will only be run if a flag
// 'swizzleTest' is specified while running the tests.
//
// E.g.: ~/go-pmem/bin/go test -tags="swizzleTest" -v # run 1
// E.g.: ~/go-pmem/bin/go test -tags="swizzleTest" -v # run 2

package log3SwizzleTest

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/pmem"
	"github.com/vmware/go-pmem-transaction/transaction"
)

const (
	intSize  = 8 // Size of an int variable
	loopSize = 8192
)

type logData struct {
	i  int
	ip *int
}

type log3Swizzle struct {
	int1 []logData
	int2 []logData
	int3 []logData
}

// Ensure that runtime pointer swizzling is force enabled before running the
// test
func TestUndoLog3Swizzle(t *testing.T) {
	firstInit := pmem.Init("/mnt/ext4-pmem0/log3swizzle")

	var log3Struct *log3Swizzle
	if firstInit {
		log3Struct = (*log3Swizzle)(pmem.New("log3Swizzle", log3Struct))
		// Create three int arrays occupying 60MB each. This ensures that three
		// separate persistent memory arenas will be created.
		log3Struct.int1 = pmake([]logData, 60*1024*1024/16) // 60MB
		log3Struct.int2 = pmake([]logData, 60*1024*1024/16) // 60MB
		log3Struct.int3 = pmake([]logData, 60*1024*1024/16) // 60MB
		runtime.PersistRange(unsafe.Pointer(log3Struct), unsafe.Sizeof(*log3Struct))

		// Write 0xFFFFFF data to the first three entries of int1, int2, int3.
		// TODO later change the bounds so that it will overflow one linked log3 array
		for i := 0; i < loopSize; i++ {
			log3Struct.int1[i].i = 0xAAAAAA
			log3Struct.int1[i].ip = &log3Struct.int2[i].i

			log3Struct.int2[i].i = 0xBBBBBB
			log3Struct.int2[i].ip = &log3Struct.int3[i].i

			log3Struct.int3[i].i = 0xCCCCCC
			log3Struct.int3[i].ip = &log3Struct.int1[i].i
		}

		runtime.PersistRange(unsafe.Pointer(&log3Struct.int1[0]), loopSize*intSize*2)
		runtime.PersistRange(unsafe.Pointer(&log3Struct.int2[0]), loopSize*intSize*2)
		runtime.PersistRange(unsafe.Pointer(&log3Struct.int3[0]), loopSize*intSize*2)

		undoTx := transaction.NewUndoTx()
		undoTx.Begin()
		for i := 0; i < loopSize; i++ {
			undoTx.Log3(unsafe.Pointer(&log3Struct.int1[i].i), intSize)
			log3Struct.int1[i].i = 0xABCDEF
			undoTx.Log3(unsafe.Pointer(&log3Struct.int2[i].i), intSize)
			log3Struct.int2[i].i = 0xBCEDFA
			undoTx.Log3(unsafe.Pointer(&log3Struct.int3[i].i), intSize)
			log3Struct.int3[i].i = 0xCDEFAB

			undoTx.Log3(unsafe.Pointer(&log3Struct.int1[i].ip), intSize)
			log3Struct.int1[i].ip = &log3Struct.int3[i].i
			undoTx.Log3(unsafe.Pointer(&log3Struct.int2[i].ip), intSize)
			log3Struct.int2[i].ip = &log3Struct.int1[i].i
			undoTx.Log3(unsafe.Pointer(&log3Struct.int3[i].ip), intSize)
			log3Struct.int3[i].ip = &log3Struct.int2[i].i
		}

		// Induce an application crash here
		return // <-- return before ending transaction to simulate CRASH!

	} else {
		log3Struct = (*log3Swizzle)(pmem.Get("log3Swizzle", log3Struct))
		if log3Struct == nil {
			log.Fatal("Invalid application state")
		}

		for i := 0; i < loopSize; i++ {
			assertEqual(t, log3Struct.int1[i].i, 0xAAAAAA)
			assertEqual(t, log3Struct.int2[i].i, 0xBBBBBB)
			assertEqual(t, log3Struct.int3[i].i, 0xCCCCCC)

			assertEqual(t, *log3Struct.int1[i].ip, log3Struct.int2[i].i)
			assertEqual(t, *log3Struct.int2[i].ip, log3Struct.int3[i].i)
			assertEqual(t, *log3Struct.int3[i].ip, log3Struct.int1[i].i)
		}
	}
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		debug.PrintStack()
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
