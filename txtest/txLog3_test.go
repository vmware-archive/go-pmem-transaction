///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package txtest

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/transaction"
)

// Keeping Log3 tests in a separate file

func TestUndoLog3Basic(t *testing.T) {
	resetData()
	undoTx := transaction.NewUndoTx()
	fmt.Println("Testing basic data type commit.")
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(b), 1)
	undoTx.Log3(unsafe.Pointer(j), 8)
	undoTx.Log3(unsafe.Pointer(&struct1.i), 8)
	*b = true
	*j = 10
	struct1.i = 100
	undoTx.End()
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)
	assertEqual(t, struct1.i, 100)

	fmt.Println("Testing basic data type abort.")
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(b), 1)
	undoTx.Log3(unsafe.Pointer(j), 8)
	*b = false
	*j = 0
	transaction.Release(undoTx) // Calls abort internally
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)

	fmt.Println("Testing basic data type abort after multiple updates.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(j), 8)
	*j = 100
	*j = 1000
	*j = 10000
	transaction.Release(undoTx) // Calls abort internally
	assertEqual(t, *j, 10)

	fmt.Println("Testing data structure commit.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
	struct1.i = 10
	struct1.iptr = &struct1.i
	struct1.slice = slice1
	undoTx.Log3(unsafe.Pointer(&slice1[0]), 8)
	slice1[0] = 11
	undoTx.End()
	assertEqual(t, struct1.i, 10)
	assertEqual(t, *struct1.iptr, 10)
	assertEqual(t, struct1.slice[0], slice1[0])

	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(struct2), unsafe.Sizeof(*struct2))
	struct2.slice = slice2
	undoTx.Log3(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
	*struct1 = *struct2
	struct1.iptr = &struct1.i
	undoTx.Log3(unsafe.Pointer(&slice2[0]), 8)
	slice2[0] = 22
	undoTx.End()
	assertEqual(t, struct1.i, struct2.i)
	assertEqual(t, *struct1.iptr, struct2.i)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing data structure abort.")
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
	struct1.i = 10
	struct1.iptr = nil
	undoTx.Log3(unsafe.Pointer(&struct1.slice), 24)
	struct1.slice = slice1
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, struct1.iptr, &struct1.i)
	assertEqual(t, struct1.slice[0], slice2[0])

	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(struct1.iptr), 8)
	struct1.i = 100
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)

	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	struct1.iptr = j
	undoTx.End()
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
	*(struct1.iptr) = 1000 // redirect update will not rollback
	undoTx.Log3(unsafe.Pointer(struct2), unsafe.Sizeof(*struct2))
	struct2.i = 3
	struct2.iptr = &struct2.i
	struct2.slice = slice1
	*struct1 = *struct2
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, *struct1.iptr, 1000)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing abort when nil slice is logged")
	struct1 = pnew(structLogTest)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log3(unsafe.Pointer(&struct1.slice), unsafe.Sizeof(struct1.slice))
	struct1.slice = pmake([]int, 2)
	transaction.Release(undoTx) // <-- abort
	if struct1.slice != nil {
		assertEqual(t, 0, 1) // Assert
	}
}

func TestUndoLog3Expand(t *testing.T) {
	sizeToCheck := 65536
	undoTx := transaction.NewUndoTx()

	fmt.Println("Testing undo log expansion commit by logging more entries")

	slice1 = pmake([]int, sizeToCheck)
	undoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		undoTx.Log3(unsafe.Pointer(&slice1[i]), 8)
		slice1[i] = i
	}
	undoTx.End()
	transaction.Release(undoTx)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], i)
	}

	fmt.Println("Testing undo log expansion abort")
	slice1 = pmake([]int, sizeToCheck)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], 0)
	}
	sizeToAbort := 65500
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		undoTx.Log3(unsafe.Pointer(&slice1[i]), 8)
		slice1[i] = i
		if i == sizeToAbort {
			break
		}
	}
	transaction.Release(undoTx)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], 0)
	}
}
