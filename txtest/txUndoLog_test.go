// +build ignore

///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package txtest

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/transaction"
)

func BenchmarkUndoLogInt(b *testing.B) {
	j = pnew(int)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(j)
		*j = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkUndoLogSlice(b *testing.B) {
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 10000)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(struct1.slice)
		struct1.slice[i%10000] = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkUndoLogStruct(b *testing.B) {
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 10000)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(struct1)
		struct1.i = i
		tx.Log(struct1.slice)
		struct1.slice[i%10000] = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkUndoLog100Ints(b *testing.B) {
	slice1 = pmake([]int, 100)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		for c := 0; c < 100; c++ {
			tx.Log(&slice1[c])
			slice1[c] = i
		}
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkRawInt(b *testing.B) {
	j = pnew(int)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		*j = i
		runtime.PersistRange(unsafe.Pointer(j), unsafe.Sizeof(*j))
	}
}

func BenchmarkRawSlice(b *testing.B) {
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		struct1.slice[i%10000] = i
		runtime.PersistRange(unsafe.Pointer(&struct1.slice[0]),
			(uintptr)(len(struct1.slice)*
				(int)(unsafe.Sizeof(struct1.slice[0]))))
	}
}

func BenchmarkRawStruct(b *testing.B) {
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		struct1.i = i
		struct1.slice[i%100] = i
		runtime.PersistRange(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
		runtime.PersistRange(unsafe.Pointer(&struct1.slice[0]),
			(uintptr)(len(struct1.slice)))
	}
}

func BenchmarkRawReadInt(b *testing.B) {
	j = pnew(int)
	*j = b.N
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := i
		_ = a
	}
}

func TestUndoLogBasic(t *testing.T) {
	resetData()
	undoTx := transaction.NewUndoTx()
	fmt.Println("Testing basic data type commit.")
	undoTx.Begin()
	undoTx.Log(b)
	undoTx.Log(j)
	undoTx.Log(&struct1.i)
	*b = true
	*j = 10
	struct1.i = 100
	undoTx.End()
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)
	assertEqual(t, struct1.i, 100)

	fmt.Println("Testing basic data type abort.")
	undoTx.Begin()
	undoTx.Log(b)
	undoTx.Log(j)
	*b = false
	*j = 0
	transaction.Release(undoTx) // Calls abort internally
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)

	fmt.Println("Testing basic data type abort after multiple updates.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(j)
	*j = 100
	*j = 1000
	*j = 10000
	transaction.Release(undoTx) // Calls abort internally
	assertEqual(t, *j, 10)

	fmt.Println("Testing data structure commit.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(struct1)
	struct1.i = 10
	struct1.iptr = &struct1.i
	struct1.slice = slice1
	undoTx.Log(struct1.slice)
	slice1[0] = 11
	undoTx.End()
	assertEqual(t, struct1.i, 10)
	assertEqual(t, *struct1.iptr, 10)
	assertEqual(t, struct1.slice[0], slice1[0])
	undoTx.Begin()
	undoTx.Log(struct2)
	struct2.slice = slice2
	undoTx.Log(struct1)
	*struct1 = *struct2
	struct1.iptr = &struct1.i
	undoTx.Log(struct1.slice)
	slice2[0] = 22
	undoTx.End()
	assertEqual(t, struct1.i, struct2.i)
	assertEqual(t, *struct1.iptr, struct2.i)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing data structure abort.")
	undoTx.Begin()
	undoTx.Log(struct1)
	struct1.i = 10
	struct1.iptr = nil
	undoTx.Log(struct1.slice)
	struct1.slice = slice1
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, struct1.iptr, &struct1.i)
	assertEqual(t, struct1.slice[0], slice2[0])
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(struct1.iptr)
	struct1.i = 100
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	struct1.iptr = j
	undoTx.End()
	undoTx.Begin()
	undoTx.Log(struct1)
	*(struct1.iptr) = 1000 // redirect update will not rollback
	undoTx.Log(struct2)
	struct2.i = 3
	struct2.iptr = &struct2.i
	struct2.slice = slice1
	*struct1 = *struct2
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, *struct1.iptr, 1000)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing slice element update commit.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(slice1)
	slice1[99] = 99
	undoTx.End()
	assertEqual(t, slice1[99], 99)

	fmt.Println("Testing slice element update abort.")
	undoTx.Begin()
	undoTx.Log(slice1[:10])
	slice2[9] = 9
	slice2[10] = 10 // out of range update will not rollback
	copy(slice1, slice2)
	transaction.Release(undoTx)
	assertEqual(t, slice1[9], 0)
	assertEqual(t, slice1[10], 10)
	assertEqual(t, slice1[99], 0)

	fmt.Println("Testing slice append commit.")
	struct1.slice = pmake([]int, 100)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(&struct1.slice) // This would log slice header & slice elements
	struct1.slice[10] = 11
	struct1.slice = append(struct1.slice, 101)
	struct1.slice[100] = 101
	undoTx.End()
	assertEqual(t, struct1.slice[10], 11)
	assertEqual(t, struct1.slice[100], 101)
	assertEqual(t, len(struct1.slice), 101)

	fmt.Println("Testing slice append abort.")
	struct2.slice = pmake([]int, 90)
	undoTx.Begin()
	undoTx.Log(&struct2.slice)
	struct2.slice[10] = 10
	struct2.slice = append(struct2.slice, 1) // causes slice header update
	transaction.Release(undoTx)
	assertEqual(t, len(struct2.slice), 90)
	assertEqual(t, struct2.slice[10], 0)
	undoTx = transaction.NewUndoTx()
	struct2.slice = pmake([]int, 100)
	struct2.slice = append(struct2.slice, 1)
	undoTx.Begin()
	undoTx.Log(&struct2.slice)
	struct2.slice = append(struct2.slice, 1) // no slice header update
	struct2.slice[20] = 20
	transaction.Release(undoTx)
	assertEqual(t, len(struct2.slice), 101)
	assertEqual(t, struct2.slice[20], 0)

	fmt.Println("Testing error for logging data in volatile memory")
	errVolData := errors.New("[undoTx] Log: Can't log data in volatile memory")
	x := new(int)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	err := undoTx.Log(x)
	assertEqual(t, err.Error(), errVolData.Error())
	*x = 1
	transaction.Release(undoTx)
	assertEqual(t, *x, 1) // x was not logged, so update not rolled back
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	y := make([]int, 10)
	err = undoTx.Log(y)
	assertEqual(t, err.Error(), errVolData.Error())
	y[0] = 10
	transaction.Release(undoTx)
	assertEqual(t, y[0], 10)

	fmt.Println("Testing data structure commit when undoTx.Log() does update")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(struct1, structLogTest{10, &struct1.i, slice1})
	slice1[0] = 11
	undoTx.End()
	assertEqual(t, struct1.i, 10)
	assertEqual(t, *struct1.iptr, 10)
	assertEqual(t, struct1.slice[0], slice1[0])

	fmt.Println("Testing data structure abort when undoTx.Log() does update")
	undoTx.Begin()
	undoTx.Log(struct1, structLogTest{20, &struct1.i, slice1})
	slice1[0] = 22
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 10)
	assertEqual(t, *struct1.iptr, 10)
	assertEqual(t, struct1.slice[0], slice1[0])

	fmt.Println("Testing variable update when the new value is nil")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(&struct1, nil)
	undoTx.End()
	if struct1 != nil {
		assertEqual(t, 0, 1) // Assert
	}

	fmt.Println("Testing logging when old slice value is empty")
	struct2.slice = pmake([]int, 0, 0)
	assertEqual(t, len(struct2.slice), 0)
	undoTx.Begin()
	undoTx.Log(&slice2[2], 10)
	undoTx.Log(&struct2.slice, slice2)
	undoTx.End()
	transaction.Release(undoTx)
	assertEqual(t, struct2.slice[2], slice2[2])
	assertEqual(t, len(struct2.slice), len(slice2))

	fmt.Println("Testing End() return value for inner, outer transaction")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Begin()
	b := undoTx.End()
	assertEqual(t, b, false)
	b = undoTx.End()
	assertEqual(t, b, true)
	transaction.Release(undoTx)

	fmt.Println("Testing abort when nil slice is logged")
	struct1 = pnew(structLogTest)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(&struct1.slice)
	struct1.slice = pmake([]int, 2)
	transaction.Release(undoTx) // <-- abort
	if struct1.slice != nil {
		assertEqual(t, 0, 1) // Assert
	}
}

func TestUndoLogRead(t *testing.T) {
	resetData()
	tx := transaction.NewUndoTx()

	struct1.slice = pmake([]int, 10)
	struct2.slice = pmake([]int, 100)
	struct1.slice[0] = 9
	struct2.slice[0] = 2
	struct2.slice[20] = 1

	fmt.Println("Testing TX.Readlog with 1 arg (ptr)")
	tx.Begin()
	// struct1.i = struct2.slice[0]+3
	tx.Log(&struct1.i, tx.ReadLog(&struct2.slice[0]).(int)+3)
	tx.End()
	assertEqual(t, struct1.i, 5)

	fmt.Println("Testing TX.Readlog with 2 args (read slice element)")
	tx.Begin()
	// struct1.slice = struct2.slice
	tx.Log(&struct1.slice, struct2.slice)
	// *j = struct1.slice[20] + 2
	tx.Log(j, tx.ReadLog(&struct1.slice, 20).(int)+2)
	tx.End()
	assertEqual(t, *j, 3)
	assertEqual(t, len(struct1.slice), 100)
	assertEqual(t, struct1.slice[0], 2)

	fmt.Println("Testing TX.Readlog with 3 args (read slice)")
	struct2.slice[90] = 90
	tx.Begin()
	// struct1.slice = struct2.slice[88:92]
	tx.Log(&struct1.slice, tx.ReadLog(&struct2.slice, 88, 92).([]int))
	// *j = struct1.slice[2]
	tx.Log(j, tx.ReadLog(&struct1.slice, 2).(int)+1)
	tx.End()
	transaction.Release(tx)
	assertEqual(t, len(struct1.slice), 4)
	assertEqual(t, struct1.slice[2], 90)
	assertEqual(t, *j, 91)
}

func TestUndoLogExpand(t *testing.T) {
	fmt.Println("Testing undo log expansion commit by logging more entries")
	undoTx := transaction.NewUndoTx()
	sizeToCheck := transaction.NumEntries*4 + 1
	slice1 = pmake([]int, sizeToCheck)
	undoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		undoTx.Log(&slice1[i])
		slice1[i] = i
	}
	undoTx.End()
	transaction.Release(undoTx)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], i)
	}

	fmt.Println("Testing undo log expansion abort")
	slice1 = pmake([]int, sizeToCheck)
	sizeToAbort := transaction.NumEntries*2 + 1
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		undoTx.Log(&slice1[i])
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

func TestUndoLogConcurrent(t *testing.T) {
	resetData()
	fmt.Println("Testing concurrent logging")
	m1 := new(sync.RWMutex)
	m2 := new(sync.RWMutex)
	var wg sync.WaitGroup
	fmt.Println("Before:", struct1.i, struct2.i)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			undoTx := transaction.NewUndoTx()
			undoTx.Begin()

			// This is a bad way of taking locks inside transaction.
			// Used this way here for testing
			m1.Lock()
			undoTx.Log(struct1)
			struct1.i = i
			m1.Unlock()
			m2.Lock()
			undoTx.Log(struct2)
			struct2.i = i
			m2.Unlock()
			undoTx.End()

			transaction.Release(undoTx)
		}(i)
	}
	wg.Wait()
	assertEqual(t, struct1.i, struct2.i)
	fmt.Println("After:", struct1.i, struct2.i)
}

func TestUndoLogNoIsolation(t *testing.T) {
	resetData()
	var wg sync.WaitGroup
	chn0 := make(chan int)
	chn1 := make(chan int)
	fmt.Println("Testing no isolation for undo log")
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			if i == 1 {
				<-chn1
			}
			defer wg.Done()
			undoTx := transaction.NewUndoTx()
			undoTx.Begin()
			undoTx.Log(struct1)
			tmp := struct1.i // shared variable read
			if i == 1 {      // Goroutine 1
				chn0 <- 1
				<-chn1
				struct1.i = tmp + 100
				undoTx.End() // Oth goroutine doesn't execute this. So it's
				// update is rolled back. But 1st goroutine already
				// saw the update through tmp variable
				transaction.Release(undoTx)
			} else { // Goroutine 0
				struct1.i = tmp + 100 // Rolled back subsequently, but update
				// is visible immediately, before tx commit
				chn1 <- 1
				<-chn0
				transaction.Release(undoTx)
				chn1 <- 1
			}
		}(i)
	}
	wg.Wait()
	assertEqual(t, struct1.i, 201)
}
