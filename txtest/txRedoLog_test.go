///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package txtest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/transaction"
)

func BenchmarkRedoLogInt(b *testing.B) {
	j = pnew(int)
	tx := transaction.NewRedoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(j, i)
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkRedoLog100Ints(b *testing.B) {
	slice1 = pmake([]int, 100)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		for c := 0; c < 100; c++ {
			tx.Log(&slice1[c], i)
		}
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkRedoLogSlice(b *testing.B) {
	struct1 = pnew(structLogTest)
	struct2 = pnew(structLogTest)
	struct1.slice = pmake([]int, 10000)
	struct2.slice = pmake([]int, 10000)
	tx := transaction.NewRedoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		struct2.slice[i%1000] = i
		tx.Log(struct1.slice, struct2.slice)
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkRedoLogReadInt(b *testing.B) {
	j = pnew(int)
	tx := transaction.NewRedoTx()
	tx.Begin()
	tx.Log(j, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := tx.ReadLog(j)
		_ = a
	}
	tx.End()
	transaction.Release(tx)
}

func TestRedoLogRead(t *testing.T) {
	resetData()
	tx := transaction.NewRedoTx()

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

func TestRedoLogExpand(t *testing.T) {
	fmt.Println("Testing redo log expansion commit by logging more entries")
	redoTx := transaction.NewRedoTx()
	sizeToCheck := transaction.NumEntries*4 + 1
	slice1 = pmake([]int, sizeToCheck)
	redoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		redoTx.Log(&slice1[i], i)
	}
	redoTx.End()
	transaction.Release(redoTx)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], i)
	}

	fmt.Println("Testing redo log expansion abort")
	slice1 = pmake([]int, sizeToCheck)
	sizeToAbort := transaction.NumEntries*2 + 1
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	for i := 0; i < sizeToCheck; i++ {
		redoTx.Log(&slice1[i], i)
		if i == sizeToAbort {
			break
		}
	}
	transaction.Release(redoTx)
	for i := 0; i < sizeToCheck; i++ {
		assertEqual(t, slice1[i], 0)
	}
}

func TestRedoLogBasic(t *testing.T) {
	resetData()
	var (
		bTmp interface{}
		//ok    bool
		jTmp interface{}
	)

	fmt.Println("Testing basic data type commit.")
	redoTx := transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(b, true)
	redoTx.Log(j, 10)
	bTmp = redoTx.ReadLog(b)
	jTmp = redoTx.ReadLog(j)
	assertEqual(t, *b, false)
	assertEqual(t, *j, 0)
	assertEqual(t, bTmp.(bool), true)
	assertEqual(t, jTmp.(int), 10)
	redoTx.End()
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)

	fmt.Println("Testing multiple writes to same variable of basic data type.")
	redoTx.Begin()
	redoTx.Log(b, false)
	redoTx.Log(j, 20)
	bTmp = redoTx.ReadLog(b)
	jTmp = redoTx.ReadLog(j)
	assertEqual(t, bTmp.(bool), false)
	assertEqual(t, jTmp.(int), 20)
	redoTx.Log(b, true)
	redoTx.Log(j, 30)
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)
	bTmp = redoTx.ReadLog(b)
	jTmp = redoTx.ReadLog(j)
	assertEqual(t, bTmp.(bool), true)
	assertEqual(t, jTmp.(int), 30)
	redoTx.End()
	assertEqual(t, *b, true)
	assertEqual(t, *j, 30)

	fmt.Println("Testing basic data type abort.")
	redoTx.Begin()
	redoTx.Log(b, false)
	redoTx.Log(j, 40)
	bTmp = redoTx.ReadLog(b)
	jTmp = redoTx.ReadLog(j)
	assertEqual(t, bTmp.(bool), false)
	assertEqual(t, jTmp.(int), 40)
	transaction.Release(redoTx) // Calls abort internally
	assertEqual(t, *b, true)
	assertEqual(t, *j, 30)

	type structRedoBasic struct {
		B    bool
		I    int
		Iptr *int
		S    string
	}
	fmt.Println("Testing basic struct commit.")
	basicStruct1 := pnew(structRedoBasic)
	basicStruct2 := pnew(structRedoBasic)
	basicStruct2.I = 10
	basicStruct2.Iptr = &basicStruct2.I
	basicStruct2.B = true
	basicStruct2.S = "Hello1"
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(basicStruct1, *basicStruct2)
	tmpBasicStruct := redoTx.ReadLog(basicStruct1)
	assertEqual(t, tmpBasicStruct.(structRedoBasic), *basicStruct2)
	assertEqual(t, basicStruct1.I, 0)
	assertEqual(t, basicStruct1.B, false)
	assertEqual(t, basicStruct1.S, "")
	redoTx.Log(&basicStruct1.S, basicStruct2.S)
	redoTx.End()
	assertEqual(t, basicStruct1.I, 10)
	assertEqual(t, *basicStruct1.Iptr, 10)
	assertEqual(t, basicStruct1.B, true)
	assertEqual(t, basicStruct1.S, "Hello1")
	basicStruct1 = pnew(structRedoBasic)
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(basicStruct1, *basicStruct2)
	tmpBasicStruct = redoTx.ReadLog(basicStruct1)
	assertEqual(t, tmpBasicStruct.(structRedoBasic), *basicStruct2)
	redoTx.Log(&basicStruct1.S, "Hello2")
	redoTx.Log(&basicStruct1.I, 20)
	redoTx.End()
	assertEqual(t, basicStruct1.I, 20)
	assertEqual(t, *basicStruct1.Iptr, 10) // pointing to basicStruct2.I
	assertEqual(t, basicStruct1.B, true)
	assertEqual(t, basicStruct1.S, "Hello2")

	fmt.Println("Testing basic struct abort.")
	basicStruct2.I = 30
	basicStruct2.B = false
	basicStruct2.S = "Hello3"
	redoTx.Begin()
	redoTx.Log(basicStruct1, *basicStruct2)
	tmpBasicStruct = redoTx.ReadLog(basicStruct1)
	assertEqual(t, tmpBasicStruct.(structRedoBasic).I, 30)
	assertEqual(t, *(tmpBasicStruct.(structRedoBasic).Iptr), 30)
	assertEqual(t, tmpBasicStruct.(structRedoBasic).B, false)
	assertEqual(t, tmpBasicStruct.(structRedoBasic).S, "Hello3")
	assertEqual(t, basicStruct1.I, 20)
	assertEqual(t, *basicStruct1.Iptr, 30)
	assertEqual(t, basicStruct1.B, true)
	assertEqual(t, basicStruct1.S, "Hello2")
	*basicStruct1.Iptr = 40 // redirect update will not rollback
	transaction.Release(redoTx)
	assertEqual(t, basicStruct1.I, 20)
	assertEqual(t, *basicStruct1.Iptr, 40) // pointing to basicStruct2.I
	assertEqual(t, basicStruct2.I, 40)     // pointing to basicStruct2.I
	assertEqual(t, basicStruct1.B, true)
	assertEqual(t, basicStruct1.S, "Hello2")

	type structRedoNested struct {
		B      bool
		I      int
		Iptr   *int
		S      string
		Sbasic structRedoBasic
	}
	fmt.Println("Testing nested struct commit.")
	nestedStruct1 := pnew(structRedoNested)
	nestedStruct2 := pnew(structRedoNested)
	nestedStruct2.I = 10
	nestedStruct2.Iptr = &nestedStruct2.I
	nestedStruct2.B = true
	nestedStruct2.S = "Hello1"
	nestedStruct2.Sbasic.I = 100
	nestedStruct2.Sbasic.Iptr = &nestedStruct2.Sbasic.I
	nestedStruct2.Sbasic.S = "World1"
	nestedStruct2.Sbasic.B = true
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(nestedStruct1, *nestedStruct2)
	tmpNestedStruct := redoTx.ReadLog(nestedStruct1)
	assertEqual(t, tmpNestedStruct.(structRedoNested), *nestedStruct2)
	assertEqual(t, nestedStruct1.I, 0)
	assertEqual(t, nestedStruct1.B, false)
	assertEqual(t, nestedStruct1.S, "")
	assertEqual(t, nestedStruct1.Sbasic.I, 0)
	assertEqual(t, nestedStruct1.Sbasic.B, false)
	assertEqual(t, nestedStruct1.Sbasic.S, "")
	redoTx.End()
	assertEqual(t, nestedStruct1.I, 10)
	assertEqual(t, *nestedStruct1.Iptr, 10)
	assertEqual(t, nestedStruct1.B, true)
	assertEqual(t, nestedStruct1.S, "Hello1")
	assertEqual(t, nestedStruct1.Sbasic.I, 100)
	assertEqual(t, *nestedStruct1.Sbasic.Iptr, 100)
	assertEqual(t, nestedStruct1.Sbasic.B, true)
	assertEqual(t, nestedStruct1.Sbasic.S, "World1")
	nestedStruct1 = pnew(structRedoNested)
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(nestedStruct1, *nestedStruct2)
	redoTx.Log(&nestedStruct1.Sbasic, *basicStruct1)
	redoTx.Log(&nestedStruct1.Iptr, &nestedStruct1.I)
	assertEqual(t, nestedStruct1.S, "")
	assertEqual(t, nestedStruct1.Sbasic.S, "")
	redoTx.Log(&nestedStruct1.S, "Hello3")
	redoTx.Log(&nestedStruct1.Sbasic.S, "World3")
	redoTx.End()
	assertEqual(t, nestedStruct1.I, 10)
	assertEqual(t, *nestedStruct1.Iptr, 10)
	assertEqual(t, nestedStruct1.B, true)
	assertEqual(t, nestedStruct1.S, "Hello3")
	assertEqual(t, nestedStruct1.Sbasic.I, 20)
	assertEqual(t, *nestedStruct1.Sbasic.Iptr, 40)
	assertEqual(t, nestedStruct1.Sbasic.B, true)
	assertEqual(t, nestedStruct1.Sbasic.S, "World3")

	fmt.Println("Testing nested struct abort.")
	nestedStruct2.I = 50
	nestedStruct2.Iptr = &nestedStruct2.I
	nestedStruct2.B = false
	nestedStruct2.S = "Hello4"
	nestedStruct2.Sbasic.I = 200
	nestedStruct2.Sbasic.Iptr = &nestedStruct2.Sbasic.I
	nestedStruct2.Sbasic.S = "World4"
	nestedStruct2.Sbasic.B = false
	redoTx.Begin()
	redoTx.Log(nestedStruct1, *nestedStruct2)
	redoTx.Log(&nestedStruct1.Sbasic, *basicStruct2)
	redoTx.Log(&nestedStruct1.S, "Hello5")
	redoTx.Log(&nestedStruct1.Sbasic.S, "World5")
	*nestedStruct1.Sbasic.Iptr = 300 // redirect update will not rollback
	transaction.Release(redoTx)
	assertEqual(t, nestedStruct1.I, 10)
	assertEqual(t, *nestedStruct1.Iptr, 10)
	assertEqual(t, nestedStruct1.B, true)
	assertEqual(t, nestedStruct1.S, "Hello3")
	assertEqual(t, nestedStruct1.Sbasic.I, 20)
	assertEqual(t, *nestedStruct1.Sbasic.Iptr, 300)
	assertEqual(t, nestedStruct1.Sbasic.B, true)
	assertEqual(t, nestedStruct1.Sbasic.S, "World3")

	fmt.Println("Testing slice element update commit.")
	slice2[99] = 99
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(slice1, slice2)
	redoTx.Log(&slice1[98], 98)
	redoTx.End()
	assertEqual(t, slice1[98], 98)
	assertEqual(t, slice1[99], 99)

	fmt.Println("Testing slice update abort.")
	slice2[9] = 9
	redoTx.Begin()
	redoTx.Log(slice1[:10], slice2[:10])
	redoTx.Log(&slice1[8], 8)
	redoTx.Log(&slice1[12], 12)
	transaction.Release(redoTx)
	assertEqual(t, slice1[9], 0)
	assertEqual(t, slice1[8], 0)
	assertEqual(t, slice1[12], 0)
	assertEqual(t, slice1[99], 99)

	fmt.Println("Testing read for data not logged before")
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	tmpNestedStruct1 := redoTx.ReadLog(nestedStruct1)
	redoTx.End()
	nestedStruct3 := tmpNestedStruct1.(structRedoNested)
	transaction.Release(redoTx)
	assertEqual(t, nestedStruct3, *nestedStruct1)

	type structRedoUnexportedField struct {
		i    int
		b    bool
		iptr *int
		uptr unsafe.Pointer
		Intf interface{} // okay to log exported interface variables
	}
	st1 := pnew(structRedoUnexportedField)
	st2 := pnew(structRedoUnexportedField)
	st1.Intf = 20
	st1.i = 10
	st1.b = true
	st1.iptr = &st1.i
	st1.uptr = unsafe.Pointer(&st1.b)
	fmt.Println("Testing logging for unexported fields of struct")
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(st2, *st1)
	tmp1 := redoTx.ReadLog(&st2.iptr)
	assertEqual(t, tmp1.(*int), st1.iptr)
	tmp2 := redoTx.ReadLog(&st2.uptr)
	assertEqual(t, tmp2.(unsafe.Pointer), st1.uptr)
	tmp3 := redoTx.ReadLog(&st2.Intf)
	assertEqual(t, tmp3.(int), 20)
	redoTx.End()
	assertEqual(t, *st2, *st1)

	fmt.Println("Testing logging for nil values")
	doublePtr := pnew(*int)
	doublePtr = &st1.iptr
	redoTx.Begin()
	redoTx.Log(doublePtr, nil)
	redoTx.End()
	if *doublePtr != nil {
		assertEqual(t, 0, 1) // Assert
	}

	fmt.Println("Testing read for nil value")
	doublePtr1 := pnew(*int)
	redoTx.Begin()
	tmp1 = redoTx.ReadLog(doublePtr1).(*int)
	if tmp1.(*int) != nil {
		assertEqual(t, 0, 1)
	}
	redoTx.End()
	transaction.Release(redoTx)

	fmt.Println("Testing slice append commit.")
	struct1.slice = pmake([]int, 100)
	struct2.slice = pmake([]int, 101)
	struct2.slice[100] = 300
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Log(&struct1.slice, append(struct1.slice, 200)) // Logs only slicehdr
	tmpSlice := redoTx.ReadLog(&struct1.slice).([]int)     // slicehdr from log
	assertEqual(t, len(tmpSlice), 101)
	redoTx.Log(tmpSlice, struct2.slice)
	redoTx.End()
	assertEqual(t, struct1.slice[100], 300)
	assertEqual(t, len(struct1.slice), 101)

	fmt.Println("Testing slice append abort.")
	struct2.slice = pmake([]int, 90)
	redoTx.Begin()
	redoTx.Log(&struct2.slice, append(struct2.slice, 10))
	tmpSlice = redoTx.ReadLog(&struct2.slice).([]int)
	assertEqual(t, len(tmpSlice), 91)
	redoTx.Log(&tmpSlice[20], 20)
	transaction.Release(redoTx)
	assertEqual(t, len(struct2.slice), 90)
	assertEqual(t, struct2.slice[20], 0)
	redoTx = transaction.NewRedoTx()
	struct2.slice = pmake([]int, 100)
	struct2.slice = append(struct2.slice, 1)
	redoTx.Begin()
	redoTx.Log(&struct2.slice, append(struct2.slice, 1)) // slicehdr not updated
	redoTx.Log(&struct2.slice[30], 30)
	transaction.Release(redoTx)
	assertEqual(t, len(struct2.slice), 101)
	assertEqual(t, struct2.slice[30], 0)

	fmt.Println("Testing error for logging data in volatile memory")
	errVolData := errors.New("[redoTx] Log: Updates to data in volatile" +
		" memory can be lost")
	x := new(int)
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	err := redoTx.Log(x, 1)
	assertEqual(t, err.Error(), errVolData.Error())
	assertEqual(t, redoTx.ReadLog(x).(int), 1) // got error, but data logged
	redoTx.End()
	assertEqual(t, *x, 1) // got error, but update still persisted
	transaction.Release(redoTx)
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	y := make([]int, 10)
	z := make([]int, 5)
	z[0] = 10
	err = redoTx.Log(y, z)
	assertEqual(t, err.Error(), errVolData.Error())
	assertEqual(t, redoTx.ReadLog(&y[0]).(int), 10)
	redoTx.End()
	assertEqual(t, y[0], 10)
	transaction.Release(redoTx)
	assertEqual(t, y[0], 10)

	fmt.Println("Testing End() return value for inner, outer transaction")
	redoTx = transaction.NewRedoTx()
	redoTx.Begin()
	redoTx.Begin()
	b := redoTx.End()
	assertEqual(t, b, false)
	b = redoTx.End()
	assertEqual(t, b, true)
	transaction.Release(redoTx)
}

func TestRedoLogIsolation(t *testing.T) {
	resetData()
	var wg sync.WaitGroup
	chn0 := make(chan int)
	chn1 := make(chan int)
	fmt.Println("Testing isolation for redo log")
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			if i == 1 {
				<-chn1
			}
			defer wg.Done()
			redoTx := transaction.NewRedoTx()
			redoTx.Begin()
			tmp := struct1.i // shared variable read
			if i == 1 {      // Goroutine 1
				chn0 <- 1
				<-chn1
				redoTx.Log(&struct1.i, tmp+100)
				redoTx.End() // Oth goroutine doesn't execute this. So it's
				// update is rolled back. 1st goroutine read the same variable
				// before, but doesn't see Goroutine 1's update in-memory
				transaction.Release(redoTx)
			} else { // Goroutine 0
				redoTx.Log(&struct1.i, tmp+100) // Rolled back subsequently,
				// because of tx abort
				chn1 <- 1
				<-chn0
				transaction.Release(redoTx)
				chn1 <- 1
			}
		}(i)
	}
	wg.Wait()
	assertEqual(t, struct1.i, 101)
}
