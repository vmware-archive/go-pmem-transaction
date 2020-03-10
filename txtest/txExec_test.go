///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package txtest

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/transaction"
)

const (
	intSize = 8
)

type basic struct {
	i     int
	slice []int
}

func add(tx transaction.TX, a *basic, b *basic) {
	tx.Log3(unsafe.Pointer(&a.i), intSize)
	a.i = a.i + b.i
}

func addRet(tx transaction.TX, a *basic, b *basic) int {
	tx.Log3(unsafe.Pointer(&a.i), intSize)
	return a.i + b.i
}

func TestExec(t *testing.T) {
	// Set up pmem appRoot and header

	var (
		tx     transaction.TX
		err    error
		retVal []reflect.Value
	)

	a := pnew(basic)
	b := pnew(basic)
	a.slice = pmake([]int, 0)
	b.slice = pmake([]int, 0)

	// Enumerating possible errors Exec() can return
	errNoArgs := errors.New("[undoTx] Exec: Must have atleast one argument")
	errFirstArg := errors.New("[undoTx] Exec: 1st argument must be a function")
	errNumArgs := errors.New("[undoTx] Exec: Incorrect no. of args in " +
		"function passed to Exec")
	errTypeArgs := errors.New("[undoTx] Exec: Incorrect type of args in " +
		"function passed to Exec")
	errTxBeginEnd := errors.New("[undoTx] Exec: Unbalanced Begin() & End() " +
		"calls inside function passed to Exec")
	errNil := err

	fmt.Println("Testing with no arguments")
	tx = transaction.NewUndoTx()
	_, err = tx.Exec()
	transaction.Release(tx)
	assertEqual(t, err.Error(), errNoArgs.Error())

	fmt.Println("Testing when 1st argument is incorrect")
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(2, 3, add)
	transaction.Release(tx)
	assertEqual(t, err.Error(), errFirstArg.Error())

	fmt.Println("Testing when number of arguments don't match")
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(add, 2)
	transaction.Release(tx)
	assertEqual(t, err.Error(), errNumArgs.Error())

	fmt.Println("Testing when type of arguments don't match")
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(add, 2, 3)
	transaction.Release(tx)
	assertEqual(t, err.Error(), errTypeArgs.Error())

	fmt.Println("Testing with more Begin() than End() calls inside function " +
		"passed to Exec")
	a.i = 2
	b.i = 3
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(func(tx transaction.TX) {
		tx.Begin()
		tx.Log3(unsafe.Pointer(a), unsafe.Sizeof(*a))
		a.i = a.i + b.i
	})
	transaction.Release(tx)
	assertEqual(t, a.i, 2) // TX reverted as Release() is called without End()
	assertEqual(t, err.Error(), errTxBeginEnd.Error())

	fmt.Println("Testing with Begin() & End() calls inside function " +
		"passed to Exec")
	a.i = 20
	b.i = 30
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(func(tx transaction.TX) {
		tx.Begin()
		tx.Log3(unsafe.Pointer(a), unsafe.Sizeof(*a))
		a.i = a.i + b.i
		tx.End()
	})
	transaction.Release(tx)
	assertEqual(t, a.i, 50)
	assertEqual(t, err, errNil)

	fmt.Println("Testing when End() is called inside function passed to Exec")
	a.i = 200
	b.i = 300
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(func(tx transaction.TX) {
		tx.Log3(unsafe.Pointer(a), unsafe.Sizeof(*a))
		a.i = a.i + b.i
		tx.End()
	})
	transaction.Release(tx)
	assertEqual(t, a.i, 500) // TX completed successfully but returns error
	assertEqual(t, err.Error(), errTxBeginEnd.Error())

	fmt.Println("Testing simple add function")
	a.i = 2
	b.i = 3
	tx = transaction.NewUndoTx()
	retVal, err = tx.Exec(add, a, b)
	transaction.Release(tx)
	assertEqual(t, err, errNil)
	assertEqual(t, a.i, 5)
	assertEqual(t, len(retVal), 0)

	fmt.Println("Testing with anon function & closure")
	a.i = 20
	b.i = 30
	tx = transaction.NewUndoTx()
	retVal, err = tx.Exec(func(tx transaction.TX) {
		tx.Log3(unsafe.Pointer(a), unsafe.Sizeof(*a))
		a.i = a.i + b.i
		a.slice = append(a.slice, 40)
		a.slice = append(a.slice, 50)
	})
	transaction.Release(tx)
	assertEqual(t, err, errNil)
	assertEqual(t, a.i, 50)
	assertEqual(t, len(a.slice), 2)
	assertEqual(t, a.slice[0], 40)
	assertEqual(t, a.slice[1], 50)
	assertEqual(t, len(retVal), 0)

	fmt.Println("Testing with abort before tx.End()")
	a.i = 200
	b.i = 300
	a.slice = pmake([]int, 0)
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(func(tx transaction.TX) {
		tx.Log3(unsafe.Pointer(a), unsafe.Sizeof(*a))
		a.i = a.i + b.i
		a.slice = append(a.slice, 400)

		// Release aborts the transaction because tx.End() is not called yet
		transaction.Release(tx)
	})
	assertEqual(t, err.Error(), errTxBeginEnd.Error())
	assertEqual(t, a.i, 200)
	assertEqual(t, len(a.slice), 0) // Changes rolled back successfully.

	fmt.Printf("Testing when some changes inside function passed to tx.Exec()")
	fmt.Println(" are not logged & there is abort before tx.End()")
	a.i = 2000
	b.i = 3000
	tx = transaction.NewUndoTx()
	_, err = tx.Exec(func(tx transaction.TX) {
		a.i = a.i + b.i
		tx.Log3(unsafe.Pointer(&a.slice), 24) // 24 is the size of slice header
		a.slice = append(a.slice, 4000)

		// Release aborts the transaction because tx.End() is not called yet
		transaction.Release(tx)
	})
	assertEqual(t, err.Error(), errTxBeginEnd.Error())
	assertEqual(t, a.i, 5000)
	assertEqual(t, len(a.slice), 0)

	fmt.Println("Testing simple add function with return value")
	a.i = 3
	b.i = 6
	tx = transaction.NewUndoTx()
	retVal, err = tx.Exec(addRet, a, b)
	transaction.Release(tx)
	assertEqual(t, err, errNil)
	assertEqual(t, len(retVal), 1)
	assertEqual(t, (int)(retVal[0].Int()), 9)

	fmt.Println("Testing anon function with struct ptr return value")
	a.i = 30
	b.i = 60
	tx = transaction.NewUndoTx()
	retVal, err = tx.Exec(func(tx transaction.TX) *basic {
		c := pnew(basic)
		c.slice = pmake([]int, 0)
		tx.Log3(unsafe.Pointer(c), unsafe.Sizeof(*c))
		c.i = a.i + b.i
		c.slice = append(c.slice, 40)
		c.slice = append(c.slice, 50)
		return c
	})
	transaction.Release(tx)
	assertEqual(t, err, errNil)
	assertEqual(t, len(retVal), 1)
	retPtrVal := retVal[0].Elem()
	retSum := (int)(retPtrVal.Field(0).Int())
	retSlice := retPtrVal.Field(1).Slice(0, 2)
	assertEqual(t, retSum, 90)
	assertEqual(t, (int)(retSlice.Index(0).Int()), 40)
	assertEqual(t, (int)(retSlice.Index(1).Int()), 50)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		debug.PrintStack()
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
