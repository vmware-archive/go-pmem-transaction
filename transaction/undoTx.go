///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

/* put log entries and data copies into persistent heap (with type info)
 * to prevent runtime garbage collection to reclaim dangling pointers caused by
 * undo updates.
 * E.g.:
 *     type S struct {
 *         P *int
 *     }
 *     tx := NewUndoTx()
 *     tx.Begin()
 *     tx.Log(&S.P)
 *     S.P = nil
 *     tx.End()
 *     transaction.Release(tx)
 *
 *
 *        | TxHeader |                // Pointer to header passed & stored as
 *    --------- | ------ -----        // part of application pmem root
 *   |          |       |
 *   V          V       V
 *  ---------------------------
 * | logPtr | logPtr |  ...    |      // Stored in pmem. Pointers to logs
 *  ---------------------------
 *     |
 *     |      ---------------------
 *      ---> | entry | entry | ... |  // Stored in pmem to track pointers to
 *            ---------------------   // data copies
 *                  |
 *                  |       -----------
 *                   ----> | data copy |   // Stored in pmem to track pointers
 *                          -----------    // in data copies. e.g., S.P in
 *                                         // previous example
 */

package transaction

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"
)

type (
	undoTx struct {
		log []entry

		// stores the tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// Index of the log handle within undoArray
		index int

		rlocks []*sync.RWMutex
		wlocks []*sync.RWMutex
	}

	undoTxHeader struct {
		magic  int
		logPtr [LOGNUM]*undoTx
	}
)

var (
	txHeaderPtr *undoTxHeader
	undoArray   *bitmap
)

/* Does the first time initialization, else restores log structure and
 * reverts uncommitted logs. Does not do any pointer swizzle. This will be
 * handled by Go-pmem runtime. Returns the pointer to undoTX internal structure,
 * so the application can store it in its pmem appRoot.
 */
func initUndoTx(logHeadPtr unsafe.Pointer) unsafe.Pointer {
	undoArray = newBitmap(LOGNUM)
	if logHeadPtr == nil {
		// First time initialization
		txHeaderPtr = pnew(undoTxHeader)
		for i := 0; i < LOGNUM; i++ {
			txHeaderPtr.logPtr[i] = _initUndoTx(NUMENTRIES, i)
		}
		// Write the magic constant after the transaction handles are persisted.
		// NewUndoTx() can then check this constant to ensure all tx handles
		// are properly initialized before releasing any.
		runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.logPtr),
			unsafe.Sizeof(txHeaderPtr.logPtr))
		txHeaderPtr.magic = MAGIC
		runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.magic),
			unsafe.Sizeof(txHeaderPtr.magic))
		logHeadPtr = unsafe.Pointer(txHeaderPtr)
	} else {
		txHeaderPtr = (*undoTxHeader)(logHeadPtr)
		if txHeaderPtr.magic != MAGIC {
			log.Fatal("undoTxHeader magic does not match!")
		}

		// Recover data from previous pending transactions, if any
		for i := 0; i < LOGNUM; i++ {
			tx := txHeaderPtr.logPtr[i]
			tx.level = 0
			tx.index = i
			tx.wlocks = make([]*sync.RWMutex, 0, 0) // Resetting volatile locks
			tx.rlocks = make([]*sync.RWMutex, 0, 0) // before checking for data
			// The value of tail is unreliable here as it might have been set
			// as 0 during a previous recovery. Hence ask abort() to reallocate
			// the array for the log entries.
			tx.abort(true)
		}
	}

	return logHeadPtr
}

func _initUndoTx(size, index int) *undoTx {
	tx := pnew(undoTx)
	tx.log = pmake([]entry, size)
	runtime.PersistRange(unsafe.Pointer(&tx.log), unsafe.Sizeof(tx.log))
	tx.wlocks = make([]*sync.RWMutex, 0, 0)
	tx.rlocks = make([]*sync.RWMutex, 0, 0)
	tx.index = index
	return tx
}

func NewUndoTx() TX {
	if txHeaderPtr == nil || txHeaderPtr.magic != MAGIC {
		log.Fatal("Undo log not correctly initialized!")
	}
	index := undoArray.nextAvailable()
	return txHeaderPtr.logPtr[index]
}

func releaseUndoTx(t *undoTx) {
	// Reset the pointers in the log entries, but need not allocate a new
	// backing array
	t.level = 0
	if t.tail > 0 {
		t.abort(false)
	}
	undoArray.clearBit(t.index)
}

func (t *undoTx) setTail(tail int) {
	t.tail = tail
	runtime.PersistRange(unsafe.Pointer(&t.tail), unsafe.Sizeof(t.tail))
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (t *undoTx) resetLogTail(realloc bool) {
	tail := t.tail
	runtime.Fence()
	if t.tail != 0 {
		t.setTail(0)
	}

	if realloc {
		// Allocate a new backing array if realloc is true or if the array was
		// expanded to accommodate more log entries.
		t.log = pmake([]entry, NUMENTRIES)
		runtime.PersistRange(unsafe.Pointer(&t.log), unsafe.Sizeof(t.log))
	} else {
		// Zero out the pointers in the log entries so that the data pointed by
		// them will be garbage collected.
		for i := 0; i < tail; i++ {
			t.log[i].ptr = nil
			t.log[i].data = nil
			runtime.FlushRange(unsafe.Pointer(&t.log[i].ptr), 2*unsafe.Sizeof(t.log[i].ptr))
		}
		runtime.Fence()
	}
}

// Also takes care of increasing the number of entries underlying log can hold
func (t *undoTx) increaseLogTail() {
	tail := t.tail + 1 // new tail
	if tail >= cap(t.log) {
		newE := 2 * cap(t.log) // Double number of entries which can be stored
		newLog := pmake([]entry, newE)
		copy(newLog, t.log)
		runtime.PersistRange(unsafe.Pointer(&newLog[0]),
			uintptr(newE)*unsafe.Sizeof(newLog[0])) // Persist new log
		t.log = newLog
		// flush, as there is a fence immediately after this
		runtime.FlushRange(unsafe.Pointer(&t.log), unsafe.Sizeof(t.log))
	}
	// common case
	runtime.Fence() // Required as Log() does not issue any store fence
	t.setTail(tail)
}

type value struct {
	typ  unsafe.Pointer
	ptr  unsafe.Pointer
	flag uintptr
}

// sliceHeader is the datastructure representation of a slice object
type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func (t *undoTx) ReadLog(intf ...interface{}) (retVal interface{}) {
	if len(intf) == 2 {
		return t.readSliceElem(intf[0], intf[1].(int))
	} else if len(intf) == 3 {
		return t.readSlice(intf[0], intf[1].(int), intf[2].(int))
	} else if len(intf) != 1 {
		panic("[undoTx] ReadLog: Incorrect number of args passed")
	}
	ptrV := reflect.ValueOf(intf[0])
	switch ptrV.Kind() {
	case reflect.Ptr:
		data := reflect.Indirect(ptrV)
		retVal = data.Interface()
	default:
		panic("[undoTx] ReadLog: Arg must be pointer")
	}
	return retVal
}

func (t *undoTx) readSliceElem(slicePtr interface{}, index int) interface{} {
	ptrV := reflect.ValueOf(slicePtr)
	s := reflect.Indirect(ptrV) // get to the reflect.Value of slice first
	return s.Index(index).Interface()
}

func (t *undoTx) readSlice(slicePtr interface{}, stIndex int,
	endIndex int) interface{} {
	ptrV := reflect.ValueOf(slicePtr)
	s := reflect.Indirect(ptrV) // get to the reflect.Value of slice first
	return s.Slice(stIndex, endIndex).Interface()
}

// TODO: Logging slice of slice not supported
func (t *undoTx) Log(intf ...interface{}) error {
	doUpdate := false
	if len(intf) == 2 { // If method is invoked with syntax Log(ptr, data),
		// this method will do (*ptr = data) operation too
		doUpdate = true
	} else if len(intf) != 1 {
		return errors.New("[undoTx] Log: Incorrectly called. Correct usage: " +
			"Log(ptr, data) OR Log(ptr) OR Log(slice)")
	}
	v1 := reflect.ValueOf(intf[0])
	if !runtime.InPmem(v1.Pointer()) {
		if doUpdate {
			updateVar(v1, reflect.ValueOf(intf[1]))
		}
		return errors.New("[undoTx] Log: Can't log data in volatile memory")
	}
	switch v1.Kind() {
	case reflect.Slice:
		t.logSlice(v1)
	case reflect.Ptr:
		tail := t.tail
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		typ := oldv.Type()
		if oldv.Kind() == reflect.Slice {
			// store size of each slice element. Used later during t.End()
			t.log[tail].sliceElemSize = int(typ.Elem().Size())
		}
		size := int(typ.Size())
		v2 := reflect.PNew(oldv.Type())
		reflect.Indirect(v2).Set(oldv) // copy old data

		// Append data to log entry.
		t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to orignal data
		t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
		t.log[tail].size = size                         // size of data

		// Flush logged data copy and entry.
		runtime.FlushRange(t.log[tail].data, uintptr(size))
		runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
			unsafe.Sizeof(t.log[tail]))

		// Update log offset in header.
		t.increaseLogTail()
		if oldv.Kind() == reflect.Slice {
			// Pointer to slice was passed to Log(). So, log slice elements too
			t.logSlice(oldv)
		}
	default:
		debug.PrintStack()
		return errors.New("[undoTx] Log: data must be pointer/slice")
	}
	if doUpdate {
		// Do the actual a = b operation here
		updateVar(v1, reflect.ValueOf(intf[1]))
	}
	return nil
}

func updateVar(ptr, data reflect.Value) {
	if ptr.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("[undoTx] updateVar: Updating data pointed by"+
			"ptr=%T not allowed", ptr))
	}
	oldV := reflect.Indirect(ptr)
	if oldV.Kind() != reflect.Slice { // ptr.Kind() must be reflect.Ptr here
		if data.Kind() == reflect.Invalid {
			// data has <nil> value
			z := reflect.Zero(oldV.Type())
			reflect.Indirect(ptr).Set(z)
		} else {
			reflect.Indirect(ptr).Set(data)
		}
		return
	}
	// must be slice header update
	if data.Kind() != reflect.Slice {
		panic(fmt.Sprintf("[undoTx] updateVar: Don't know how to log data type"+
			"ptr=%T, data = %T", oldV, data))
	}
	sourceVal := (*value)(unsafe.Pointer(&oldV))
	sshdr := (*sliceHeader)(sourceVal.ptr) // source slice header
	vptr := (*value)(unsafe.Pointer(&data))
	dshdr := (*sliceHeader)(vptr.ptr) // data slice header
	*sshdr = *dshdr
}

func (t *undoTx) logSlice(v1 reflect.Value) {
	typ := v1.Type()
	v1len := v1.Len()
	size := v1len * int(typ.Elem().Size())
	v2 := reflect.PMakeSlice(typ, v1len, v1len)
	vptr := (*value)(unsafe.Pointer(&v2))
	vshdr := (*sliceHeader)(vptr.ptr)
	sourceVal := (*value)(unsafe.Pointer(&v1))
	sshdr := (*sliceHeader)(sourceVal.ptr)
	if size != 0 {
		sourcePtr := (*[MAXINT]byte)(sshdr.data)[:size:size]
		destPtr := (*[MAXINT]byte)(vshdr.data)[:size:size]
		copy(destPtr, sourcePtr)
	}
	tail := t.tail
	t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	t.log[tail].size = size                         // size of data

	// Flush logged data copy and entry.
	runtime.FlushRange(t.log[tail].data, uintptr(size))
	runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
		unsafe.Sizeof(t.log[tail]))

	// Update log offset in header.
	t.increaseLogTail()
}

/* Exec function receives a variable number of interfaces as its arguments.
 * Usage: Exec(fn_name, fn_arg1, fn_arg2, ...)
 * Function fn_name() should not return anything.
 * No need to Begin() & End() transaction separately if Exec() is used.
 * Caveat: All locks within function fn_name(fn_arg1, fn_arg2, ...) should be
 * taken before making Exec() call. Locks should be released after Exec() call.
 */
func (t *undoTx) Exec(intf ...interface{}) (retVal []reflect.Value, err error) {
	if len(intf) < 1 {
		return retVal,
			errors.New("[undoTx] Exec: Must have atleast one argument")
	}
	fnPosInInterfaceArgs := 0
	fn := reflect.ValueOf(intf[fnPosInInterfaceArgs]) // The function to call
	if fn.Kind() != reflect.Func {
		return retVal,
			errors.New("[undoTx] Exec: 1st argument must be a function")
	}
	fnType := fn.Type()
	// Populate the arguments of the function correctly
	argv := make([]reflect.Value, fnType.NumIn())
	if len(argv) != len(intf) {
		return retVal, errors.New("[undoTx] Exec: Incorrect no. of args in " +
			"function passed to Exec")
	}
	for i := range argv {
		if i == fnPosInInterfaceArgs {
			// Add t *undoTx as the 1st argument to be passed to the function
			// fn. This is not passed by the application when it calls Exec().
			argv[i] = reflect.ValueOf(t)
		} else {
			// get the arguments to the function call from the call to Exec()
			// and populate in argv
			if reflect.TypeOf(intf[i]) != fnType.In(i) {
				return retVal, errors.New("[undoTx] Exec: Incorrect type of " +
					"args in function passed to Exec")
			}
			argv[i] = reflect.ValueOf(intf[i])
		}
	}
	t.Begin()
	defer func() {
		if err == nil {
			err = t.End()
		} else {
			t.End() // Prevent overwriting of error if it is non-nil
		}
	}()
	txLevel := t.level
	retVal = fn.Call(argv)
	if txLevel != t.level {
		return retVal, errors.New("[undoTx] Exec: Unbalanced Begin() & End() " +
			"calls inside function passed to Exec")
	}
	return retVal, err
}

func (t *undoTx) Begin() error {
	t.level++
	return nil
}

/* Also persists the new data written by application, so application
 * doesn't need to do it separately. For nested transactions, End() call to
 * inner transaction does nothing. Only when the outermost transaction ends,
 * all application data is flushed to pmem.
 */
func (t *undoTx) End() error {
	if t.level == 0 {
		return errors.New("[undoTx] End: no transaction to commit!")
	}
	t.level--
	if t.level == 0 {
		defer t.Unlock()

		// Need to flush current value of logged areas
		for i := t.tail - 1; i >= 0; i-- {
			runtime.FlushRange(t.log[i].ptr, uintptr(t.log[i].size))
			if t.log[i].sliceElemSize != 0 {
				// ptr points to sliceHeader. So, need to persist the slice too
				// TODO: We can discard the original slice & skip in this loop
				shdr := (*sliceHeader)(t.log[i].ptr)
				runtime.FlushRange(shdr.data, uintptr(shdr.len*
					t.log[i].sliceElemSize))
				t.log[i].sliceElemSize = 0 // reset
			}
		}
		if t.tail > 0 {
			t.resetLogTail(false) // discard all logs.
		}
	}
	return nil
}

func (t *undoTx) RLock(m *sync.RWMutex) {
	m.RLock()
	t.rlocks = append(t.rlocks, m)
}

func (t *undoTx) WLock(m *sync.RWMutex) {
	m.Lock()
	t.wlocks = append(t.wlocks, m)
}

func (t *undoTx) Lock(m *sync.RWMutex) {
	t.WLock(m)
}

// Unlock API unlocks all the read and write locks taken so far
func (t *undoTx) Unlock() {
	for i, m := range t.wlocks {
		m.Unlock()
		t.wlocks[i] = nil
	}
	t.wlocks = t.wlocks[:0]

	for i, m := range t.rlocks {
		m.RUnlock()
		t.rlocks[i] = nil
	}
	t.rlocks = t.rlocks[:0]
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (t *undoTx) abort(realloc bool) error {
	defer t.Unlock()
	// Replay undo logs. Order last updates first, during abort
	for i := t.tail - 1; i >= 0; i-- {
		origDataPtr := (*[MAXINT]byte)(t.log[i].ptr)
		logDataPtr := (*[MAXINT]byte)(t.log[i].data)
		original := origDataPtr[:t.log[i].size:t.log[i].size]
		logdata := logDataPtr[:t.log[i].size:t.log[i].size]
		copy(original, logdata)
		runtime.FlushRange(t.log[i].ptr, uintptr(t.log[i].size))
	}
	t.resetLogTail(realloc)
	return nil
}
