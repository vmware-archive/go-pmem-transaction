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
	pair struct {
		first  int
		second int
	}

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

		// record which log entries store sliceheader, and store the size of
		// each element in that slice. This is only used when transaction ends
		// successfully. So this structure is stored in volatile memory.
		storeSliceHdr []pair

		// list of log entries which need not be flushed during transaction's
		// successful end. This structure is stored in volatile memory.
		skipList []int
	}

	undoTxHeader struct {
		magic  int
		logPtr [logNum]*undoTx
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
	undoArray = newBitmap(logNum)
	if logHeadPtr == nil {
		// First time initialization
		txHeaderPtr = pnew(undoTxHeader)
		for i := 0; i < logNum; i++ {
			txHeaderPtr.logPtr[i] = _initUndoTx(NumEntries, i)
		}
		// Write the magic constant after the transaction handles are persisted.
		// NewUndoTx() can then check this constant to ensure all tx handles
		// are properly initialized before releasing any.
		runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.logPtr), logNum*ptrSize)
		txHeaderPtr.magic = magic
		runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.magic), ptrSize)
		logHeadPtr = unsafe.Pointer(txHeaderPtr)
	} else {
		txHeaderPtr = (*undoTxHeader)(logHeadPtr)
		if txHeaderPtr.magic != magic {
			log.Fatal("undoTxHeader magic does not match!")
		}

		// Recover data from previous pending transactions, if any
		for i := 0; i < logNum; i++ {
			tx := txHeaderPtr.logPtr[i]
			tx.index = i
			tx.wlocks = make([]*sync.RWMutex, 0, 0) // Resetting volatile locks
			tx.rlocks = make([]*sync.RWMutex, 0, 0) // before checking for data
			tx.resetVData()
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
	tx.resetVData()
	tx.index = index
	return tx
}

func NewUndoTx() TX {
	if txHeaderPtr == nil || txHeaderPtr.magic != magic {
		log.Fatal("Undo log not correctly initialized!")
	}
	index := undoArray.nextAvailable()
	return txHeaderPtr.logPtr[index]
}

func releaseUndoTx(t *undoTx) {
	// Reset the pointers in the log entries, but need not allocate a new
	// backing array
	t.abort(false)
	undoArray.clearBit(t.index)
}

func (t *undoTx) setTail(tail int) {
	runtime.Fence()
	t.tail = tail
	runtime.PersistRange(unsafe.Pointer(&t.tail), ptrSize)
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (t *undoTx) resetLogTail(realloc bool) {
	tail := t.tail
	if tail != 0 {
		t.setTail(0)
	}

	if realloc {
		// Allocate a new backing array if realloc is true or if the array was
		// expanded to accommodate more log entries.
		t.log = pmake([]entry, NumEntries)
		runtime.PersistRange(unsafe.Pointer(&t.log), unsafe.Sizeof(t.log))
	} else {
		// Zero out the pointers in the log entries so that the data pointed by
		// them will be garbage collected.
		for i := 0; i < tail; i++ {
			t.log[i].ptr = nil
			t.log[i].data = nil
			runtime.FlushRange(unsafe.Pointer(&t.log[i].ptr), 2*ptrSize)
		}
		// Fence can be avoided here because when we log a new entry, we will
		// call a store fence before updating the value of tail. And if we crash
		// at this point, a new log array will be allocated in the restart path.
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
		// There is a fence immediately after this in setTail()
		runtime.FlushRange(unsafe.Pointer(&t.log), unsafe.Sizeof(t.log))
	}
	// common case
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

func (t *undoTx) Log2(src, dst unsafe.Pointer, size uintptr) error {
	tail := t.tail
	// Append data to log entry.
	t.log[tail].ptr = src        // point to orignal data
	t.log[tail].data = dst       // point to logged copy
	t.log[tail].size = int(size) // size of data

	srcByte := (*[maxInt]byte)(src)
	dstByte := (*[maxInt]byte)(dst)
	copy(dstByte[:size], srcByte[:size])

	// Flush logged data copy and entry.
	runtime.FlushRange(dst, size)
	runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
		unsafe.Sizeof(t.log[tail]))

	// Update log offset in header.
	t.increaseLogTail()
	return nil
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
		t.logSlice(v1, true)
	case reflect.Ptr:
		tail := t.tail
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		typ := oldv.Type()
		if oldv.Kind() == reflect.Slice {
			// record this log entry and store size of each slice element.
			// Used later during End()
			t.storeSliceHdr = append(t.storeSliceHdr, pair{tail, int(typ.Elem().Size())})
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
			// Pointer to slice was passed to Log(). So, log slice elements too,
			// but no need to flush these contents in End()
			t.logSlice(oldv, false)
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

func (t *undoTx) logSlice(v1 reflect.Value, flushAtEnd bool) {
	// Don't create log entries, if there is no slice, or slice is not in pmem
	if v1.Pointer() == 0 || !runtime.InPmem(v1.Pointer()) {
		return
	}
	typ := v1.Type()
	v1len := v1.Len()
	size := v1len * int(typ.Elem().Size())
	v2 := reflect.PMakeSlice(typ, v1len, v1len)
	vptr := (*value)(unsafe.Pointer(&v2))
	vshdr := (*sliceHeader)(vptr.ptr)
	sourceVal := (*value)(unsafe.Pointer(&v1))
	sshdr := (*sliceHeader)(sourceVal.ptr)
	if size != 0 {
		sourcePtr := (*[maxInt]byte)(sshdr.data)[:size:size]
		destPtr := (*[maxInt]byte)(vshdr.data)[:size:size]
		copy(destPtr, sourcePtr)
	}
	tail := t.tail
	t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	t.log[tail].size = size                         // size of data

	if !flushAtEnd {
		// This happens when sliceheader is logged. User can then update
		// the sliceheader and/or the slice contents. So, when sliceheader is
		// flushed in End(), new slice contents also must be flushed in End().
		// So this log entry containing old slice contents don't have to be
		// flushed in End(). Add this to list of entries to be skipped.
		t.skipList = append(t.skipList, tail)
	}

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
	defer t.End()
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
 * all application data is flushed to pmem. Returns a bool indicating if it
 * is safe to release the transaction handle.
 */
func (t *undoTx) End() bool {
	if t.level == 0 {
		return true
	}
	t.level--
	if t.level == 0 {
		defer t.unLock()

		var j, k int
		// Need to flush current value of logged areas
		for i := 0; i < t.tail; i++ {
			// If entry recorded in skiplist, do nothing
			if k < len(t.skipList) && t.skipList[k] == i {
				k++
				continue
			}

			runtime.FlushRange(t.log[i].ptr, uintptr(t.log[i].size))

			if j < len(t.storeSliceHdr) && t.storeSliceHdr[j].first == i {
				// ptr points to sliceHeader. So, need to persist the slice too
				// We also skip flushing the original slice
				shdr := (*sliceHeader)(t.log[i].ptr)
				runtime.FlushRange(shdr.data, uintptr(shdr.len*
					t.storeSliceHdr[j].second))
				j++
			}
		}
		t.resetVData()
		t.resetLogTail(false) // discard all logs.
		return true
	}
	return false
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

func (t *undoTx) unLock() {
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
	defer t.unLock()
	// Replay undo logs. Order last updates first, during abort
	t.level = 0
	for i := t.tail - 1; i >= 0; i-- {
		origDataPtr := (*[maxInt]byte)(t.log[i].ptr)
		logDataPtr := (*[maxInt]byte)(t.log[i].data)
		original := origDataPtr[:t.log[i].size:t.log[i].size]
		logdata := logDataPtr[:t.log[i].size:t.log[i].size]
		copy(original, logdata)
		runtime.FlushRange(t.log[i].ptr, uintptr(t.log[i].size))
	}
	t.resetVData()
	t.resetLogTail(realloc)
	return nil
}

func (t *undoTx) resetVData() {
	t.storeSliceHdr = make([]pair, 0, 0)
	t.skipList = make([]int, 0, 0)
}
