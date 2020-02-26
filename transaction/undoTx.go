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

	// Each undo log handle. Contains volatile metadata associated with the handle
	undoTx struct {
		// tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// Index of the log handle within undoArray
		index int

		// record which log entries store sliceheader, and store the size of
		// each element in that slice.
		storeSliceHdr []pair

		// Pointer to persistent data associated with this handle
		data *uLogData

		// list of log entries which need not be flushed during transaction's
		// successful end.
		skipList []int
		rlocks   []*sync.RWMutex
		wlocks   []*sync.RWMutex
		fs       *flushSt
	}

	// Actual undo log data residing in persistent memory
	uLogData struct {
		log []entry
	}

	undoTxHeader struct {
		magic   int
		logData [logNum]*uLogData
	}
)

var (
	txHeaderPtr *undoTxHeader
	undoArray   *bitmap
	uHandles    [logNum]undoTx
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
		initUndoHandles()
		// Write the magic constant after the transaction handles are persisted.
		// NewUndoTx() can then check this constant to ensure all tx handles
		// are properly initialized before releasing any.
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
			handle := txHeaderPtr.logData[i]
			uHandles[i].data = handle
			uHandles[i].index = i
			// Reallocate the array for the log entries. TODO: How does this
			// change with tail not in pmem?
			uHandles[i].abort(true)
			uHandles[i].resetVData()
		}
	}

	return logHeadPtr
}

func initUndoHandles() {
	for i := 0; i < logNum; i++ {
		handle := pnew(uLogData)
		handle.log = pmake([]entry, NumEntries)
		runtime.PersistRange(unsafe.Pointer(&handle.log), unsafe.Sizeof(handle.log))
		txHeaderPtr.logData[i] = handle
		uHandles[i].resetVData()
		uHandles[i].data = handle
		uHandles[i].index = i
	}

	runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.logData), logNum*ptrSize)
}

func NewUndoTx() TX {
	if txHeaderPtr == nil || txHeaderPtr.magic != magic {
		log.Fatal("Undo log not correctly initialized!")
	}
	index := undoArray.nextAvailable()
	return &uHandles[index]
}

func releaseUndoTx(t *undoTx) {
	// Reset the pointers in the log entries, but need not allocate a new
	// backing array
	t.abort(false)
	undoArray.clearBit(t.index)
}

func (t *undoTx) setTail(tail int) {
	t.tail = tail
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (t *undoTx) resetLogData(realloc bool) {
	uData := t.data
	if realloc {
		// Allocate a new backing array if realloc is true. After a program
		// crash, we must always reach here
		uData.log = pmake([]entry, NumEntries)
		runtime.PersistRange(unsafe.Pointer(&uData.log), unsafe.Sizeof(uData.log))
	} else {
		// Zero out the pointers in the log entries so that the data pointed by
		// them will be garbage collected.
		for i := 0; i < len(uData.log); i++ {
			if uData.log[i].genNum == 0 {
				break
			}
			uData.log[i].ptr = nil
			uData.log[i].data = nil
			uData.log[i].genNum = 0
			runtime.FlushRange(unsafe.Pointer(&uData.log[i].ptr), 3*ptrSize)
		}
	}
}

// Also takes care of increasing the number of entries underlying log can hold
func (t *undoTx) increaseLogTail() {
	uData := t.data
	tail := t.tail + 1 // new tail
	if tail >= cap(uData.log) {
		newE := 2 * cap(uData.log) // Double number of entries which can be stored
		newLog := pmake([]entry, newE)
		copy(newLog, uData.log)
		runtime.PersistRange(unsafe.Pointer(&newLog[0]),
			uintptr(newE)*unsafe.Sizeof(newLog[0])) // Persist new log
		uData.log = newLog
		runtime.PersistRange(unsafe.Pointer(&uData.log), unsafe.Sizeof(uData.log))
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
	uData := t.data
	tail := t.tail
	// Append data to log entry.
	movnt(dst, src, size)
	movnt(unsafe.Pointer(&uData.log[tail]),
		unsafe.Pointer(&entry{src, dst, int(size), 1}),
		unsafe.Sizeof(uData.log[tail]))

	// Fence after movnt
	runtime.Fence()
	t.fs.insert(uintptr(src), size)
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
		uData := t.data
		uData.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to orignal data
		uData.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
		uData.log[tail].genNum = 1
		uData.log[tail].size = size // size of data

		// Flush logged data copy and entry.
		runtime.FlushRange(uData.log[tail].data, uintptr(size))
		runtime.FlushRange(unsafe.Pointer(&uData.log[tail]),
			unsafe.Sizeof(uData.log[tail]))

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
	uData := t.data
	uData.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	uData.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	uData.log[tail].genNum = 1
	uData.log[tail].size = size // size of data

	if !flushAtEnd {
		// This happens when sliceheader is logged. User can then update
		// the sliceheader and/or the slice contents. So, when sliceheader is
		// flushed in End(), new slice contents also must be flushed in End().
		// So this log entry containing old slice contents don't have to be
		// flushed in End(). Add this to list of entries to be skipped.
		t.skipList = append(t.skipList, tail)
	}

	// Flush logged data copy and entry.
	runtime.FlushRange(uData.log[tail].data, uintptr(size))
	runtime.FlushRange(unsafe.Pointer(&uData.log[tail]),
		unsafe.Sizeof(uData.log[tail]))

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
		t.fs.flushAndDestroy()
		t.resetVData()
		t.resetLogData(false) // discard all logs.
		runtime.Fence()
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
	uData := t.data
	l := len(uData.log)
	for i := l - 1; i >= 0; i-- {
		if uData.log[i].genNum == 0 { // no data in this log entry. TODO: This can
			// be avoided if we guarantee update to one memory location is stored
			// just once in the log entries. Then, we can loop forward & when
			// we get genNum==0 we can exit the loop.
			continue
		}
		origDataPtr := (*[maxInt]byte)(uData.log[i].ptr)
		logDataPtr := (*[maxInt]byte)(uData.log[i].data)
		original := origDataPtr[:uData.log[i].size:uData.log[i].size]
		logdata := logDataPtr[:uData.log[i].size:uData.log[i].size]
		copy(original, logdata)
		runtime.FlushRange(uData.log[i].ptr, uintptr(uData.log[i].size))
	}
	runtime.Fence()
	t.resetLogData(realloc)
	t.resetVData()
	return nil
}

func (t *undoTx) resetVData() {
	t.tail = 0
	t.level = 0
	t.storeSliceHdr = t.storeSliceHdr[:0]
	t.skipList = t.skipList[:0]
	t.rlocks = t.rlocks[:0]
	t.wlocks = t.wlocks[:0]
	t.fs = new(flushSt)
}
