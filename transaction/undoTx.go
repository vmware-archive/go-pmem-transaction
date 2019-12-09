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
	// Volatile metadata associated with each transaction handle. These are
	// helpful in the common case when transaction ends successfully.
	vData struct {
		tmpBuf   [64]byte         // cacheline sized and aligned buffer
		ptrArray []unsafe.Pointer // all pointers within this handle

		// tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// record which log entries store sliceheader, and store the size of
		// each element in that slice.
		storeSliceHdr []pair

		// list of log entries which need not be flushed during transaction's
		// successful end.
		skipList []int
		rlocks   []*sync.RWMutex
		wlocks   []*sync.RWMutex
		fs       *flushSt
		index    int
		txp      *undoTx
		junk     [32]byte //  to make tmpBuf cache aligned
	}

	undoTx struct {
		log []byte
		// Index of the log handle within undoArray
	}

	undoTxHeader struct {
		magic  int
		logPtr [logNum]*undoTx
	}
)

var (
	txHeaderPtr *undoTxHeader
	undoArray   *bitmap
	zeroes      [64]byte // Go guarantees that variables will be zeroed out
	tVData      [logNum]*vData
)

const (
	undoLogInitSize = 65536
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
			ptr := _initUndoTx(undoLogInitSize, i)
			txHeaderPtr.logPtr[i] = ptr
			tVData[i].txp = ptr
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
			tVData[i] = new(vData)
			tVData[i].txp = tx
			tVData[i].index = i
			// Reallocate the array for the log entries. TODO: How does this
			// change with tail not in pmem?
			tVData[i].abort(true)
		}
	}

	return logHeadPtr
}

func _initUndoTx(size, index int) *undoTx {
	tx := pnew(undoTx)
	tx.log = pmake([]byte, size)
	runtime.PersistRange(unsafe.Pointer(&tx.log), unsafe.Sizeof(tx.log))
	resetVData(index)
	return tx
}

func NewUndoTx() TX {
	if txHeaderPtr == nil || txHeaderPtr.magic != magic {
		log.Fatal("Undo log not correctly initialized!")
	}
	index := undoArray.nextAvailable()
	return tVData[index]
}

func releaseUndoTx(tv *vData) {
	// Reset the pointers in the log entries, but need not allocate a new
	// backing array
	tv.abort(false)
	undoArray.clearBit(tv.index)
}

func (tv *vData) setTail(tail int) {
	tv.tail = tail
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (t *undoTx) resetLogData(realloc bool) {

	// TODO -- we are not trimming the log array

	if realloc {
		// Allocate a new backing array if realloc is true. After a program
		// crash, we must always reach here
		//t.log = pmake([]byte, undoLogInitSize)
		//runtime.PersistRange(unsafe.Pointer(&t.log), unsafe.Sizeof(t.log))
	} else {
		// Nothing to do
	}
}

// Also takes care of increasing the number of entries underlying log can hold
func (tv *vData) increaseLogTail() {
	txn := tv.txp
	//println("tv ", unsafe.Pointer(tv), " increasing from ", cap(txn.log), " to ", 2*cap(txn.log))
	newE := 2 * cap(txn.log) // Double number of entries which can be stored
	newLog := pmake([]byte, newE)
	copy(newLog, txn.log)
	runtime.PersistRange(unsafe.Pointer(&newLog[0]),
		uintptr(newE)*unsafe.Sizeof(newLog[0])) // Persist new log
	txn.log = newLog
	runtime.PersistRange(unsafe.Pointer(&txn.log), unsafe.Sizeof(txn.log))

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

func (tv *vData) ReadLog(intf ...interface{}) (retVal interface{}) {
	txn := tv.txp
	if len(intf) == 2 {
		return txn.readSliceElem(intf[0], intf[1].(int))
	} else if len(intf) == 3 {
		return txn.readSlice(intf[0], intf[1].(int), intf[2].(int))
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

func (tv *vData) Log3(src unsafe.Pointer, size uintptr) error {
	txn := tv.txp
	tail := tv.tail
	sizeBackup := size
	srcU := uintptr(src)
	tmpBuf := unsafe.Pointer(&tv.tmpBuf[0])

	toAdd := int((size+16+63)/64) * 64
	if tail+toAdd > cap(txn.log) { // READS CACHELINE
		tv.increaseLogTail()
	}

	tv.ptrArray = runtime.LogAddPtrs(uintptr(src), int(size), tv.ptrArray)

	// TO DELETE
	if uintptr(tmpBuf)%64 != 0 {
		println("tmp array address = ", tmpBuf)
		log.Fatal("tmp array is not cache aligned!")
	}

	// Append data to log entry.

	// copy 64 bytes at a time

	szPtr := (*uintptr)(tmpBuf)
	ptrPtr := (*uintptr)(unsafe.Pointer(&tv.tmpBuf[8]))
	dataPtr := unsafe.Pointer(&tv.tmpBuf[16])

	*szPtr = size
	*ptrPtr = uintptr(src)
	sz := uintptr(cacheSize - 16)
	if size < sz {
		sz = size
	}

	// copy the first sz bytes
	destSlc := (*(*[1 << 28]byte)(dataPtr))[:sz]
	srcSlc := (*(*[1 << 28]byte)(unsafe.Pointer(srcU)))[:sz]
	copy(destSlc, srcSlc)
	srcU += sz
	size -= sz

	// memset the remain bytes if any
	/*
		rem := cacheSize - sz - 16
		if rem {
			destSlc = (*(*[1 << 28]byte)(dataPtr))[:]
			srcSlc = (*(*[1 << 28]byte)(unsafe.Pointer(&zeroes[0])))[:rem]
			copy(destSlc, srcSlc)
		}
	*/
	movnt(unsafe.Pointer(&txn.log[tail]), tmpBuf, cacheSize)
	/*if uintptr(unsafe.Pointer(&t.log[tail]))%cacheSize != 0 {
		log.Fatal("Unaligned address 1")
	}*/
	tail += cacheSize

	// Copy remaining 64 byte aligned entries
	for size >= cacheSize {
		destSlc = (*(*[1 << 28]byte)(tmpBuf))[:]
		srcSlc = (*(*[1 << 28]byte)(unsafe.Pointer(srcU)))[:cacheSize]
		copy(destSlc, srcSlc)

		movnt(unsafe.Pointer(&txn.log[tail]), tmpBuf, cacheSize)
		if uintptr(unsafe.Pointer(&txn.log[tail]))%cacheSize != 0 {
			log.Fatal("Unaligned address 2")
		}
		size -= cacheSize
		srcU += cacheSize
		tail += cacheSize
	}

	// Copy the final < 64-byte entry
	sz = size
	/*if size >= cacheSize { // DEBUG : TO DELETE
		log.Fatal("Invalid size")
	}*/
	if sz > 0 {
		destSlc = (*(*[1 << 28]byte)(tmpBuf))[:]
		srcSlc = (*(*[1 << 28]byte)(unsafe.Pointer(srcU)))[:sz]
		copy(destSlc, srcSlc)

		/*
			rem = cacheSize - sz
			if rem == 0 { // DEBUG TO DELETE
				log.Fatal("Invalid size")
			}
		*/
		if uintptr(unsafe.Pointer(&txn.log[tail]))%cacheSize != 0 {
			log.Fatal("Unaligned address 3")
		}
		movnt(unsafe.Pointer(&txn.log[tail]), tmpBuf, cacheSize)
		tail += cacheSize
	}

	// Fence after movnt
	runtime.Fence()
	tv.fs.insert(uintptr(src), sizeBackup)

	//t.setTail(tail)
	tv.tail = tail

	return nil
}

func (tv *vData) Log2(src, dst unsafe.Pointer, size uintptr) error {
	txn := tv.txp
	tail := tv.tail
	// Append data to log entry.
	movnt(dst, src, size)
	movnt(unsafe.Pointer(&txn.log[tail]), unsafe.Pointer(&entry{src, dst, int(size), 1}), unsafe.Sizeof(txn.log[tail]))

	// Fence after movnt
	runtime.Fence()
	tv.fs.insert(uintptr(src), size)
	tv.increaseLogTail()
	return nil
}

// TODO: Logging slice of slice not supported
func (tv *vData) Log(intf ...interface{}) error {
	//txn := tv.txp
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
		tv.logSlice(v1, true)
	case reflect.Ptr:
		tail := tv.tail
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		typ := oldv.Type()
		if oldv.Kind() == reflect.Slice {
			// record this log entry and store size of each slice element.
			// Used later during End()
			tv.storeSliceHdr = append(tv.storeSliceHdr, pair{tail, int(typ.Elem().Size())})
		}
		/* TODO
		size := int(typ.Size())
		v2 := reflect.PNew(oldv.Type())
		reflect.Indirect(v2).Set(oldv) // copy old data
			// Append data to log entry.
			txn.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to orignal data
			txn.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
			txn.log[tail].genNum = 1
			txn.log[tail].size = size // size of data

			// Flush logged data copy and entry.
			runtime.FlushRange(t.log[tail].data, uintptr(size))
			runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
				unsafe.Sizeof(t.log[tail]))
		*/
		// Update log offset in header.
		tv.increaseLogTail()
		if oldv.Kind() == reflect.Slice {
			// Pointer to slice was passed to Log(). So, log slice elements too,
			// but no need to flush these contents in End()
			tv.logSlice(oldv, false)
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

func (tv *vData) logSlice(v1 reflect.Value, flushAtEnd bool) {
	// Don't create log entries, if there is no slice, or slice is not in pmem
	if v1.Pointer() == 0 || !runtime.InPmem(v1.Pointer()) {
		return
	}
	/* TODO
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
	tail := t.v.tail
	txn.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	txn.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	txn.log[tail].genNum = 1
	txn.log[tail].size = size // size of data

	if !flushAtEnd {
		// This happens when sliceheader is logged. User can then update
		// the sliceheader and/or the slice contents. So, when sliceheader is
		// flushed in End(), new slice contents also must be flushed in End().
		// So this log entry containing old slice contents don't have to be
		// flushed in End(). Add this to list of entries to be skipped.
		tv.skipList = append(tv.skipList, tail)
	}

		// Flush logged data copy and entry.
		runtime.FlushRange(txn.log[tail].data, uintptr(size))
		runtime.FlushRange(unsafe.Pointer(&txn.log[tail]),
			unsafe.Sizeof(txn.log[tail]))
	*/
	// Update log offset in header.
	tv.increaseLogTail()
}

/* Exec function receives a variable number of interfaces as its arguments.
 * Usage: Exec(fn_name, fn_arg1, fn_arg2, ...)
 * Function fn_name() should not return anything.
 * No need to Begin() & End() transaction separately if Exec() is used.
 * Caveat: All locks within function fn_name(fn_arg1, fn_arg2, ...) should be
 * taken before making Exec() call. Locks should be released after Exec() call.
 */
func (tv *vData) Exec(intf ...interface{}) (retVal []reflect.Value, err error) {
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
			argv[i] = reflect.ValueOf(tv)
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
	tv.Begin()
	defer tv.End()
	txLevel := tv.level
	retVal = fn.Call(argv)
	if txLevel != tv.level {
		return retVal, errors.New("[undoTx] Exec: Unbalanced Begin() & End() " +
			"calls inside function passed to Exec")
	}
	return retVal, err
}

func (tv *vData) Begin() error {
	tv.level++
	return nil
}

/* Also persists the new data written by application, so application
 * doesn't need to do it separately. For nested transactions, End() call to
 * inner transaction does nothing. Only when the outermost transaction ends,
 * all application data is flushed to pmem. Returns a bool indicating if it
 * is safe to release the transaction handle.
 */
func (tv *vData) End() bool {
	if tv.level == 0 {
		return true
	}
	index := tv.index
	txn := tv.txp

	tv.level--
	if tv.level == 0 {
		defer tv.unLock()
		tv.fs.flushAndDestroy()
		resetVData(index)
		txn.resetLogData(false) // discard all logs.
		runtime.Fence()
		return true
	}
	return false
}

func (tv *vData) RLock(m *sync.RWMutex) {
	m.RLock()
	tv.rlocks = append(tv.rlocks, m)
}

func (tv *vData) WLock(m *sync.RWMutex) {
	m.Lock()
	tv.wlocks = append(tv.wlocks, m)
}

func (tv *vData) Lock(m *sync.RWMutex) {
	tv.WLock(m)
}

func (tv *vData) unLock() {
	for i, m := range tv.wlocks {
		m.Unlock()
		tv.wlocks[i] = nil
	}
	tv.wlocks = tv.wlocks[:0]

	for i, m := range tv.rlocks {
		m.RUnlock()
		tv.rlocks[i] = nil
	}
	tv.rlocks = tv.rlocks[:0]
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (tv *vData) abort(realloc bool) error {
	defer tv.unLock()
	// Replay undo logs. Order last updates first, during abort
	tv.level = 0
	txn := tv.txp

	tail := tv.tail
	off := 0

	for tail > 0 {
		size := *(*uintptr)(unsafe.Pointer(&txn.log[off]))
		origPtr := unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&txn.log[off+8])))

		origData := (*[maxInt]byte)(unsafe.Pointer(origPtr))
		logData := (*[maxInt]byte)(unsafe.Pointer(&txn.log[off+16]))

		copy(origData[:size], logData[:])
		runtime.FlushRange(origPtr, size)

		toAdd := int((size+16+63)/64) * 64
		tail -= toAdd
		off += toAdd
	}

	runtime.Fence()
	txn.resetLogData(realloc)
	resetVData(tv.index)
	return nil
}

func resetVData(index int) {
	txp := unsafe.Pointer(uintptr(0))
	if tVData[index] != nil {
		txp = unsafe.Pointer(tVData[index].txp)
	}
	tVData[index] = new(vData)
	tVData[index].fs = new(flushSt)
	tVData[index].index = index
	tVData[index].txp = (*undoTx)(txp)
}
