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
		tmpBuf [128]byte // A temporary buffer to copy data
		genNum uintptr   // A volatile copy of the generation number

		// An array that holds all pointers found in the logged data in
		// so that they will be found by the GC.
		ptrArray []unsafe.Pointer

		// tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// record which log entries store sliceheader, and store the size of
		// each element in that slice.
		storeSliceHdr []pair

		// Pointer to the array in persistent memory where logged data is stored.
		// first points to the first linked array while curr points to the
		// currently used array which may or may not be equal to first.
		first *uLogData
		curr  *uLogData

		// list of log entries which need not be flushed during transaction's
		// successful end.
		skipList []int
		rlocks   []*sync.RWMutex
		wlocks   []*sync.RWMutex
		fs       flushSt
	}

	// Actual undo log data residing in persistent memory
	uLogData struct {
		log    []byte
		genNum uintptr // generation number of the logged data
		next   *uLogData
	}

	undoTxHeader struct {
		magic   int
		logData [logNum]uLogData
	}
)

var (
	txHeaderPtr *undoTxHeader
	undoArray   *bitmap
	// zeroes is used to memset a cacheline size region of memory. It is sized
	// at 128 bytes as we want a 64-byte aligned region somewhere inside zeroes.
	zeroes   [128]byte
	uHandles [logNum]undoTx
)

const (
	// Initial size of the undo log buffer in persistent memory
	uLogInitSize = 65536

	// Header data size before every undo log entry. Each undo log entry header
	// has the layout shown below, and occupies 24 bytes.
	// +--------+------+---------+-----------+
	// | genNum | size | Pointer |    Data   |
	// |   8    |   8  |    8    |  var-size |
	// +--------+------+---------+-----------+
	uLogHdrSize = 24
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
			handle := &txHeaderPtr.logData[i]
			uHandles[i].first = handle
			uHandles[i].genNum = handle.genNum
			uHandles[i].abort()
		}
	}

	return logHeadPtr
}

func initUndoHandles() {
	for i := 0; i < logNum; i++ {
		handle := &txHeaderPtr.logData[i]
		handle.genNum = 1
		handle.log = pmake([]byte, uLogInitSize)
		uHandles[i].first = handle
		uHandles[i].curr = handle
		uHandles[i].genNum = 1
	}

	runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.logData),
		logNum*unsafe.Sizeof(txHeaderPtr.logData[0]))
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
	t.abort()
	index := (uintptr(unsafe.Pointer(t)) - uintptr(unsafe.Pointer(&uHandles[0]))) /
		unsafe.Sizeof(uHandles[0])
	undoArray.clearBit(int(index))
}

func (t *undoTx) setTail(tail int) {
	t.tail = tail
}

// resetLogData makes the undo handle point to the first element of the linked
// list of undo logs. The pointer array is zeroed out to ensure those pointers
// can get garbage collected.
func (t *undoTx) resetLogData() {
	t.curr = t.first
	t.tail = 0

	// Zero out ptrArray
	len := len(t.ptrArray)
	if len == 0 {
		return
	}
	t.ptrArray[0] = nil
	for i := 1; i < len; i *= 2 {
		copy(t.ptrArray[i:], t.ptrArray[:i])
	}
	t.ptrArray = t.ptrArray[:0]
}

// increaseLogTail creates an undo log buffer of size at least toAdd bytes and
// links it to the existing linked list of log buffers. It also tries to reuse
// any existing buffers that has sufficient capacity.
func (t *undoTx) increaseLogTail(toAdd int) {
	uData := t.curr

	// We need at least toAdd bytes and current buffer do not have that much
	// space available.
	newCap := cap(uData.log) * 2
	if toAdd > newCap {
		// make sure newCap is a multiple of 64
		newCap = (64 * (toAdd + 63) / 64)
	}

	// Check if there is a linked log array with capacity at least 'toAdd'.
	if uData.next != nil {
		nextData := uData.next
		if cap(nextData.log) >= newCap {
			t.curr = nextData
			t.tail = 0
			return
		}
	}

	newLog := pnew(uLogData)
	newLog.log = pmake([]byte, newCap)
	runtime.PersistRange(unsafe.Pointer(newLog), 24)

	uData.next = newLog
	runtime.PersistRange(unsafe.Pointer(&uData.next), 8)
	t.curr = newLog
	t.tail = 0
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

// Log3 logs data in a linked list of byte arrays. 'src' is the pointer to the
// data to be logged and 'size' is the number of bytes to log.
func (t *undoTx) Log3(src unsafe.Pointer, size uintptr) error {
	uData := t.curr
	tail := t.tail
	sizeBackup := size
	srcU := uintptr(src)

	// Number of bytes we need to log this entry. It is size + uLogHdrSize,
	// rounded up to cache line size
	logSize := int((size+uLogHdrSize+63)/cacheSize) * cacheSize
	if tail+logSize > cap(uData.log) {
		t.increaseLogTail(logSize)
		uData = t.curr
		tail = t.tail
	}

	// Create a volatile copy of all pointers in this object.
	t.ptrArray = runtime.CollectPtrs(uintptr(src), int(size), t.ptrArray)

	tmpBuf := unsafe.Pointer(&t.tmpBuf[0])
	diff := uintptr(tmpBuf) % cacheSize
	if diff != 0 {
		diff = cacheSize - diff
		tmpBuf = unsafe.Pointer(&t.tmpBuf[diff])
	}

	// Append data to log entry
	// copy 64 bytes at a time

	genPtr := (*uintptr)(tmpBuf)
	szPtr := (*uintptr)(unsafe.Pointer(&t.tmpBuf[diff+8]))
	origPtr := (*uintptr)(unsafe.Pointer(&t.tmpBuf[diff+16]))
	dataPtr := unsafe.Pointer(&t.tmpBuf[diff+24])

	*genPtr = t.genNum
	*szPtr = size
	*origPtr = uintptr(src)
	sz := uintptr(cacheSize - uLogHdrSize)
	if size < sz {
		sz = size
	}

	// copy the first sz bytes
	destSlc := (*(*[maxInt]byte)(dataPtr))[:sz]
	srcSlc := (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:sz]
	copy(destSlc, srcSlc)
	srcU += sz
	size -= sz
	movnt(unsafe.Pointer(&uData.log[tail]), tmpBuf, cacheSize)
	tail += cacheSize

	// Copy remaining 64 byte aligned entries
	for size >= cacheSize {
		destSlc = (*(*[maxInt]byte)(tmpBuf))[:]
		srcSlc = (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:cacheSize]
		copy(destSlc, srcSlc)
		movnt(unsafe.Pointer(&uData.log[tail]), tmpBuf, cacheSize)
		size -= cacheSize
		srcU += cacheSize
		tail += cacheSize
	}

	// Copy the final < 64-byte entry
	sz = size
	if sz > 0 {
		destSlc = (*(*[maxInt]byte)(tmpBuf))[:]
		srcSlc = (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:sz]
		copy(destSlc, srcSlc)
		movnt(unsafe.Pointer(&uData.log[tail]), tmpBuf, cacheSize)
		tail += cacheSize
	}

	runtime.Fence() // Fence after movnt
	t.fs.insert(uintptr(src), sizeBackup)
	t.tail = tail

	return nil
}

func (t *undoTx) Log2(src, dst unsafe.Pointer, size uintptr) error {
	log.Fatal("Log2() not implemented")
	return nil
}

// TODO: Logging slice of slice not supported
func (t *undoTx) Log(intf ...interface{}) error {
	log.Fatal("Log() not implemented")
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

		/* TODO
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
		*/

		// Update log offset in header.
		t.increaseLogTail(0) // TODO
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

	/* TODO
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
	*/

	// Update log offset in header.
	t.increaseLogTail(0) // TODO
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
		t.genNum++
		t.first.genNum = t.genNum
		runtime.PersistRange(unsafe.Pointer(&t.first.genNum), 8)
		t.resetLogData() // discard all logs.
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

// abort() aborts the ongoing undo transaction. It reverts the changes made by
// the application by copying the logged copy of the data to its original
// location in persistent memory.
func (t *undoTx) abort() error {
	defer t.unLock()
	t.level = 0
	uData := t.first

	// Aborting log entries has to be done from last to first. Since this is
	// difficult when using a linked list of array, undoEntries first builds
	// a list of pointers at which each entry begins. These are then aborted in
	// the inverse order in the next step
	var undoEntries []uintptr

	for uData != nil {
		off := 0
		for off+64 <= len(uData.log) {
			logBuf := unsafe.Pointer(&uData.log[off])
			genPtr := (*uintptr)(logBuf)
			if *genPtr != t.genNum {
				// The generation number of this entry does not match the
				// current active generation number. So this is not a valid
				// log entry.
				break
			}

			sizePtr := (unsafe.Pointer(uintptr(logBuf) + ptrSize))
			size := *(*uintptr)(sizePtr)
			undoEntries = append(undoEntries, uintptr(sizePtr))
			toAdd := int((size+uLogHdrSize+63)/64) * 64
			off += toAdd
		}
		uData = uData.next
	}

	for j := len(undoEntries) - 1; j >= 0; j-- {
		entry := undoEntries[j]
		size := *(*uintptr)(unsafe.Pointer(entry))
		origPtr := *(*uintptr)(unsafe.Pointer(entry + ptrSize))
		dataPtr := unsafe.Pointer(entry + 16)
		origData := (*[maxInt]byte)(unsafe.Pointer(origPtr))
		logData := (*[maxInt]byte)(dataPtr)
		copy(origData[:size], logData[:])
		runtime.FlushRange(unsafe.Pointer(origPtr), size)
	}
	runtime.Fence()

	t.first.genNum++
	runtime.PersistRange(unsafe.Pointer(&t.first.genNum), ptrSize)
	t.genNum = t.first.genNum
	t.resetLogData()
	return nil
}
