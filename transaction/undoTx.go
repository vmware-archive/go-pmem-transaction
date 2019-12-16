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
		tmpBuf   [128]byte        // cacheline sized and aligned buffer
		ptrArray []unsafe.Pointer // all pointers within this handle
		genNum   uintptr          // A volatile copy of the generation number

		// tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// record which log entries store sliceheader, and store the size of
		// each element in that slice.
		//storeSliceHdr []pair

		first *uLogData
		txp   *uLogData // Would always be pointing to the newest struct

		// list of log entries which need not be flushed during transaction's
		// successful end.
		//skipList []int
		rlocks []*sync.RWMutex
		wlocks []*sync.RWMutex
		fs     flushSt
	}

	undoTx struct {
		log []byte
	}

	uLogData struct {
		log    []byte  // the third component of slice header is a pointerx
		genNum uintptr // Current active generation number
		next   *uLogData
	}

	undoTxHeader struct {
		magic     int
		logHandle [logNum]uLogData
	}
)

var (
	txHeaderPtr *undoTxHeader
	undoArray   *bitmap
	zeroes      [64]byte // Go guarantees that variables will be zeroed out
	junkVal     [32]byte // TODO - to delete
	tVData      [logNum]vData
)

const (
	undoLogInitSize = 65536
	// Header data size before every undo log entry. Each undo log entry header
	// has the layout shown below, and occupies 24 bytes.
	// +--------+------+---------+-----------+
	// | genNum | size | Pointer | Data .... |
	// |   8    |   8  |    8    | var size  |
	// +--------+------+---------+-----------+
	//
	undoLogHdrSize = 24
)

/*
func init() {
	println("tvdata allocated at ", unsafe.Pointer(&tVData[0]))
	println("size of one tvdata = ", unsafe.Sizeof(tVData[0]))
	println("Total size of tvdata = ", unsafe.Sizeof(tVData)*logNum)
	println("for each vdata, its tmpbuf align is : ")
	for i := 0; i < logNum; i++ {
		print(uintptr(unsafe.Pointer(&tVData[i].tmpBuf[0]))%64, " ")
	}
	println(" ")
}
*/

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
			handle := &txHeaderPtr.logHandle[i]
			tVData[i].first = handle
			tVData[i].genNum = handle.genNum
			// Reallocate the array for the log entries. TODO: How does this
			// change with tail not in pmem?
			tVData[i].abort(true)
		}
	}
	return logHeadPtr
}

func initUndoHandles() {
	for i := 0; i < logNum; i++ {
		handle := &txHeaderPtr.logHandle[i]
		handle.genNum = 1
		handle.log = pmake([]byte, undoLogInitSize)
		tVData[i].txp = handle
		tVData[i].first = handle
		tVData[i].genNum = 1
	}
	runtime.PersistRange(unsafe.Pointer(&txHeaderPtr.logHandle),
		logNum*unsafe.Sizeof(txHeaderPtr.logHandle[0]))
}

func NewUndoTx() TX {
	if txHeaderPtr == nil || txHeaderPtr.magic != magic {
		log.Fatal("Undo log not correctly initialized!")
	}
	index := undoArray.nextAvailable()
	return &tVData[index]
}

func releaseUndoTx(tv *vData) {
	// Reset the pointers in the log entries, but need not allocate a new
	// backing array
	tv.abort(false)
	index := (uintptr(unsafe.Pointer(tv)) - uintptr(unsafe.Pointer(&tVData[0]))) /
		unsafe.Sizeof(tVData[0])
	undoArray.clearBit(int(index))
}

func (tv *vData) setTail(tail int) {
	tv.tail = tail
}

// The realloc parameter indicates if the backing array for the log entries
// need to be reallocated.
func (tv *vData) resetLogData(realloc bool) {

	// IF NEXT IS NOT NIL, THEN WE CAN JUST KEEP REUSING..
	tv.txp = tv.first

	// Zero out ptrArray
	///*
	len := len(tv.ptrArray)
	src := (*(*[maxInt]byte)(unsafe.Pointer(&zeroes[0])))[:]
	zeroed := 0
	for len > 0 {
		sz := 8 // 8 pointers = 64 bytes
		if len < 8 {
			sz = len
		}
		dest := (*(*[maxInt]byte)(unsafe.Pointer(&tv.ptrArray[zeroed])))[:sz]
		copy(dest, src)
		len -= sz
		zeroed += sz
	}
	tv.ptrArray = tv.ptrArray[:0]
	// */

	// TODO -- we are not trimming the log array
	tv.tail = 0

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
func (tv *vData) increaseLogTail(toAdd int) {
	txn0 := tv.txp

	// We need at least toAdd bytes and current buffer do not have that much
	// space available.
	newCap := cap(txn0.log) * 2
	if toAdd > newCap {
		// make sure newCap is a multiple of 64
		newCap = (64 * (toAdd + 63) / 64)
	}

	if txn0.next != nil {
		nextLog := txn0.next
		if cap(nextLog.log) >= newCap {
			tv.txp = nextLog
			tv.tail = 0
			return
		}
	}

	//println("tv ", unsafe.Pointer(tv), " increasing from ", cap(txn0.log), " to ", 2*cap(txn0.log))

	newLog := pnew(uLogData)
	newLog.log = pmake([]byte, newCap)
	runtime.PersistRange(unsafe.Pointer(newLog), 24)

	txn0.next = newLog
	runtime.PersistRange(unsafe.Pointer(&txn0.next), 8)
	tv.txp = newLog
	tv.tail = 0
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
	//txn0 := tv.txp
	if len(intf) == 2 {
		return nil
		//return txn0.readSliceElem(intf[0], intf[1].(int))
	} else if len(intf) == 3 {
		return nil
		//return txn0.readSlice(intf[0], intf[1].(int), intf[2].(int))
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
	txn0 := tv.txp
	tail := tv.tail
	sizeBackup := size
	srcU := uintptr(src)
	tmpBuf := unsafe.Pointer(&tv.tmpBuf[0])

	// Number of bytes we need to log this entry. It is size + undoLogHdrSize,
	// rounded up to cache line size
	toAdd := int((size+undoLogHdrSize+63)/cacheSize) * cacheSize
	if tail+toAdd > cap(txn0.log) { // READS CACHELINE
		tv.increaseLogTail(toAdd)
		txn0 = tv.txp
	}

	// Create a volatile copy of all pointers in this object.
	tv.ptrArray = runtime.LogAddPtrs(uintptr(src), int(size), tv.ptrArray)

	// TO DELETE
	diff := uintptr(tmpBuf) % cacheSize
	if diff != 0 {
		diff = cacheSize - diff
		tmpBuf = unsafe.Pointer(&tv.tmpBuf[diff])
	}

	// Append data to log entry.

	// copy 64 bytes at a time

	genPtr := (*uintptr)(tmpBuf)
	szPtr := (*uintptr)(unsafe.Pointer(&tv.tmpBuf[diff+8]))
	origPtr := (*uintptr)(unsafe.Pointer(&tv.tmpBuf[diff+16]))
	dataPtr := unsafe.Pointer(&tv.tmpBuf[diff+24])

	*genPtr = tv.genNum
	*szPtr = size
	*origPtr = uintptr(src)
	sz := uintptr(cacheSize - undoLogHdrSize)
	if size < sz {
		sz = size
	}

	// copy the first sz bytes
	destSlc := (*(*[maxInt]byte)(dataPtr))[:sz]
	srcSlc := (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:sz]
	copy(destSlc, srcSlc)
	srcU += sz
	size -= sz

	// memset the remain bytes if any
	/*
		rem := cacheSize - sz - 16
		if rem {
			destSlc = (*(*[maxInt]byte)(dataPtr))[:]
			srcSlc = (*(*[maxInt]byte)(unsafe.Pointer(&zeroes[0])))[:rem]
			copy(destSlc, srcSlc)
		}
	*/
	movnt(unsafe.Pointer(&txn0.log[tail]), tmpBuf, cacheSize)
	/*if uintptr(unsafe.Pointer(&t.log[tail]))%cacheSize != 0 {
		log.Fatal("Unaligned address 1")
	}*/
	tail += cacheSize

	// Copy remaining 64 byte aligned entries
	for size >= cacheSize {
		destSlc = (*(*[maxInt]byte)(tmpBuf))[:]
		srcSlc = (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:cacheSize]
		copy(destSlc, srcSlc)

		movnt(unsafe.Pointer(&txn0.log[tail]), tmpBuf, cacheSize)
		if uintptr(unsafe.Pointer(&txn0.log[tail]))%cacheSize != 0 {
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
		destSlc = (*(*[maxInt]byte)(tmpBuf))[:]
		srcSlc = (*(*[maxInt]byte)(unsafe.Pointer(srcU)))[:sz]
		copy(destSlc, srcSlc)

		/*
			rem = cacheSize - sz
			if rem == 0 { // DEBUG TO DELETE
				log.Fatal("Invalid size")
			}
		*/
		if uintptr(unsafe.Pointer(&txn0.log[tail]))%cacheSize != 0 {
			log.Fatal("Unaligned address 3")
		}
		movnt(unsafe.Pointer(&txn0.log[tail]), tmpBuf, cacheSize)
		tail += cacheSize
	}

	// Fence after movnt
	runtime.Fence()
	tv.fs.insert(uintptr(src), sizeBackup)

	tv.tail = tail

	return nil
}

func (tv *vData) Log2(src, dst unsafe.Pointer, size uintptr) error {
	log.Fatal("Log2() not implemented")
	txn0 := tv.txp
	tail := tv.tail
	// Append data to log entry.
	movnt(dst, src, size)
	movnt(unsafe.Pointer(&txn0.log[tail]), unsafe.Pointer(&entry{src, dst, int(size), 1}), unsafe.Sizeof(txn0.log[tail]))

	// Fence after movnt
	runtime.Fence()
	tv.fs.insert(uintptr(src), size)
	tv.increaseLogTail(0)
	return nil
}

// TODO: Logging slice of slice not supported
func (tv *vData) Log(intf ...interface{}) error {
	log.Fatal("Log() not implemented")
	//txn0 := tv.txp
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
		//tail := tv.tail
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		//typ := oldv.Type()
		if oldv.Kind() == reflect.Slice {
			// record this log entry and store size of each slice element.
			// Used later during End()
			//tv.storeSliceHdr = append(tv.storeSliceHdr, pair{tail, int(typ.Elem().Size())})
		}
		/* TODO
		size := int(typ.Size())
		v2 := reflect.PNew(oldv.Type())
		reflect.Indirect(v2).Set(oldv) // copy old data
			// Append data to log entry.
			txn0.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to orignal data
			txn0.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
			txn0.log[tail].genNum = 1
			txn0.log[tail].size = size // size of data

			// Flush logged data copy and entry.
			runtime.FlushRange(t.log[tail].data, uintptr(size))
			runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
				unsafe.Sizeof(t.log[tail]))
		*/
		// Update log offset in header.
		tv.increaseLogTail(0)
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
	txn0.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	txn0.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	txn0.log[tail].genNum = 1
	txn0.log[tail].size = size // size of data

	if !flushAtEnd {
		// This happens when sliceheader is logged. User can then update
		// the sliceheader and/or the slice contents. So, when sliceheader is
		// flushed in End(), new slice contents also must be flushed in End().
		// So this log entry containing old slice contents don't have to be
		// flushed in End(). Add this to list of entries to be skipped.
		tv.skipList = append(tv.skipList, tail)
	}

		// Flush logged data copy and entry.
		runtime.FlushRange(txn0.log[tail].data, uintptr(size))
		runtime.FlushRange(unsafe.Pointer(&txn0.log[tail]),
			unsafe.Sizeof(txn0.log[tail]))
	*/
	// Update log offset in header.
	tv.increaseLogTail(0)
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

	tv.level--
	if tv.level == 0 {
		defer tv.unLock()
		tv.fs.flushAndDestroy()

		tv.genNum++
		tv.first.genNum = tv.genNum
		runtime.PersistRange(unsafe.Pointer(&tv.first.genNum), 8)
		// Can also zero out the volatile pointers here (tv.ptrArray)
		tv.resetLogData(false) // discard all logs.
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
	txn0 := tv.first // CHECK

	for txn0 != nil {
		off := 0
		for off+64 <= len(txn0.log) {
			logBuf := unsafe.Pointer(&txn0.log[off])
			genPtr := (*uintptr)(logBuf)
			if *genPtr != tv.genNum {
				break
			}
			size := *(*uintptr)(unsafe.Pointer(&txn0.log[off+8]))
			origPtr := *(*uintptr)(unsafe.Pointer(&txn0.log[off+16]))
			dataPtr := unsafe.Pointer(&txn0.log[off+24])

			origData := (*[maxInt]byte)(unsafe.Pointer(origPtr))
			logData := (*[maxInt]byte)(dataPtr)
			copy(origData[:size], logData[:])
			runtime.FlushRange(unsafe.Pointer(origPtr), size)

			toAdd := int((size+undoLogHdrSize+63)/64) * 64
			off += toAdd
		}
		txn0 = txn0.next
	}
	runtime.Fence()

	tv.first.genNum++
	runtime.PersistRange(unsafe.Pointer(&tv.first.genNum), 8)
	tv.genNum = tv.first.genNum
	tv.resetLogData(realloc)
	return nil
}
