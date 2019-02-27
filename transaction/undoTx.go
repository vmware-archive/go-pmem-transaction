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

		// num of entries which can be stored in log
		nEntry int
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
	undoPool    chan *undoTx // volatile structure for pointing to logs.
)

/* Does the first time initialization, else restores log structure and
 * reverts uncommitted logs. Does not do any pointer swizzle. This will be
 * handled by Go-pmem runtime. Returns the pointer to undoTX internal structure,
 * so the application can store it in its pmem appRoot.
 */
func initUndoTx(logHeadPtr unsafe.Pointer) unsafe.Pointer {
	if logHeadPtr == nil {
		// First time initialization
		txHeaderPtr = pnew(undoTxHeader)
		txHeaderPtr.magic = MAGIC
		for i := 0; i < LOGNUM; i++ {
			txHeaderPtr.logPtr[i] = _initUndoTx(NUMENTRIES)
		}
		runtime.PersistRange(unsafe.Pointer(txHeaderPtr),
			unsafe.Sizeof(*txHeaderPtr))
		logHeadPtr = unsafe.Pointer(txHeaderPtr)
	} else {
		txHeaderPtr = (*undoTxHeader)(logHeadPtr)
		if txHeaderPtr.magic != MAGIC {
			log.Fatal("undoTxHeader magic does not match!")
		}

		// Recover data from previous pending transactions, if any
		var tx *undoTx
		for i := 0; i < LOGNUM; i++ {
			tx = txHeaderPtr.logPtr[i]
			tx.wlocks = make([]*sync.RWMutex, 0, 0) // Resetting volatile locks
			tx.rlocks = make([]*sync.RWMutex, 0, 0) // before checking for data
			tx.abort()
		}
	}
	undoPool = make(chan *undoTx, LOGNUM)
	for i := 0; i < LOGNUM; i++ {
		undoPool <- txHeaderPtr.logPtr[i]
	}
	return logHeadPtr
}

func _initUndoTx(size int) *undoTx {
	tx := pnew(undoTx)
	tx.nEntry = size
	tx.log = pmake([]entry, size)
	runtime.PersistRange(unsafe.Pointer(&tx.log), unsafe.Sizeof(tx.log))
	tx.wlocks = make([]*sync.RWMutex, 0, 0)
	tx.rlocks = make([]*sync.RWMutex, 0, 0)
	return tx
}

func NewUndoTx() TX {
	if undoPool == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-undoPool
	return t
}

func releaseUndoTx(t *undoTx) {
	t.abort()
	undoPool <- t
}

// also takes care of increasing/decreasing the number of entries log can hold
func (t *undoTx) updateLogTail(tail int) {
	if tail >= t.nEntry {
		newE := 2 * t.nEntry // Double number of entries which can be stored
		newLog := pmake([]entry, newE)
		copy(newLog, t.log)
		runtime.PersistRange(unsafe.Pointer(&newLog[0]),
			uintptr(newE)*unsafe.Sizeof(newLog[0])) // Persist new log
		oldLogSliceHdr := (*sliceHeader)(unsafe.Pointer(&t.log))
		newLogSliceHdr := (*sliceHeader)(unsafe.Pointer(&newLog))

		// Essentially doing t.log = newLog, but this needs to be atomic
		// Each of the below three updates are atomic
		// A crash in the middle should be fine
		oldLogSliceHdr.data = newLogSliceHdr.data
		oldLogSliceHdr.cap = newLogSliceHdr.cap
		oldLogSliceHdr.len = newLogSliceHdr.len

		t.nEntry = newE
		t.tail = tail
		runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
	} else if tail == 0 { // reset to original size
		t.log = t.log[:NUMENTRIES]
		t.nEntry = NUMENTRIES
		t.tail = tail
		runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
	} else { // common case
		t.tail = tail
		runtime.PersistRange(unsafe.Pointer(&t.tail), unsafe.Sizeof(t.tail))
	}
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

func (t *undoTx) ReadLog(interface{}) (retVal interface{}) {
	// undoTx doesn't support this. TODO: Should we panic here?
	return retVal
}

func (t *undoTx) Log(data ...interface{}) error {
	if len(data) != 1 {
		return errors.New("[undoTx] Log: Incorrectly used. Correct usage: " +
			"Log(ptr) OR Log(slice)")
	}
	v1 := reflect.ValueOf(data[0])
	if !runtime.InPmem(v1.Pointer()) {
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
		t.updateLogTail(tail + 1)
		if oldv.Kind() == reflect.Slice {
			// Pointer to slice was passed to Log(). So, log slice elements too
			t.logSlice(oldv)
		}
	default:
		debug.PrintStack()
		return errors.New("[undoTx] Log: data must be pointer/slice")
	}
	return nil
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
	sourcePtr := (*[LBUFFERSIZE]byte)(sshdr.data)[:size:size]
	destPtr := (*[LBUFFERSIZE]byte)(vshdr.data)[:size:size]
	copy(destPtr, sourcePtr)
	tail := t.tail
	t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	t.log[tail].size = size                         // size of data

	// Flush logged data copy and entry.
	runtime.FlushRange(t.log[tail].data, uintptr(size))
	runtime.FlushRange(unsafe.Pointer(&t.log[tail]),
		unsafe.Sizeof(t.log[tail]))

	// Update log offset in header.
	t.updateLogTail(tail + 1)
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
		defer t.unLock()

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
		t.updateLogTail(0) // discard all logs.
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

func (t *undoTx) unLock() {
	for _, m := range t.wlocks {
		m.Unlock()
	}
	t.wlocks = make([]*sync.RWMutex, 0, 0)
	for _, m := range t.rlocks {
		m.RUnlock()
	}
	t.rlocks = make([]*sync.RWMutex, 0, 0)
}

func (t *undoTx) abort() error {
	defer t.unLock()
	if t.tail == 0 {
		// Nothing stored in this log
		return nil
	}

	// Has uncommitted log. Replay undo logs
	// Order last updates first, during abort
	t.level = 0
	for i := t.tail - 1; i >= 0; i-- {
		origDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].ptr)
		logDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].data)
		original := origDataPtr[:t.log[i].size:t.log[i].size]
		logdata := logDataPtr[:t.log[i].size:t.log[i].size]
		copy(original, logdata)
		runtime.FlushRange(t.log[i].ptr, uintptr(t.log[i].size))
	}
	t.updateLogTail(0)
	return nil
}
