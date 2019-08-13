///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

/* Put address of the data to be updated and the new data entry in persistent
 * heap (with type info). All changes are copied to in-place data structures
 * at end of transaction. Value-based logging is done, so in-place updates
 * are idempotent & can be reperformed from the start in case of nested crashes.
 * E.g.:
 *     type S struct {
 *         P *int
 *     }
 *     tx := NewredoTx()
 *     tx.Begin()
 *     tx.Log(S.P, 100)
 *     a := tx.ReadLog(S.P) // At this point, a = 100, but S.P = 0
 *     tx.End() // S.P = 100 after this
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
 *      ---> | entry | entry | ... |  // Has address of updates & new data copy
 *            ---------------------
 *                  |
 *                  |       -----------
 *                   ----> | data copy |   // Single copy for each address
 *                          -----------    // In case of multiple updates to one
 *                                         // address, this copy is updated
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
	redoTx struct {
		log []entry

		// stores the tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int

		// Index of the log handle within redoArray
		index int

		// num of entries which can be stored in log
		nEntry    int
		committed bool
		m         map[unsafe.Pointer]int
		rlocks    []*sync.RWMutex
		wlocks    []*sync.RWMutex
	}

	redoTxHeader struct {
		magic  int
		logPtr [logNum]*redoTx
	}
)

var (
	headerPtr *redoTxHeader
	redoArray *bitmap
)

/* Does the first time initialization, else restores log structure and
 * flushed committed logs. Returns the pointer to redoTX internal structure,
 * so the application can store this in its pmem appRoot.
 */
func initRedoTx(logHeadPtr unsafe.Pointer) unsafe.Pointer {
	redoArray = newBitmap(logNum)
	if logHeadPtr == nil {
		// First time initialization
		headerPtr = pnew(redoTxHeader)
		for i := 0; i < logNum; i++ {
			headerPtr.logPtr[i] = _initRedoTx(NumEntries, i)
		}
		// Write the magic constant after the transaction handles are persisted.
		// NewRedoTx() can then check this constant to ensure all tx handles
		// are properly initialized before releasing any.
		runtime.PersistRange(unsafe.Pointer(&headerPtr.logPtr), logNum*ptrSize)
		headerPtr.magic = magic
		runtime.PersistRange(unsafe.Pointer(&headerPtr.magic), ptrSize)
		logHeadPtr = unsafe.Pointer(headerPtr)
	} else {
		headerPtr = (*redoTxHeader)(logHeadPtr)
		if headerPtr.magic != magic {
			log.Fatal("redoTxHeader magic does not match!")
		}

		// Depending on committed status of transactions, flush changes to
		// data structures or delete all log entries.
		var tx *redoTx
		for i := 0; i < logNum; i++ {
			tx = headerPtr.logPtr[i]
			tx.index = i
			tx.wlocks = make([]*sync.RWMutex, 0, 0) // Resetting volatile locks
			tx.rlocks = make([]*sync.RWMutex, 0, 0) // before checking for data
			if tx.committed {
				tx.commit(true)
			} else {
				tx.abort()
			}
		}
	}

	return logHeadPtr
}

func _initRedoTx(size, index int) *redoTx {
	tx := pnew(redoTx)
	tx.nEntry = size
	tx.log = pmake([]entry, size)
	runtime.PersistRange(unsafe.Pointer(tx), unsafe.Sizeof(*tx))
	tx.index = index
	tx.m = make(map[unsafe.Pointer]int) // On abort m isn't used, so not in pmem
	tx.wlocks = make([]*sync.RWMutex, 0, 0)
	tx.rlocks = make([]*sync.RWMutex, 0, 0)
	return tx
}

func NewRedoTx() TX {
	if headerPtr == nil || headerPtr.magic != magic {
		log.Fatal("redo log not correctly initialized!")
	}
	index := redoArray.nextAvailable()
	return headerPtr.logPtr[index]
}

func releaseRedoTx(t *redoTx) {
	t.abort()
	redoArray.clearBit(t.index)
}

func (t *redoTx) ReadLog(intf ...interface{}) (retVal interface{}) {
	if len(intf) == 2 {
		return t.readSliceElem(intf[0], intf[1].(int))
	} else if len(intf) == 3 {
		return t.readSlice(intf[0], intf[1].(int), intf[2].(int))
	} else if len(intf) != 1 {
		panic("[redoTx] ReadLog: Incorrect number of args passed")
	}
	ptrV := reflect.ValueOf(intf[0])
	switch ptrV.Kind() {
	case reflect.Ptr:
		oldVal := reflect.Indirect(ptrV)
		var logData reflect.Value
		if oldVal.Kind() == reflect.Struct {
			// construct struct by reading each field from log
			typ := oldVal.Type()
			retStructPtr := reflect.New(typ)
			retStruct := retStructPtr.Elem()
			for i := 0; i < oldVal.NumField(); i++ {
				structFieldPtr := oldVal.Field(i).Addr()
				structFieldTyp := typ.Field(i).Type
				if structFieldTyp.Kind() == reflect.Struct {
					// Handle nested struct
					v1 := t.ReadLog(structFieldPtr.Interface())
					logData = reflect.ValueOf(v1)
				} else {
					logData = t.readLogEntry(structFieldPtr.Pointer(),
						structFieldTyp)
				}
				if retStruct.Field(i).CanSet() {
					retStruct.Field(i).Set(logData) // populate struct field
				} else {
					log.Fatal("[redoTx] ReadLog: Cannot read struct with " +
						"unexported field")
				}
			}
			retVal = retStruct.Interface()
		} else if oldVal.Kind() == reflect.Invalid {
			// Do nothing.
		} else {
			typ := oldVal.Type()
			logData = t.readLogEntry(ptrV.Pointer(), typ)
			retVal = logData.Interface()
		}
	default:
		log.Fatal("[redoTx] ReadLog: Arg must be pointer")
	}
	return retVal
}

func (t *redoTx) readLogEntry(ptr uintptr, typ reflect.Type) (v reflect.Value) {
	tail, ok := t.m[unsafe.Pointer(ptr)]
	if !ok {
		dataPtr := reflect.NewAt(typ, unsafe.Pointer(ptr))
		v = reflect.Indirect(dataPtr)
		// TODO: Data was not stored in redo log before. Should we log now?
		return v
	}
	logDataPtr := reflect.NewAt(typ, t.log[tail].data)
	v = reflect.Indirect(logDataPtr)
	return v
}

func (t *redoTx) readSliceElem(slicePtr interface{}, index int) interface{} {
	var retVal reflect.Value
	ptrV := reflect.ValueOf(slicePtr)
	sTyp := ptrV.Type().Elem() // type of slice
	switch ptrV.Kind() {
	case reflect.Ptr:
		logData := t.readLogEntry(ptrV.Pointer(), sTyp) // read sliceheader 1st
		v := (*value)(unsafe.Pointer(&logData))
		newShdr := (*sliceHeader)(v.ptr)
		if index > newShdr.len {
			log.Fatal("[redoTx] readSliceElem: Index out of bounds")
		}
		elemType := sTyp.Elem() // type of elements in slice
		elemPtr := uintptr(newShdr.data) + (uintptr(index) * elemType.Size())
		retVal = t.readLogEntry(elemPtr, elemType)
	default:
		log.Fatal("[redoTx] readSliceElem: Arg must be pointer to slice")
	}
	return retVal.Interface()
}

func (t *redoTx) readSlice(slicePtr interface{}, stIndex int,
	endIndex int) interface{} {
	var retVal reflect.Value
	ptrV := reflect.ValueOf(slicePtr)
	sTyp := ptrV.Type().Elem() // type of slice
	switch ptrV.Kind() {
	case reflect.Ptr:
		logData := t.readLogEntry(ptrV.Pointer(), sTyp) // read sliceheader 1st
		v := (*value)(unsafe.Pointer(&logData))
		newShdr := (*sliceHeader)(v.ptr)
		origL := newShdr.len
		if stIndex < 0 || endIndex > origL {
			log.Fatal("[redoTx] readSlice: Index out of bounds")
		} else if stIndex > endIndex {
			log.Fatal("[redoTx] readSlice: 1st index should be <= 2nd index")
		}
		elemType := sTyp.Elem() // type of elements in slice
		sLen := endIndex - stIndex
		s := reflect.PMakeSlice(sTyp, sLen, sLen)
		for i := 0; i < sLen; i++ {
			sElem := s.Index(i)
			elemPtr := uintptr(newShdr.data) + (uintptr(stIndex+i) *
				elemType.Size())
			sElem.Set(t.readLogEntry(elemPtr, elemType))
		}
		retVal = s
	default:
		log.Fatal("[redoTx] readSlice: Arg must be pointer to slice")
	}
	return retVal.Interface()
}

func checkDataTypes(newV reflect.Value, v1 reflect.Value) (err error) {
	if newV.Kind() != reflect.Invalid {
		// Invalid => Logging <nil> value. No type check
		oldV := reflect.Indirect(v1)
		if oldV.Kind() != reflect.Interface {
			// Can't do type comparison if logging interface
			oldType := v1.Type().Elem()
			if newV.Type() != oldType {
				err = errors.New("Log Error. Data passed to Log() is not of " +
					"the same type as underlying data of ptr")
			}
		}
	}
	return err
}

// Caveat: With the current implementation, Redo Log doesn't support logging
// structs with unexported slice, struct, interface members. Individual fields
// of struct can be logged.
func (t *redoTx) Log(intf ...interface{}) (err error) {
	if len(intf) != 2 {
		return errors.New("[redoTx] Log: Incorrectly called. Correct usage: " +
			"Log(ptr, data)")
	}
	v1 := reflect.ValueOf(intf[0])
	v2 := reflect.ValueOf(intf[1])
	switch kind := v1.Kind(); kind {
	case reflect.Slice:
		if v2.Type() != v1.Type() {
			return errors.New("Log Error. Slice values passed to Log() are " +
				"not of the same type")
		}

		// Each slice element is stored separately in log
		var vLen int
		if v1.Len() < v2.Len() {
			vLen = v1.Len()
		} else {
			vLen = v2.Len()
		}
		for i := 0; i < vLen; i++ {
			elemNewVal := v2.Index(i)
			elemPtr := v1.Index(i).Addr()
			t.writeLogEntry(elemPtr.Pointer(), elemNewVal, elemNewVal.Type())
		}
	case reflect.Ptr:
		oldType := v1.Type().Elem() // type of data, v1 is pointing to
		err = checkDataTypes(v2, v1)
		if err != nil {
			return err
		}
		if v2.Kind() == reflect.Struct {
			// Each struct field is stored separately in log
			oldV := reflect.Indirect(v1)
			for i := 0; i < v2.NumField(); i++ {
				newVal := v2.Field(i)
				oldVal := oldV.Field(i)
				oldType = oldVal.Type()
				ptrOrig := oldVal.Addr()
				if newVal.Kind() == reflect.Struct {
					if !newVal.CanInterface() {
						log.Fatal("[redoTx] Log: Cannot log unexported struct" +
							"member variable")
					}
					err = t.Log(ptrOrig.Interface(), newVal.Interface())
				} else if newVal.Kind() == reflect.Slice {
					if !newVal.CanInterface() {
						log.Fatal("[redoTx] Log: Cannot log unexported slice" +
							"member variable")
					}
					err = t.Log(oldVal.Interface(), newVal.Interface())
				} else {
					err = t.writeLogEntry(ptrOrig.Pointer(), newVal, oldType)
				}
				if err != nil {
					return err
				}
			}
		} else {
			err = t.writeLogEntry(v1.Pointer(), v2, oldType)
		}
	default:
		debug.PrintStack()
		return errors.New("[redoTx] Log: first arg must be pointer/slice")
	}
	if !runtime.InPmem(v1.Pointer()) {
		err = errors.New("[redoTx] Log: Updates to data in volatile memory" +
			" can be lost")
	}
	return err
}

func (t *redoTx) writeLogEntry(ptr uintptr, data reflect.Value,
	typ reflect.Type) error {
	size := int(typ.Size())
	var logDataPtr reflect.Value
	logDataPtr = reflect.PNew(typ)
	if data.Kind() == reflect.Invalid {
		// Do nothing, data has <nil> value
	} else if data.CanInterface() {
		reflect.Indirect(logDataPtr).Set(data)
	} else { // To log unexported fields of struct
		switch data.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Int64:
			reflect.Indirect(logDataPtr).SetInt(data.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
			reflect.Uint64, reflect.Uintptr:
			reflect.Indirect(logDataPtr).SetUint(data.Uint())
		case reflect.Float32, reflect.Float64:
			reflect.Indirect(logDataPtr).SetFloat(data.Float())
		case reflect.Bool:
			reflect.Indirect(logDataPtr).SetBool(data.Bool())
		case reflect.Complex64, reflect.Complex128:
			reflect.Indirect(logDataPtr).SetComplex(data.Complex())
		case reflect.String:
			reflect.Indirect(logDataPtr).SetString(data.String())
		case reflect.Ptr, reflect.UnsafePointer:
			// Go only allows setting a pointer as unsafe.Pointer, so logDataPtr
			// has to be reallocated with new type as unsafe.Pointer
			ptrData := unsafe.Pointer(data.Pointer())
			logDataPtr = reflect.PNew(reflect.TypeOf(ptrData))
			reflect.Indirect(logDataPtr).SetPointer(ptrData)
		default:
			log.Fatalf("[redoTx] Log: Cannot log unexported data of kind: ",
				data.Kind(), "type: ", typ)
		}
	}

	// Check if write to this addr already stored in log by checking in map.
	// If yes, update value in-place in log. Else add new entry to log.
	tail, ok := t.m[unsafe.Pointer(ptr)]
	if !ok {
		tail = t.tail
		t.m[unsafe.Pointer(ptr)] = t.tail

		// Update log offset in header.
		t.tail++
		if t.tail >= t.nEntry { // Expand log if necessary
			newE := 2 * t.nEntry
			newLog := pmake([]entry, newE)
			copy(newLog, t.log)
			t.log = newLog
			t.nEntry = newE
		}
	}

	// Update log to have addr of original data, addr of new copy & size of data
	t.log[tail].ptr = unsafe.Pointer(ptr)
	t.log[tail].data = unsafe.Pointer(logDataPtr.Pointer())
	t.log[tail].size = size
	if data.Kind() == reflect.Slice {
		// ptr to sliceHeader was passed for logging, so need to persist
		// new slice too on tx complete. This will be used in t.commit()
		t.log[tail].sliceElemSize = int(data.Type().Elem().Size())
	}
	return nil
}

/* Exec function receives a variable number of interfaces as its arguments.
 * Usage: Exec(fn_name, fn_arg1, fn_arg2, ...)
 * Function fn_name() should not return anything.
 * No need to Begin() & End() transaction separately if Exec() is used.
 * Caveat: All locks within function fn_name(fn_arg1, fn_arg2, ...) should be
 * taken before making Exec() call. Locks should be released after Exec() call.
 */
func (t *redoTx) Exec(intf ...interface{}) (retVal []reflect.Value, err error) {
	if len(intf) < 1 {
		return retVal,
			errors.New("[redoTx] Exec: Must have atleast one argument")
	}
	fnPosInInterfaceArgs := 0
	fn := reflect.ValueOf(intf[fnPosInInterfaceArgs]) // The function to call
	if fn.Kind() != reflect.Func {
		return retVal,
			errors.New("[redoTx] Exec: 1st argument must be a function")
	}
	fnType := fn.Type()
	fnName := runtime.FuncForPC(fn.Pointer()).Name()
	// Populate the arguments of the function correctly
	argv := make([]reflect.Value, fnType.NumIn())
	if len(argv) != len(intf) {
		return retVal, errors.New("[redoTx] Exec: Incorrect no. of args to " +
			"function " + fnName)
	}
	for i := range argv {
		if i == fnPosInInterfaceArgs {
			// Add t *redoTx as the 1st argument to be passed to the function
			// fn. This is not passed by the application when it calls Exec().
			argv[i] = reflect.ValueOf(t)
		} else {
			// get the arguments to the function call from the call to Exec()
			// and populate in argv
			if reflect.TypeOf(intf[i]) != fnType.In(i) {
				return retVal, errors.New("[redoTx] Exec: Incorrect type of " +
					"args to function " + fnName)
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
		return retVal, errors.New("[redoTx] Exec: Unbalanced Begin() & End() " +
			"calls inside function " + fnName)
	}
	return retVal, err
}

func (t *redoTx) Begin() error {
	t.level++
	return nil
}

/* Persists the update written to redoLog during the transaction lifetime. For
 * nested transactions, End() call to inner transaction does nothing.
 */
func (t *redoTx) End() error {
	if t.level == 0 {
		return errors.New("[redoTx] End: no transaction to commit")
	}
	t.level--
	if t.level == 0 {
		// Flush changes in log. Mark tx as committed. Call commit()
		// to transfer changes to app data structures
		for i := t.tail - 1; i >= 0; i-- {
			runtime.PersistRange(t.log[i].data, uintptr(t.log[i].size))
		}
		runtime.PersistRange(unsafe.Pointer(&t.log[0]),
			uintptr(t.tail*(int)(unsafe.Sizeof(t.log[0]))))
		runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
		t.committed = true
		runtime.PersistRange(unsafe.Pointer(&t.committed),
			unsafe.Sizeof(t.committed))
		t.commit(false)
	}
	return nil
}

func (t *redoTx) RLock(m *sync.RWMutex) {
	m.RLock()
	t.rlocks = append(t.rlocks, m)
}

func (t *redoTx) WLock(m *sync.RWMutex) {
	m.Lock()
	t.wlocks = append(t.wlocks, m)
}

func (t *redoTx) Lock(m *sync.RWMutex) {
	t.WLock(m)
}

// Unlock API unlock all the read and write locks taken so far
func (t *redoTx) Unlock() {
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

// Performs in-place updates of app data structures. Started again, if crashed
// in between
func (t *redoTx) commit(skipVolData bool) error {
	for i := t.tail - 1; i >= 0; i-- {
		oldDataPtr := (*[maxint]byte)(t.log[i].ptr)
		if skipVolData && !runtime.InPmem(uintptr(unsafe.Pointer(oldDataPtr))) {
			// If commit() was called during Init, control reaches here. If so,
			// we drop updates to data in volatile memory
			continue
		}
		logDataPtr := (*[maxint]byte)(t.log[i].data)
		oldData := oldDataPtr[:t.log[i].size:t.log[i].size]
		logData := logDataPtr[:t.log[i].size:t.log[i].size]
		copy(oldData, logData)
		runtime.PersistRange(t.log[i].ptr, uintptr(t.log[i].size))
		if t.log[i].sliceElemSize != 0 {
			// ptr points to sliceHeader. So, need to persist the slice too
			shdr := (*sliceHeader)(t.log[i].ptr)
			runtime.PersistRange(shdr.data, uintptr(shdr.len*
				t.log[i].sliceElemSize))
		}
	}
	t.committed = false
	runtime.PersistRange(unsafe.Pointer(&t.committed),
		unsafe.Sizeof(t.committed))
	t.reset(t.tail)
	return nil
}

// Resets every entry in the log
func (t *redoTx) abort() error {
	t.reset(t.tail)
	return nil
}

// Resets the entries from sz-1 to 0 in the log
func (t *redoTx) reset(sz int) {
	defer t.Unlock()
	t.level = 0
	t.m = make(map[unsafe.Pointer]int)
	t.log = t.log[:NumEntries] // reset to original size
	t.nEntry = NumEntries
	if sz > NumEntries {
		sz = NumEntries
	}
	for i := sz - 1; i >= 0; i-- {
		t.log[i].ptr = nil
		t.log[i].data = nil
		t.log[i].size = 0
		t.log[i].sliceElemSize = 0
	}
	t.tail = 0
}
