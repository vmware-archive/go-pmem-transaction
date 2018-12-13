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
 *     tx.End() // S.P = 0 after this
 *     transaction.Release(tx)
 *
 *
 *        | TxHeader |                // Pointer to header passed & stored as
 *    --------- | ------ -----        // part of application pmem root
 *   |          |       |
 *   V          V       V
 *  ---------------------------
 * | logPtr | logPtr |  ...    |      // Stored in pmem. Pointers to small &
 *  ---------------------------       // large logs
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
		level     int
		large     bool
		committed bool
		m         map[unsafe.Pointer]int
		rlocks    []*sync.RWMutex
		wlocks    []*sync.RWMutex
	}

	redoTxHeader struct {
		magic   int
		sLogPtr [SLOGNUM]*redoTx // small txs
		lLogPtr [LLOGNUM]*redoTx // large txs
	}
)

var (
	headerPtr *redoTxHeader
	redoPool  [2]chan *redoTx // volatile structure for pointing to logs.
	// pool[0] for small txs, pool[1] for large txs
)

/* Does the first time initialization, else restores log structure and
 * flushed committed logs. Returns the pointer to redoTX internal structure,
 * so the application can store this in its pmem appRoot.
 */
func initRedoTx(logHeadPtr unsafe.Pointer) unsafe.Pointer {
	if logHeadPtr == nil {
		// First time initialization
		headerPtr = pnew(redoTxHeader)
		headerPtr.magic = MAGIC
		for i := 0; i < SLOGNUM; i++ {
			headerPtr.sLogPtr[i] = _initRedoTx(SENTRYSIZE)
		}
		for i := 0; i < LLOGNUM; i++ {
			headerPtr.lLogPtr[i] = _initRedoTx(LENTRYSIZE)
		}
		runtime.PersistRange(unsafe.Pointer(headerPtr),
			unsafe.Sizeof(*headerPtr))
		logHeadPtr = unsafe.Pointer(headerPtr)
	} else {
		headerPtr = (*redoTxHeader)(logHeadPtr)
		if headerPtr.magic != MAGIC {
			log.Fatal("redoTxHeader magic does not match!")
		}

		// Depending on committed status of transactions, flush changes to
		// data structures or delete all log entries.
		var tx *redoTx
		for i := 0; i < SLOGNUM+LLOGNUM; i++ {
			if i < SLOGNUM {
				tx = headerPtr.sLogPtr[i]
			} else {
				tx = headerPtr.lLogPtr[i-SLOGNUM]
			}
			tx.wlocks = make([]*sync.RWMutex, 0, 0) // Resetting volatile locks
			tx.rlocks = make([]*sync.RWMutex, 0, 0) // before checking for data
			if tx.committed {
				tx.commit()
			} else {
				tx.abort()
			}
		}
	}
	redoPool[0] = make(chan *redoTx, SLOGNUM)
	redoPool[1] = make(chan *redoTx, LLOGNUM)
	for i := 0; i < SLOGNUM; i++ {
		redoPool[0] <- headerPtr.sLogPtr[i]
	}
	for i := 0; i < LLOGNUM; i++ {
		redoPool[1] <- headerPtr.lLogPtr[i]
	}
	return logHeadPtr
}

func _initRedoTx(size int) *redoTx {
	tx := pnew(redoTx)
	if size == LENTRYSIZE {
		tx.large = true
	}
	tx.log = pmake([]entry, size)
	runtime.PersistRange(unsafe.Pointer(tx), unsafe.Sizeof(*tx))
	tx.m = make(map[unsafe.Pointer]int) // On abort m isn't used, so not in pmem
	tx.wlocks = make([]*sync.RWMutex, 0, 0)
	tx.rlocks = make([]*sync.RWMutex, 0, 0)
	return tx
}

func NewRedoTx() TX {
	if redoPool[0] == nil {
		log.Fatal("redo log not correctly initialized!")
	}
	t := <-redoPool[0]
	return t
}

func NewLargeRedoTx() TX {
	if redoPool[1] == nil {
		log.Fatal("redo log not correctly initialized!")
	}
	t := <-redoPool[1]
	return t
}

func releaseRedoTx(t *redoTx) {
	t.abort()
	if t.large {
		redoPool[1] <- t
	} else {
		redoPool[0] <- t
	}
}

func (t *redoTx) ReadLog(ptr interface{}) (retVal interface{}) {
	ptrV := reflect.ValueOf(ptr)
	switch kind := ptrV.Kind(); kind {
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
	default: // TODO: Check if need to read & return slice
		log.Fatal("[redoTx] ReadLog: Arg must be pointer/slice")
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
		if t.large && t.tail >= LENTRYSIZE {
			log.Fatal("[redoTx] Log: Too large transaction. Already logged ",
				LENTRYSIZE, " entries")
		} else if !t.large && t.tail >= SENTRYSIZE {
			log.Fatal("[redoTx] Log: Too large transaction. Already logged ",
				SENTRYSIZE, " entries")
		}
	}

	// Update log to have addr of original data, addr of new copy & size of data
	t.log[tail].ptr = unsafe.Pointer(ptr)
	t.log[tail].data = unsafe.Pointer(logDataPtr.Pointer())
	t.log[tail].size = size
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
		t.committed = true
		runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
		t.commit()
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

func (t *redoTx) unLock() {
	for _, m := range t.wlocks {
		m.Unlock()
	}
	t.wlocks = make([]*sync.RWMutex, 0, 0)
	for _, m := range t.rlocks {
		m.RUnlock()
	}
	t.rlocks = make([]*sync.RWMutex, 0, 0)
}

// Performs in-place updates of app data structures. Started again, if crashed
// in between
func (t *redoTx) commit() error {
	for i := t.tail - 1; i >= 0; i-- {
		oldDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].ptr)
		logDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].data)
		oldData := oldDataPtr[:t.log[i].size:t.log[i].size]
		logData := logDataPtr[:t.log[i].size:t.log[i].size]
		copy(oldData, logData)
		runtime.PersistRange(t.log[i].ptr, uintptr(t.log[i].size))
	}
	t.committed = false
	runtime.PersistRange(unsafe.Pointer(&t.committed),
		unsafe.Sizeof(t.committed))
	t.reset(t.tail)
	return nil
}

// Resets every entry in the log
func (t *redoTx) abort() error {
	if t.large {
		t.reset(LENTRYSIZE)
	} else {
		t.reset(SENTRYSIZE)
	}
	return nil
}

// Resets the entries from sz-1 to 0 in the log
func (t *redoTx) reset(sz int) {
	defer t.unLock()
	t.level = 0
	t.m = make(map[unsafe.Pointer]int)
	for i := sz - 1; i >= 0; i-- {
		t.log[i].ptr = nil
		t.log[i].data = nil
		t.log[i].size = 0
	}
	t.tail = 0
}
