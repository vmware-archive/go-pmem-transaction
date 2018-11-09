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
	"unsafe"
)

type (
	redoTx struct {
		log []entry

		// stores the tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level    int
		large    bool
		commited bool
		m        map[unsafe.Pointer]int
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

		// Depending on commited status of transactions, flush changes to
		// data structures or delete all log entries.
		var tx *redoTx
		for i := 0; i < SLOGNUM+LLOGNUM; i++ {
			if i < SLOGNUM {
				tx = headerPtr.sLogPtr[i]
			} else {
				tx = headerPtr.lLogPtr[i-SLOGNUM]
			}
			if tx.commited {
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
	runtime.PersistRange(unsafe.Pointer(&tx.log),
		uintptr(len(tx.log)*(int)(unsafe.Sizeof(tx.log[0]))))
	runtime.PersistRange(unsafe.Pointer(tx), unsafe.Sizeof(*tx))
	tx.m = make(map[unsafe.Pointer]int) // On abort m isn't used, so not in pmem
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

func (t *redoTx) FakeLog(interface{}) {
	// No logging
}

func (t *redoTx) ReadLog(ptr interface{}) (retVal interface{}, err error) {
	ptrV := reflect.ValueOf(ptr)
	oldVal := reflect.Indirect(ptrV)
	typ := oldVal.Type()
	var logData reflect.Value
	switch kind := ptrV.Kind(); kind {
	case reflect.Ptr:
		if oldVal.Kind() == reflect.Struct {
			// construct struct by reading each field from log
			retStructPtr := reflect.New(typ)
			retStruct := retStructPtr.Elem()
			for i := 0; i < oldVal.NumField(); i++ {
				structFieldPtr := oldVal.Field(i).Addr()
				structFieldTyp := typ.Field(i).Type
				if structFieldTyp.Kind() == reflect.Struct {
					// Handle nested struct
					v1, err1 := t.ReadLog(structFieldPtr.Interface())
					if err1 != nil {
						return retVal, err1
					}
					logData = reflect.ValueOf(v1)
				} else {
					logData, err = t.readLogEntry(structFieldPtr.Pointer(),
						structFieldTyp)
					if err != nil {
						return retVal, err
					}
				}
				retStruct.Field(i).Set(logData) // populate struct field
			}
			retVal = retStruct.Interface()
		} else {
			logData, err = t.readLogEntry(ptrV.Pointer(), typ)
			if err != nil {
				return retVal, err
			}
			retVal = logData.Interface()
		}
	default: // TODO: Check if need to read & return slice
		return retVal, errors.New("[redoTx] ReadLog: Arg must be pointer/slice")
	}
	return retVal, err
}

func (t *redoTx) readLogEntry(ptr uintptr, typ reflect.Type) (v reflect.Value,
	err error) {
	tail, ok := t.m[unsafe.Pointer(ptr)]
	if !ok {
		return v, errors.New("[redoTx] ReadLog: data not logged before")
	}
	logDataPtr := reflect.NewAt(typ, t.log[tail].data)
	v = reflect.Indirect(logDataPtr)
	return v, nil
}

// Caveat: With the current implementation, Redo Log doesn't support logging
// structs with unexported variables. Individual fields of struct can be logged.
// This can be solved with some extra overhead. Left as future work for now.
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
		for i := 0; i < v1.Len(); i++ {
			elemNewVal := v2.Index(i)
			elemPtr := v1.Index(i).Addr()
			t.writeLogEntry(elemPtr.Pointer(), elemNewVal)
		}
	case reflect.Ptr:
		oldV := reflect.Indirect(v1) // the underlying data of v1
		if v2.Type() != oldV.Type() {
			return errors.New("Log Error. Data passed to Log() is not of " +
				"the same type as underlying data of ptr")
		}
		if v2.Kind() == reflect.Struct {
			// Each struct field is stored separately in log
			for i := 0; i < v2.NumField(); i++ {
				newVal := v2.Field(i)
				oldVal := oldV.Field(i)
				ptrOrig := oldVal.Addr()
				if newVal.Kind() == reflect.Struct {
					err = t.Log(ptrOrig.Interface(), newVal.Interface())
				} else if newVal.Kind() == reflect.Slice {
					err = t.Log(oldVal.Interface(), newVal.Interface())
				} else {
					err = t.writeLogEntry(ptrOrig.Pointer(), newVal)
				}
				if err != nil {
					return err
				}
			}
		} else {
			err = t.writeLogEntry(v1.Pointer(), v2)
		}
	default:
		debug.PrintStack()
		return errors.New("[redoTx] Log: data must be pointer/slice")
	}
	return err
}

func (t *redoTx) writeLogEntry(ptr uintptr, data reflect.Value) error {
	if data.CanInterface() == false {
		return errors.New("[redoTx] Log: can't log unexported variables")
	}
	typ := data.Type()
	size := int(typ.Size())
	logDataPtr := reflect.PNew(typ)
	reflect.Indirect(logDataPtr).Set(data)

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
func (t *redoTx) Exec(intf ...interface{}) (err error, retVal []reflect.Value) {
	if len(intf) < 1 {
		return errors.New("[redoTx] Exec: Must have atleast one argument"),
			retVal
	}
	fnPosInInterfaceArgs := 0
	fn := reflect.ValueOf(intf[fnPosInInterfaceArgs]) // The function to call
	if fn.Kind() != reflect.Func {
		return errors.New("[redoTx] Exec: 1st argument must be a function"),
			retVal
	}
	fnType := fn.Type()
	fnName := runtime.FuncForPC(fn.Pointer()).Name()
	// Populate the arguments of the function correctly
	argv := make([]reflect.Value, fnType.NumIn())
	if len(argv) != len(intf) {
		return errors.New("[redoTx] Exec: Incorrect no. of args to function " +
			fnName), retVal
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
				return errors.New("[redoTx] Exec: Incorrect type of args to " +
					"function " + fnName), retVal
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
		return errors.New("[redoTx] Exec: Unbalanced Begin() & End() calls " +
			"inside function " + fnName), retVal
	}
	return err, retVal
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
		// Flush changes in log. Mark tx as commited. Call commit()
		// to transfer changes to app data structures
		for i := t.tail - 1; i >= 0; i-- {
			runtime.PersistRange(t.log[i].data, uintptr(t.log[i].size))
		}
		runtime.PersistRange(unsafe.Pointer(&t.log[0]),
			uintptr(t.tail*(int)(unsafe.Sizeof(t.log[0]))))
		t.commited = true
		runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
		t.commit()
	}
	return nil
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
	t.commited = false
	runtime.PersistRange(unsafe.Pointer(&t.commited), unsafe.Sizeof(t.commited))
	t.abort()

	// TODO: Check if this is okay to remove
	// runtime.PersistRange(unsafe.Pointer(t), unsafe.Sizeof(*t))
	return nil
}

// Clears the entries of the log
func (t *redoTx) abort() error {
	if t.tail == 0 {
		// Nothing stored in this log
		return nil
	}
	t.level = 0
	// Replay redo logs
	for i := t.tail - 1; i >= 0; i-- {
		delete(t.m, t.log[i].ptr)
		t.log[i].ptr = nil
		t.log[i].data = nil
		t.log[i].size = 0
	}
	t.tail = 0
	return nil
}
