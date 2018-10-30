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
 * | logPtr | logPtr |  ...    |      // Stored in pmem. Pointers to small &
 *  ---------------------------       // large logs
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
	"errors" // "fmt" // TODO: Remove fmt import. Used for debug prints
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"unsafe"
)

type (
	/* entry for each undo log update, stay in persistent heap with pointer
	to data copy */
	entry struct {
		id   int // TODO: global counter to determine entry order for recovery
		ptr  unsafe.Pointer
		data unsafe.Pointer
		size int
	}

	undoTx struct {
		log []entry

		// stores the tail position of the log where new data would be stored
		tail int

		// Level of nesting. Needed for nested transactions
		level int
		large bool
	}

	undoTxHeader struct {
		magic   int
		sLogPtr [SLOGNUM]*undoTx // small txs
		lLogPtr [LLOGNUM]*undoTx // large txs
	}
)

const (
	MAGIC      = 131071
	LLOGNUM    = 12
	SLOGNUM    = 500
	LENTRYSIZE = 16 * 1024
	SENTRYSIZE = 128
)

var (
	txHeaderPtr *undoTxHeader
	undoPool    [2]chan *undoTx // volatile structure for pointing to logs.
	// pool[0] for small txs, pool[1] for large txs
)

/* Does the first time initialization, else restores log structure and
 * reverts uncommitted logs. Does not do any pointer swizzle. This will be
 * handled by Go-pmem runtime. Returns the pointer to undoTX internal structure,
 * so the application can store it in its pmem appRoot.
 */
func initUndoTx(logHeadPtr unsafe.Pointer) unsafe.Pointer {
	if logHeadPtr == nil {

		// First time initialization
		// fmt.Println("[undoTx] init: first time initialization")
		txHeaderPtr = pnew(undoTxHeader)
		txHeaderPtr.magic = MAGIC
		for i := 0; i < SLOGNUM; i++ {
			txHeaderPtr.sLogPtr[i] = _initUndoTx(SENTRYSIZE)
		}
		for i := 0; i < LLOGNUM; i++ {
			txHeaderPtr.lLogPtr[i] = _initUndoTx(LENTRYSIZE)
		}
		runtime.PersistRange(unsafe.Pointer(txHeaderPtr),
			unsafe.Sizeof(*txHeaderPtr))
		logHeadPtr = unsafe.Pointer(txHeaderPtr)
	} else {

		// fmt.Println("[undoTx] init: Dont initialize. Have prev run's header")
		txHeaderPtr = (*undoTxHeader)(logHeadPtr)
		if txHeaderPtr.magic != MAGIC {
			log.Fatal("undoTxHeader magic does not match!")
		}

		// Recover data from previous pending transactions, if any
		for i := 0; i < SLOGNUM; i++ {
			tx := txHeaderPtr.sLogPtr[i]
			tx.abort()
		}
		for i := 0; i < LLOGNUM; i++ {
			tx := txHeaderPtr.lLogPtr[i]
			tx.abort()
		}
	}

	undoPool[0] = make(chan *undoTx, SLOGNUM)
	undoPool[1] = make(chan *undoTx, LLOGNUM)
	for i := 0; i < SLOGNUM; i++ {
		undoPool[0] <- txHeaderPtr.sLogPtr[i]
	}
	for i := 0; i < LLOGNUM; i++ {
		undoPool[1] <- txHeaderPtr.lLogPtr[i]
	}
	return logHeadPtr
}

func _initUndoTx(size int) *undoTx {
	tx := pnew(undoTx)
	if size == LENTRYSIZE {
		tx.large = true
	}
	tx.log = pmake([]entry, size)
	runtime.PersistRange(unsafe.Pointer(&tx.log),
		uintptr(len(tx.log)*(int)(unsafe.Sizeof(tx.log[0]))))
	runtime.PersistRange(unsafe.Pointer(tx), unsafe.Sizeof(*tx))
	return tx
}

func NewUndoTx() TX {
	if undoPool[0] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-undoPool[0]
	return t
}

func NewLargeUndoTx() TX {
	if undoPool[1] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-undoPool[1]
	return t
}

func releaseUndoTx(t *undoTx) {
	t.abort()
	if t.large {
		undoPool[1] <- t
	} else {
		undoPool[0] <- t
	}
}

func (t *undoTx) updateLogTail(tail int) {
	// atomic update
	runtime.Fence()
	if t.large && tail >= LENTRYSIZE {
		log.Fatal("Too large transaction. Already logged ",
			LENTRYSIZE, " entries")
	} else if !t.large && tail >= SENTRYSIZE {
		log.Fatal("Too large transaction. Already logged ",
			SENTRYSIZE, " entries")
	}
	t.tail = tail
	runtime.FlushRange(unsafe.Pointer(&t.tail), unsafe.Sizeof(t.tail))
	runtime.Fence()
}

type Value struct {
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

func (t *undoTx) FakeLog(interface{}) {
	// No logging
}

func (t *undoTx) Log(data interface{}) error {
	// Check data type, allocate and assign copy of data.
	var (
		v1   reflect.Value = reflect.ValueOf(data)
		v2   reflect.Value
		typ  reflect.Type
		size int
	)
	switch kind := v1.Kind(); kind {
	case reflect.Slice:
		typ = v1.Type()
		v1len := v1.Len()
		size = v1len * int(typ.Elem().Size())
		v2 = reflect.PMakeSlice(typ, v1len, v1len)
		vptr := (*Value)(unsafe.Pointer(&v2))
		vshdr := (*sliceHeader)(vptr.ptr)
		sourceVal := (*Value)(unsafe.Pointer(&v1))
		sshdr := (*sliceHeader)(sourceVal.ptr)
		sourcePtr := (*[LBUFFERSIZE]byte)(sshdr.data)[:size:size]
		destPtr := (*[LBUFFERSIZE]byte)(vshdr.data)[:size:size]
		copy(destPtr, sourcePtr)
	case reflect.Ptr:
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		typ = oldv.Type()
		size = int(typ.Size())
		v2 = reflect.PNew(oldv.Type())
		reflect.Indirect(v2).Set(oldv) // copy old data
	default:
		debug.PrintStack()
		return errors.New("[undoTx] Log: data must be pointer/slice!")
	}

	// Append data to log entry.
	tail := t.tail
	t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	t.log[tail].size = size                         // size of data

	// fmt.Println("[undoTx] Log: tail =", tail, "ptr =", t.log[tail].ptr)
	// fmt.Println("data =", reflect.Indirect(v2), "size =", t.log[tail].size)

	// Flush logged data copy and entry.
	runtime.PersistRange(t.log[tail].data, uintptr(size))
	runtime.PersistRange(unsafe.Pointer(&t.log[tail]),
		unsafe.Sizeof(t.log[tail]))

	// Update log offset in header.
	t.updateLogTail(tail + 1)
	return nil
}

/* Exec function receives a variable number of interfaces as its arguments.
 * Usage: Exec(fn_name, fn_arg1, fn_arg2, ...)
 * Function fn_name() should not return anything.
 * No need to Begin() & End() transaction separately if Exec() is used.
 * Caveat: All locks within function fn_name(fn_arg1, fn_arg2, ...) should be
 * taken before making Exec() call. Locks should be released after Exec() call.
 */
func (t *undoTx) Exec(intf ...interface{}) (err error, retVal []reflect.Value) {
	if len(intf) < 1 {
		return errors.New("[undoTx] Exec: Must have atleast one argument"),
			retVal
	}
	fnPosInInterfaceArgs := 0
	fn := reflect.ValueOf(intf[fnPosInInterfaceArgs]) // The function to call
	if fn.Kind() != reflect.Func {
		return errors.New("[undoTx] Exec: 1st argument must be a function"),
			retVal
	}
	fnType := fn.Type()
	fnName := runtime.FuncForPC(fn.Pointer()).Name()
	// Populate the arguments of the function correctly
	argv := make([]reflect.Value, fnType.NumIn())
	if len(argv) != len(intf) {
		return errors.New("[undoTx] Exec: Incorrect no. of args to function " +
			fnName), retVal
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
				return errors.New("[undoTx] Exec: Incorrect type of args to " +
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
		return errors.New("[undoTx] Exec: Unbalanced Begin() & End() calls " +
			"inside function " + fnName), retVal
	}
	return err, retVal
}

func (t *undoTx) Begin() error {

	// fmt.Println("[undoTx] Begin")
	t.level += 1
	return nil
}

/* Also persists the new data written by application, so application
 * doesn't need to do it separately. For nested transactions, End() call to
 * inner transaction does nothing. Only when the outermost transaction ends,
 * all application data is flushed to pmem. TODO: See if this behavior is okay.
 */
func (t *undoTx) End() error {
	if t.level == 0 {
		return errors.New("[undoTx] End: no transaction to commit!")
	}
	t.level--
	if t.level == 0 {

		// Need to flush current value of logged areas
		// fmt.Println("[undoTx] End: Persist updates made to app structures")
		for i := t.tail - 1; i >= 0; i-- {
			runtime.PersistRange(t.log[i].ptr, uintptr(t.log[i].size))
		}
		t.updateLogTail(0) // discard all logs.
	} else {

		// fmt.Println("[undoTx] End: Nested transaction. Not doing anything")
	}
	return nil
}

func (t *undoTx) abort() error {
	if t.tail == 0 {

		// Nothing stored in this log
		return nil
	}

	// fmt.Println("[undoTx] abort: uncommitted transaction")
	// Has uncommitted log
	// TODO: order abort sequence according to some global counter
	t.level = 0

	// Replay undo logs
	for i := t.tail - 1; i >= 0; i-- {
		origDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].ptr)
		logDataPtr := (*[LBUFFERSIZE]byte)(t.log[i].data)
		original := origDataPtr[:t.log[i].size:t.log[i].size]
		logdata := logDataPtr[:t.log[i].size:t.log[i].size]

		// TODO: Remove this. Fail here to test nested crashing
		// time.Sleep(2 * time.Second)
		copy(original, logdata)
		runtime.PersistRange(t.log[i].ptr, uintptr(t.log[i].size))
	}
	t.updateLogTail(0)
	return nil
}
