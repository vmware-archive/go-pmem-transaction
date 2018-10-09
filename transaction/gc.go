package transaction

/* put log entries and data copies into persistent heap (with type info)
 * to prevent runtime garbage collection to reclaim dangling pointers caused by undo updates.
 * E.g.:
 *     type S struct {
 *         P *int
 *     }
 *     tx.Begin()
 *     tx.Log(&S)
 *     S.P = nil   // <-- we need ot prevent data pointed by p from been garbage collected until tx got committed.
 *     tx.Commit()
 *
 *
 *  ---------------------------
 * | header | header |  ...    |                   # store in metadata area for easy retriving
 *  ---------------------------
 *     |
 *     |      ---------------------
 *      ---> | entry | entry | ... |               # store in persistent heap to track pointers to data copies
 *            ---------------------
 *                  |
 *                  |       -----------
 *                   ----> | data copy |           # store in persistent heap to track pointers in data copies
 *                          -----------            # e.g., S.P in previous example
 */

import (
	"errors"
	"log"
	"reflect"
	"runtime/debug"
	"sync"
	"unsafe"
)

type (
	/* PER THREAD tx, hold pointer to log entry array in persistent heap and record log tail position. */
	undoHeader struct {
		tail int
		log  []entry
	}

	/* entry for each undo log update, stay in peresistent heap with pointer to data copy. */
	entry struct {
		id   int // TODO: global counter to determine entry order for recovery.
		ptr  unsafe.Pointer
		data unsafe.Pointer
		size int
	}

	/* runtime volatile status of tx. */
	undoTx struct {
		header *undoHeader
		log    []entry
		level  int
		rlocks []*sync.RWMutex
		wlocks []*sync.RWMutex
		large  bool
	}
)

const (
	LLOGNUM    = 12
	SLOGNUM    = 500
	LENTRYSIZE = 16 * 1024
	SENTRYSIZE = 128
)

var (
	undoPool [2]chan *undoTx // pool[0] for small txs, pool[1] for large txs
)

// TODO: correctly swizzle and revert uncommitted logs.
func InitUndo(headerArea []byte) {
	headersize := int(unsafe.Sizeof(undoHeader{}))
	if len(headerArea) < (LLOGNUM+SLOGNUM)*headersize {
		log.Fatal("Not enough room for init undo metadata.")
	}

	undoPool[0] = make(chan *undoTx, SLOGNUM)
	undoPool[1] = make(chan *undoTx, LLOGNUM)
	pos := 0
	for i := 0; i < SLOGNUM; i++ {
		initUndo(headerArea[pos:], undoPool[0], SENTRYSIZE)
		pos += headersize
	}
	for i := 0; i < LLOGNUM; i++ {
		initUndo(headerArea[pos:], undoPool[1], LENTRYSIZE)
		pos += headersize
	}
	Persist(unsafe.Pointer(&headerArea[0]), pos)
	sfence()
}

func initUndo(header []byte, txPool chan *undoTx, size int) {
	tx := new(undoTx)
	tx.header = (*undoHeader)(unsafe.Pointer(&header[0]))
	if tx.header.tail > 0 { // has uncommitted log
		// TODO: order abort sequence according to global counter
		tx.log = tx.header.log
		tx.Abort()
	} else {
		tx.header.log = pmake([]entry, size)
		tx.log = tx.header.log
	}
	if size == LENTRYSIZE {
		tx.large = true
	}
	tx.wlocks = make([]*sync.RWMutex, 0, 3)
	tx.rlocks = make([]*sync.RWMutex, 0, 3)
	txPool <- tx
}

func NewUndo() TX {
	if undoPool[0] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-undoPool[0]
	// log.Println("Get log ", t.id)
	return t
}

func NewLargeUndo() TX {
	if undoPool[1] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-undoPool[1]
	// log.Println("Get log ", t.id)
	return t
}

func releaseUndo(t *undoTx) {
	t.Abort()
	// log.Println("Release log ", t.id)
	if t.large {
		undoPool[1] <- t
	} else {
		undoPool[0] <- t
	}
}

func (t *undoTx) setUndoHdr(tail int) {
	sfence()
	t.header.tail = tail // atomic update
	clflush(unsafe.Pointer(t.header))
	sfence()
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
		source_ptr := (*[LBUFFERSIZE]byte)(sshdr.data)[:size:size]
		dest_ptr := (*[LBUFFERSIZE]byte)(vshdr.data)[:size:size]
		copy(dest_ptr, source_ptr)

	case reflect.Ptr:
		oldv := reflect.Indirect(v1) // get the underlying data of pointer
		typ = oldv.Type()
		size = int(typ.Size())
		v2 = reflect.PNew(oldv.Type())
		reflect.Indirect(v2).Set(oldv) // copy old data
	default:
		debug.PrintStack()
		return errors.New("tx.undo: Log data must be pointer/slice!")
	}
	// Append data to log entry.
	tail := t.header.tail
	t.log[tail].ptr = unsafe.Pointer(v1.Pointer())  // point to original data
	t.log[tail].data = unsafe.Pointer(v2.Pointer()) // point to logged copy
	t.log[tail].size = size                         // size of data

	// Flush logged data copy and entry.
	Persist(t.log[tail].data, size)
	Persist(unsafe.Pointer(&t.log[tail]), int(unsafe.Sizeof(t.log[tail])))

	// Update log offset in header.
	t.setUndoHdr(tail + 1)
	return nil
}

func (t *undoTx) FakeLog(interface{}) {
	// No logging
}

func (t *undoTx) Begin() error {
	t.level += 1
	return nil
}

func (t *undoTx) Commit() error {
	if t.level == 0 {
		return errors.New("tx.undo: no transaction to commit!")
	}
	t.level--
	if t.level == 0 {
		defer t.unLock()
		/* Need to flush current value of logged areas. */
		for i := t.header.tail - 1; i >= 0; i-- {
			Persist(t.log[i].ptr, t.log[i].size)
		}
		t.setUndoHdr(0) // discard all logs.
	}
	return nil
}

func (t *undoTx) Abort() error {
	defer t.unLock()
	t.level = 0
	/* Replay undo logs. */
	for i := t.header.tail - 1; i >= 0; i-- {
		original := (*[LBUFFERSIZE]byte)(t.log[i].ptr)[:t.log[i].size:t.log[i].size]
		logdata := (*[LBUFFERSIZE]byte)(t.log[i].data)[:t.log[i].size:t.log[i].size]
		copy(original, logdata)
		Persist(t.log[i].ptr, t.log[i].size)
	}
	t.setUndoHdr(0)
	return nil
}

func (t *undoTx) RLock(m *sync.RWMutex) {
	m.RLock()
	//log.Println("Log ", t.id, " rlocking ", m)
	t.rlocks = append(t.rlocks, m)
}

func (t *undoTx) WLock(m *sync.RWMutex) {
	m.Lock()
	//log.Println("Log ", t.id, " wlocking ", m)
	t.wlocks = append(t.wlocks, m)
}

func (t *undoTx) Lock(m *sync.RWMutex) {
	t.WLock(m)
}

func (t *undoTx) unLock() {
	for _, m := range t.wlocks {
		//log.Println("Log ", t.id, " unlocking ", m)
		m.Unlock()
	}
	t.wlocks = t.wlocks[0:0]
	for _, m := range t.rlocks {
		//log.Println("Log ", t.id, " runlocking ", m)
		m.RUnlock()
	}
	t.rlocks = t.rlocks[0:0]
}
