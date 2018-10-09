package transaction

import (
	"log"
	"sync"
	"unsafe"
)

const (
	LOGSIZE     int     = 4 * 1024 * 1024
	CACHELINE   uintptr = 64
	LBUFFERSIZE         = 512 * 1024
	BUFFERSIZE          = 4 * 1024
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(interface{}) error
		FakeLog(interface{})
		Commit() error
		Abort() error
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
		Lock(*sync.RWMutex)
	}
)

func Init(logArea []byte) {
	InitUndo(logArea[:len(logArea)])
}

func Release(t TX) {
	// currently only support simple undo logging transaction.
	switch v := t.(type) {
	case *undoTx:
		releaseUndo(v)
	case *readonlyTx:
		releaseReadonly(v)
	default:
		log.Panic("Releasing unsupported transaction!")
	}
}

// directly persist pmem range
func Persist(p unsafe.Pointer, s int)

func sfence()

func clflush(unsafe.Pointer)
