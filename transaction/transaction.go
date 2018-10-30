package transaction

import (
	"log"
	"reflect"
	"unsafe"
)

const (
	LOGSIZE     int = 4 * 1024 * 1024
	LBUFFERSIZE     = 512 * 1024
	BUFFERSIZE      = 4 * 1024
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(interface{}) error
		Exec(...interface{}) (error, []reflect.Value)
		FakeLog(interface{})
		End() error
		abort() error
	}
)

func Init(logHeadPtr unsafe.Pointer) unsafe.Pointer {

	// currently only support simple undo logging transaction.
	return initUndoTx(logHeadPtr)
}

func Release(t TX) {

	// currently only support simple undo logging transaction.
	switch v := t.(type) {
	case *undoTx:
		releaseUndoTx(v)
	default:
		log.Panic("Releasing unsupported transaction!")
	}
}
