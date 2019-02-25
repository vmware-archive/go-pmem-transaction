///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package transaction

import (
	"log"
	"reflect"
	"sync"
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
		Log(...interface{}) error
		ReadLog(interface{}) interface{}
		Exec(...interface{}) ([]reflect.Value, error)
		End() error
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
		Lock(*sync.RWMutex)
		abort() error
	}
)

func Init(logHeadPtr unsafe.Pointer, logType string) unsafe.Pointer {
	switch logType {
	case "undo":
		return initUndoTx(logHeadPtr)
	case "redo":
		return initRedoTx(logHeadPtr)
	default:
		log.Panic("initializing unsupported transaction! Try undo/redo")
	}
	return nil
}

func Release(t TX) {
	switch v := t.(type) {
	case *undoTx:
		releaseUndoTx(v)
	case *redoTx:
		releaseRedoTx(v)
	default:
		log.Panic("Releasing unsupported transaction!")
	}
}
