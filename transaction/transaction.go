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
	LBUFFERSIZE = 512 * 1024 // TODO: This could be a problem in the future
	MAGIC       = 131071
	LOGNUM      = 512
	NUMENTRIES  = 128
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(...interface{}) error
		ReadLog(...interface{}) interface{}
		Exec(...interface{}) ([]reflect.Value, error)
		End() error
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
		Lock(*sync.RWMutex)
		abort() error
	}

	// entry for each log update, stays in persistent heap.
	// ptr is the address of variable to be updated
	// data points to old data copy for undo log & new data for redo log
	entry struct {
		ptr           unsafe.Pointer
		data          unsafe.Pointer
		size          int
		sliceElemSize int // Non-zero value indicates ptr points to slice header
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
