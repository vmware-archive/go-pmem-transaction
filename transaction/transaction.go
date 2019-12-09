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
	maxInt     = 1<<31 - 1
	magic      = 131071
	logNum     = 512
	NumEntries = 128
	ptrSize    = 8 // Size of an integer or pointer value in Go
	cacheSize  = 64
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(...interface{}) error
		Log2(src, dst unsafe.Pointer, size uintptr) error
		Log3(src unsafe.Pointer, size uintptr) error
		ReadLog(...interface{}) interface{}
		Exec(...interface{}) ([]reflect.Value, error)
		End() bool
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
		Lock(*sync.RWMutex)
	}

	// entry for each log update, stays in persistent heap.
	// ptr is the address of variable to be updated
	// data points to old data copy for undo log & new data for redo log
	entry struct {
		ptr    unsafe.Pointer
		data   unsafe.Pointer
		genNum int
		size   int
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
	case *vData:
		releaseUndoTx(v)
	case *redoTx:
		releaseRedoTx(v)
	default:
		log.Panic("Releasing unsupported transaction!")
	}
}
