///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package pmem

import (
	"errors"
	"fmt"
	"github.com/vmware/go-pmem-transaction/transaction"
	"log"
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

type (
	namedObject struct {
		name []byte
		typ  []byte
		ptr  unsafe.Pointer
	}
	pmemHeader struct {
		// Transaction Log Header. We don't know what the app might use. So,
		// initialize both undo & redo log
		undoTxHeadPtr unsafe.Pointer
		redoTxHeadPtr unsafe.Pointer

		// App-specific data structures, updated on named New, Make calls
		// TODO: Use map, since this is a key-value pair, but Persistent maps
		// are not supported yet.
		appData []namedObject
	}
)

var (
	rootPtr *pmemHeader
	m       *sync.RWMutex // Updates to map are thread safe through this lock
)

func populateTxHeaderInRoot() {
	rootPtr.undoTxHeadPtr = transaction.Init(rootPtr.undoTxHeadPtr, "undo")
	rootPtr.redoTxHeadPtr = transaction.Init(rootPtr.redoTxHeadPtr, "redo")
}

// Init returns true if this was a first time initialization.
func Init(fileName string) bool {
	runtimeRootPtr, err := runtime.PmemInit(fileName)
	if err != nil {
		log.Fatal("Persistent memory initialization failed")
	}
	var firstInit bool
	if runtimeRootPtr == nil { // first time initialization
		rootPtr = pnew(pmemHeader)
		populateTxHeaderInRoot()
		rootPtr.appData = pmake([]namedObject, 1) // Start with size of 1
		runtime.PersistRange(unsafe.Pointer(rootPtr),
			unsafe.Sizeof(*rootPtr))
		runtime.SetRoot(unsafe.Pointer(rootPtr))
		firstInit = true
	} else {
		rootPtr = (*pmemHeader)(runtimeRootPtr)
		populateTxHeaderInRoot()
	}
	m = new(sync.RWMutex)
	return firstInit
}

type value struct {
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

// Make returns the interface to the slice asked for, slice being created in
// persistent heap. Only supports slices for now. If an object with same name
// already exists, it panics.
// Syntax:   var s []int
//           s = pmem.Make("myName", s, 10).([]int)
func Make(name string, intf ...interface{}) interface{} {
	v1 := reflect.ValueOf(intf[0])
	if v1.Kind() != reflect.Slice {
		log.Fatal("Can only pmem.Make slice")
	}
	found, i := exists(name)
	sTyp := v1.Type()
	sTypString := sTyp.PkgPath() + sTyp.String()
	if found {
		obj := rootPtr.appData[i]
		panic(fmt.Sprintf("Object %s already exists", string(obj.name[:])))
	}
	v2 := reflect.ValueOf(intf[1])
	sLen := int(v2.Int())
	newV := reflect.PMakeSlice(sTyp, sLen, sLen)
	vPtr := (*value)(unsafe.Pointer(&newV))
	sliceHdr := pnew(sliceHeader)
	*sliceHdr = *(*sliceHeader)(vPtr.ptr)
	runtime.PersistRange(unsafe.Pointer(sliceHdr), unsafe.Sizeof(*sliceHdr))
	nameByte := pmake([]byte, len(name))
	sTypByte := pmake([]byte, len(sTypString))
	copy(nameByte, name)
	copy(sTypByte, sTypString)
	runtime.PersistRange(unsafe.Pointer(&nameByte[0]), uintptr(len(nameByte)))
	runtime.PersistRange(unsafe.Pointer(&sTypByte[0]), uintptr(len(sTypByte)))
	newNamedObj := namedObject{nameByte, sTypByte, unsafe.Pointer(sliceHdr)}
	tx := transaction.NewUndoTx()
	m.Lock()
	tx.Begin()
	tx.Log(&rootPtr.appData) // add to root pointer
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	tx.End()
	m.Unlock()
	transaction.Release(tx)
	slicePtrWithTyp := reflect.NewAt(sTyp, unsafe.Pointer(sliceHdr))
	sliceVal := reflect.Indirect(slicePtrWithTyp)
	return sliceVal.Interface()
}

// New is used to create named objects in persistent heap. This object would
// survive crashes. Returns unsafe.Pointer to the object if the creation was
// successful. If an object with same name already exists, it panics.
// Syntax: var a *int
//         a = (*int)(pmem.New("myName", a))
func New(name string, intf interface{}) unsafe.Pointer {
	v := reflect.ValueOf(intf)
	if v.Kind() == reflect.Slice {
		log.Fatal("Cannot create new slice with New. Try Make")
	}
	t := v.Type()
	ts := t.PkgPath() + t.String()
	found, i := exists(name)
	if found {
		obj := rootPtr.appData[i]
		panic(fmt.Sprintf("Object %s already exists", string(obj.name[:])))
	}
	nameByte := pmake([]byte, len(name))
	tByte := pmake([]byte, len(ts))
	newObj := reflect.PNew(t.Elem()) //Elem() returns type of object t points to
	copy(nameByte, name)
	copy(tByte, ts)
	runtime.PersistRange(unsafe.Pointer(&nameByte[0]), uintptr(len(nameByte)))
	runtime.PersistRange(unsafe.Pointer(&tByte[0]), uintptr(len(tByte)))
	newNamedObj := namedObject{nameByte, tByte, unsafe.Pointer(newObj.Pointer())}
	tx := transaction.NewUndoTx()
	m.Lock()
	tx.Begin()
	tx.Log(&rootPtr.appData) // add to root pointer
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	tx.End()
	m.Unlock()
	transaction.Release(tx)
	return unsafe.Pointer(newObj.Pointer())
}

// Delete deletes a named object created using New or Make. Returns error if
// no such object exists
func Delete(name string) error {
	found, i := exists(name)
	if !found {
		return errors.New("No such object allocated before")
	}
	tx := transaction.NewUndoTx()
	m.Lock()
	tx.Begin()
	tx.Log(&rootPtr.appData)
	rootPtr.appData = append(rootPtr.appData[:i], rootPtr.appData[i+1:]...)
	tx.End()
	m.Unlock()
	transaction.Release(tx)
	return nil
}

// Get the named object if it exists. Returns an unsafe pointer to the object
// if it was made before. Return nil otherwise. Syntax same as New()
func Get(name string, intf interface{}) unsafe.Pointer {
	found, i := exists(name)
	if !found {
		return nil
	}
	v := reflect.ValueOf(intf)
	if v.Kind() == reflect.Slice {
		log.Fatal("Cannot get slice with Get. Try GetSlice")
	}
	t := v.Type()
	ts := t.PkgPath() + t.String()
	obj := rootPtr.appData[i]
	if string(obj.typ[:]) != ts {
		log.Fatal("Object ", string(obj.name[:]), "was created before with ",
			"type ", string(obj.typ[:]))
	}
	return obj.ptr
}

// GetSlice is Get() for named slices. Syntax same as Make()
func GetSlice(name string, intf ...interface{}) interface{} {
	v1 := reflect.ValueOf(intf[0])
	if v1.Kind() != reflect.Slice {
		log.Fatal("Can only GetSlice to retrieve named slices")
	}
	found, i := exists(name)
	if !found {
		return nil
	}
	sTyp := v1.Type()
	sTypString := sTyp.PkgPath() + sTyp.String()
	obj := rootPtr.appData[i]
	if string(obj.typ[:]) != sTypString {
		log.Fatal("Object ", string(obj.name[:]), " was made before with type ",
			string(obj.typ[:]))
	}
	slicePtrWithTyp := reflect.NewAt(sTyp, obj.ptr)
	sliceVal := reflect.Indirect(slicePtrWithTyp)
	return sliceVal.Interface()
}

func exists(name string) (found bool, i int) {
	var obj namedObject
	m.RLock()
	for i, obj = range rootPtr.appData {
		if string(obj.name[:]) == name {
			found = true
			break
		}
	}
	m.RUnlock()
	if !found {
		return found, -1
	}
	return found, i
}
