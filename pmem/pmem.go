package pmem

import (
	"go-pmem-transaction/transaction"
	"log"
	"reflect"
	"runtime"
	"unsafe"
)

type (
	namedObject struct {
		name string
		typ  reflect.Type
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

var rootPtr *pmemHeader

func populateTxHeaderInRoot() {
	rootPtr.undoTxHeadPtr = transaction.Init(rootPtr.undoTxHeadPtr, "undo")
	rootPtr.redoTxHeadPtr = transaction.Init(rootPtr.redoTxHeadPtr, "redo")
}

// Init returns true if this was a first time initialization.
func Init(fileName string, size int, offset int, gcPercent int) bool {
	runtimeRootPtr, err := runtime.PmemInit(fileName, size, offset)
	if err != nil {
		log.Fatal("Persistent memory initialization failed")
	}
	var firstInit bool
	if runtimeRootPtr == nil { // first time initialization
		rootPtr = pnew(pmemHeader)
		populateTxHeaderInRoot()
		rootPtr.appData = pmake([]namedObject, 1) // TODO: Start with size of 1
		runtime.PersistRange(unsafe.Pointer(rootPtr),
			unsafe.Sizeof(*rootPtr))
		runtime.SetRoot(unsafe.Pointer(rootPtr))
		firstInit = true
	} else {
		rootPtr = (*pmemHeader)(runtimeRootPtr)
		populateTxHeaderInRoot()
	}
	runtime.EnableGC(gcPercent)
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
// persistent heap. Only supports slices for now.
// Syntax: Make("myName", []int, 10)
func Make(name string, intf ...interface{}) interface{} {
	v1 := reflect.ValueOf(intf[0])
	if v1.Kind() != reflect.Slice {
		log.Fatal("Can only pmem.Make slice")
	}
	sTyp := v1.Type()
	for i, obj := range rootPtr.appData {
		if obj.name == name {
			if obj.typ != sTyp {
				log.Fatal("Object was made before with type", obj.typ)
			}
			slicePtrWithTyp := reflect.NewAt(sTyp, rootPtr.appData[i].ptr)
			sliceVal := reflect.Indirect(slicePtrWithTyp)
			return sliceVal.Interface()
		}
	}
	v2 := reflect.ValueOf(intf[1])
	sLen := int(v2.Int())
	newV := reflect.PMakeSlice(sTyp, sLen, sLen)
	vPtr := (*value)(unsafe.Pointer(&newV))
	sliceHdr := pnew(sliceHeader)
	*sliceHdr = *(*sliceHeader)(vPtr.ptr)
	runtime.PersistRange(unsafe.Pointer(sliceHdr), unsafe.Sizeof(*sliceHdr))
	newNamedObj := namedObject{name, sTyp, unsafe.Pointer(sliceHdr)}
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.Log(&rootPtr.appData)
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	runtime.PersistRange(unsafe.Pointer(&rootPtr.appData[0]),
		uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	tx.End()
	transaction.Release(tx)
	slicePtrWithTyp := reflect.NewAt(sTyp, unsafe.Pointer(sliceHdr))
	sliceVal := reflect.Indirect(slicePtrWithTyp)
	return sliceVal.Interface()
}

// New is used to create named objects in persistent heap. This object would
// survive crashes. Returns unsafe.Pointer to the object
// If an object with same name is found, pointer to the existing object is
// returned else a new object is allocated & persisted.
func New(name string, intf interface{}) unsafe.Pointer {
	v := reflect.ValueOf(intf)
	t := v.Type()
	for i, obj := range rootPtr.appData {
		if obj.name == name {
			if obj.typ != t {
				log.Fatal("Object was made before with type ", obj.typ)
			}
			return rootPtr.appData[i].ptr
		}
	}
	newObj := reflect.PNew(t.Elem()) //Elem() returns type of object t points to
	newNamedObj := namedObject{name, t, unsafe.Pointer(newObj.Pointer())}
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.Log(&rootPtr.appData)
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	runtime.PersistRange(unsafe.Pointer(&rootPtr.appData[0]),
		uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	tx.End()
	transaction.Release(tx)
	return unsafe.Pointer(newObj.Pointer())
}
