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
		ptr  unsafe.Pointer
	}
	pmemHeader struct {
		// Transaction Log Header. We don't know what the app might use. So,
		// initialize both undo & redo log
		undoTxHeadPtr unsafe.Pointer
		redoTxHeadPtr unsafe.Pointer

		// App-specific data structures, updated on named pnew, pmake calls
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

func Init(fileName string, size int, offset int, gcPercent int) {
	runtimeRootPtr, err := runtime.PmemInit(fileName, size, offset)
	if err != nil {
		log.Fatal("Persistent memory initialization failed")
	}
	if runtimeRootPtr == nil { // first time initialization
		rootPtr = pnew(pmemHeader)
		populateTxHeaderInRoot()
		rootPtr.appData = pmake([]namedObject, 1) // TODO: Start with size of 1
		runtime.PersistRange(unsafe.Pointer(rootPtr),
			unsafe.Sizeof(*rootPtr))
		runtime.SetRoot(unsafe.Pointer(rootPtr))
	} else {
		rootPtr = (*pmemHeader)(runtimeRootPtr)
		populateTxHeaderInRoot()
	}
	runtime.EnableGC(gcPercent)
	return
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

// Syntax: PMake("myName", []int, 10). Returns unsafe.Pointer to slice header
// PNew is used to create named slices in persistent heap.
// Only supports slices for now.
func PMake(name string, intf ...interface{}) unsafe.Pointer {
	v1 := reflect.ValueOf(intf[0])
	v2 := reflect.ValueOf(intf[1])
	if v1.Kind() != reflect.Slice {
		log.Fatal("Can only PMake slice")
	}
	for i, obj := range rootPtr.appData {
		if obj.name == name {
			return rootPtr.appData[i].ptr
		}
	}
	sTyp := v1.Type()
	sLen := int(v2.Int())
	newV := reflect.PMakeSlice(sTyp, sLen, sLen)
	vPtr := (*Value)(unsafe.Pointer(&newV))
	sliceHdr := pnew(sliceHeader)
	*sliceHdr = *(*sliceHeader)(vPtr.ptr)
	runtime.PersistRange(unsafe.Pointer(sliceHdr), unsafe.Sizeof(*sliceHdr))
	newNamedObj := namedObject{name, unsafe.Pointer(sliceHdr)}
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	runtime.PersistRange(unsafe.Pointer(&rootPtr.appData),
		uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	return unsafe.Pointer(sliceHdr)
}

// PNew is used to create named objects in persistent heap. This object would
// survive crashes. Returns unsafe.Pointer to object
// With name as key, find if an object with the name exists already.
// If found, no allocation needed, return existing object pointer
// If not found, allocate new object & persist changes
func PNew(name string, intf interface{}) unsafe.Pointer {
	for i, obj := range rootPtr.appData {
		if obj.name == name {
			return rootPtr.appData[i].ptr
		}
	}
	newObj := reflect.PNew(reflect.TypeOf(intf))
	newNamedObj := namedObject{name, unsafe.Pointer(newObj.Pointer())}
	rootPtr.appData = append(rootPtr.appData, newNamedObj)
	runtime.PersistRange(unsafe.Pointer(&rootPtr.appData),
		uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	return unsafe.Pointer(newObj.Pointer())
}
