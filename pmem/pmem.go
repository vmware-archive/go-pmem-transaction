package pmem

import (
	"errors"
	"go-pmem-transaction/transaction"
	"log"
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

type (
	namedObject struct {
		name string
		typ  string
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
func Init(fileName string, size int, offset int, gcPercent int) bool {
	runtimeRootPtr, err := runtime.PmemInit(fileName, size, offset)
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
	runtime.EnableGC(gcPercent)
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
// already exists, it is updated to point to the new object.
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
	v2 := reflect.ValueOf(intf[1])
	sLen := int(v2.Int())
	newV := reflect.PMakeSlice(sTyp, sLen, sLen)
	vPtr := (*value)(unsafe.Pointer(&newV))
	sliceHdr := pnew(sliceHeader)
	*sliceHdr = *(*sliceHeader)(vPtr.ptr)
	runtime.PersistRange(unsafe.Pointer(sliceHdr), unsafe.Sizeof(*sliceHdr))
	newNamedObj := namedObject{name, sTypString, unsafe.Pointer(sliceHdr)}
	tx := transaction.NewUndoTx()
	m.Lock()
	tx.Begin()
	if found {
		tx.Log(&rootPtr.appData[i])
		rootPtr.appData[i] = newNamedObj
	} else {
		tx.Log(&rootPtr.appData)
		rootPtr.appData = append(rootPtr.appData, newNamedObj)
		runtime.PersistRange(unsafe.Pointer(&rootPtr.appData[0]),
			uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	}
	tx.End()
	m.Unlock()
	transaction.Release(tx)
	slicePtrWithTyp := reflect.NewAt(sTyp, unsafe.Pointer(sliceHdr))
	sliceVal := reflect.Indirect(slicePtrWithTyp)
	return sliceVal.Interface()
}

// New is used to create named objects in persistent heap. This object would
// survive crashes. Returns unsafe.Pointer to the object
// If an object with same name already exists, it is updated to point to the new
// object.
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
	newObj := reflect.PNew(t.Elem()) //Elem() returns type of object t points to
	newNamedObj := namedObject{name, ts, unsafe.Pointer(newObj.Pointer())}
	tx := transaction.NewUndoTx()
	m.Lock()
	tx.Begin()
	if found { // object with name already exists, update to point to new type
		tx.Log(&rootPtr.appData[i])
		rootPtr.appData[i] = newNamedObj
	} else { // add to root pointer
		tx.Log(&rootPtr.appData)
		rootPtr.appData = append(rootPtr.appData, newNamedObj)
		runtime.PersistRange(unsafe.Pointer(&rootPtr.appData[0]),
			uintptr(len(rootPtr.appData))*unsafe.Sizeof(newNamedObj))
	}
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
	runtime.PersistRange(unsafe.Pointer(&rootPtr.appData[0]),
		uintptr(len(rootPtr.appData))*unsafe.Sizeof(rootPtr.appData[0]))
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
	if obj.typ != ts {
		log.Fatal("Object ", obj.name, "was created before with type ", obj.typ)
	}
	return rootPtr.appData[i].ptr
}

// GetSlice is Get() for named slices. Syntax same as Make()
func GetSlice(name string, intf ...interface{}) interface{} {
	v1 := reflect.ValueOf(intf[0])
	if v1.Kind() != reflect.Slice {
		log.Fatal("Can only pmem.Make slice")
	}
	found, i := exists(name)
	if !found {
		return nil
	}
	sTyp := v1.Type()
	sTypString := sTyp.PkgPath() + sTyp.String()
	obj := rootPtr.appData[i]
	if obj.typ != sTypString {
		log.Fatal("Object ", obj.name, " was made before with type ", obj.typ)
	}
	slicePtrWithTyp := reflect.NewAt(sTyp, obj.ptr)
	sliceVal := reflect.Indirect(slicePtrWithTyp)
	return sliceVal.Interface()
}

func exists(name string) (found bool, i int) {
	var obj namedObject
	m.RLock()
	for i, obj = range rootPtr.appData {
		if obj.name == name {
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
