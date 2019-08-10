package txtest

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/vmware/go-pmem-transaction/transaction"
)

type dataObject struct {
	val   int
	data  [8]int
	links [8]*int
	slc   []int
}

var (
	obj *dataObject
)

func resetLogBData() {
	obj = pnew(dataObject)
}

func TestUndoLogBBasic(t *testing.T) {
	fmt.Println("Testing LogB undo logging basic tests.")
	resetLogBData()
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.LogB(unsafe.Pointer(&obj.val))
	obj.val = 15
	tx.Log(&obj.data[5])
	obj.data[5] = 25
	tx.LogB(unsafe.Pointer(&obj.links[4]))
	obj.links[4] = &obj.data[5]
	tx.End()
	transaction.Release(tx)
	assertEqual(t, obj.val, 15)
	assertEqual(t, obj.data[5], 25)
	assertEqual(t, *obj.links[4], 25)
}

// This test uses multiple handles to update the datastructure but each
// handle updates unqiue data elements in the data structure.
func TestUndoLogMultipleHandles(t *testing.T) {
	fmt.Println("Testing LogB undo logging using multiple handles.")
	resetLogBData()
	tx1 := transaction.NewUndoTx()
	tx1.Begin()
	tx1.LogB(unsafe.Pointer(&obj.val))
	obj.val = 0x11223344
	tx1.Log(&obj.data[5])
	obj.data[5] = 0x991122883

	tx2 := transaction.NewUndoTx()
	tx2.Begin()
	tx2.LogB(unsafe.Pointer(&obj.links[4]))
	obj.links[4] = &obj.data[5]
	tx2.Log(&obj.data[1])
	obj.data[1] = 0xFF11EE22DD
	tx2.End()
	tx1.End()
	transaction.Release(tx2)
	transaction.Release(tx1)

	assertEqual(t, obj.val, 0x11223344)
	assertEqual(t, obj.data[5], 0x991122883)
	assertEqual(t, *obj.links[4], 0x991122883)
	assertEqual(t, obj.data[1], 0xFF11EE22DD)
}

func TestUndoLogBAbort(t *testing.T) {
	fmt.Println("Testing LogB undo logging abort.")
	resetLogBData()
	obj.data[5] = 0xABCD1234
	obj.val = 0xBADC0DE
	obj.links[4] = &obj.data[5]
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.LogB(unsafe.Pointer(&obj.data[5]))
	obj.data[5] = 15
	tx.LogB(unsafe.Pointer(&obj.val))
	obj.val = 0xBFEDCA12
	tx.Log(&obj.links[4])
	obj.links[4] = &obj.data[3]
	transaction.Release(tx)
	assertEqual(t, obj.data[5], 0xABCD1234)
	assertEqual(t, obj.val, 0xBADC0DE)
	assertEqual(t, *obj.links[4], 0xABCD1234)
}

func TestUndoLogMixed(t *testing.T) {
	// Test that abort satisfies the undo logging ordering
	fmt.Println("Testing intermixing undo logging LogB and Log calls.")
	resetLogBData()
	obj.data[2] = 0xFFFFAAAA
	obj.links[1] = &obj.data[1]
	obj.val = 0x1234FEDC
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.Log(obj)
	obj.data[2] = 0x66665555
	obj.links[1] = &obj.data[0]
	obj.val = 0x98765432

	tx.Log(obj)

	tx.LogB(unsafe.Pointer(&obj.data[2]))
	obj.data[2] = 0xDEADC0DE
	obj.val = 0xAAAAFFFF

	transaction.Release(tx)
	assertEqual(t, obj.data[2], 0xFFFFAAAA)
	assertEqual(t, obj.val, 0x1234FEDC)
}

func TestUndoNestedCommit(t *testing.T) {
	fmt.Println("Testing LogB undo logging nested transaction commit.")
	resetLogBData()

	tx := transaction.NewUndoTx()

	// Initialize some data first
	tx.Begin()
	tx.Log(&obj)
	obj.val = 0xDA7ADA7A
	obj.data[4] = 0x73514CA6
	obj.links[3] = &obj.data[4]
	obj.links[5] = &obj.val
	obj.data[6] = 0x19283745
	tx.End()

	// Begin nested logs
	tx.Begin()
	tx.LogB(unsafe.Pointer(&obj.val))
	obj.val = 0xFA1B2C3D4
	tx.LogB(unsafe.Pointer(&obj.data[4]))
	obj.data[4] = 0xA9B8C7D6
	tx.Log(&obj.links[3])
	obj.links[3] = &obj.data[0]
	tx.Log(&obj.links[5])
	obj.links[5] = &obj.data[7]

	tx.Begin()
	tx.Log(&obj.val)
	obj.val = 0x9A8B7C6D
	tx.LogB(unsafe.Pointer(&obj.data[4]))
	obj.data[4] = 0xFFEEDDCC
	tx.Log(&obj.links[3])
	obj.links[3] = &obj.data[6]
	tx.LogB(unsafe.Pointer(&obj.links[5])) // safe
	obj.links[5] = &obj.data[4]
	tx.End()

	tx.End()
	transaction.Release(tx)

	assertEqual(t, obj.val, 0x9A8B7C6D)
	assertEqual(t, obj.data[4], 0xFFEEDDCC)
	assertEqual(t, *obj.links[3], 0x19283745)
	assertEqual(t, *obj.links[5], 0xFFEEDDCC)
}

func TestUndoNestedAbort(t *testing.T) {
	fmt.Println("Testing LogB undo logging nested transaction abort.")
	resetLogBData()
	tx := transaction.NewUndoTx()

	// Initialize some data first
	tx.Begin()
	tx.Log(&obj)
	obj.val = 0xDA7ADA7A
	obj.data[4] = 0x73514CA6
	obj.links[3] = &obj.data[4]
	obj.links[5] = &obj.val
	tx.End()

	// Begin nested logs
	tx.Begin()
	tx.LogB(unsafe.Pointer(&obj.val))
	obj.val = 0xFA1B2C3D4
	tx.LogB(unsafe.Pointer(&obj.data[4]))
	obj.data[4] = 0xA9B8C7D6
	tx.Log(&obj.links[3])
	obj.links[3] = &obj.data[0]
	tx.Log(&obj.links[5])
	obj.links[5] = &obj.data[7]

	tx.Begin()
	tx.Log(&obj.val)
	obj.val = 0x9A8B7C6D
	tx.LogB(unsafe.Pointer(&obj.data[4]))
	obj.data[4] = 0xFFEEDDCC
	tx.Log(&obj.links[3])
	obj.links[3] = &obj.data[6]
	tx.LogB(unsafe.Pointer(&obj.links[5])) // safe
	obj.links[5] = &obj.data[4]

	transaction.Release(tx)

	assertEqual(t, obj.val, 0xDA7ADA7A)
	assertEqual(t, obj.data[4], 0x73514CA6)
	assertEqual(t, *obj.links[3], 0x73514CA6)
	assertEqual(t, *obj.links[5], 0xDA7ADA7A)
}

func TestUndoLogBExpand(t *testing.T) {
	fmt.Println("Testing LogB undo logging expansion - commit")
	resetLogBData()
	tx := transaction.NewUndoTx()
	tx.Begin()
	sz := transaction.NumEntries * 4
	obj.slc = pmake([]int, sz)
	for i := 0; i < sz; i++ {
		tx.LogB(unsafe.Pointer(&obj.slc[i]))
		obj.slc[i] = i
	}
	tx.End()
	transaction.Release(tx)

	for i := 0; i < sz; i++ {
		assertEqual(t, obj.slc[i], i)
	}

	fmt.Println("Testing LogB undo logging expansion - abort")
	tx = transaction.NewUndoTx()
	tx.Begin()
	obj.slc = pmake([]int, sz)
	for i := 0; i < sz; i++ {
		tx.LogB(unsafe.Pointer(&obj.slc[i]))
		obj.slc[i] = i
	}
	transaction.Release(tx)

	for i := 0; i < sz; i++ {
		assertEqual(t, obj.slc[i], 0)
	}
}
