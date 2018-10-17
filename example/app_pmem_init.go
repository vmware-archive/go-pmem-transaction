/*
 * This example currently implements a doubly linked list, instead of using list
 * package in Go. This file sets up persistent memory related metadata such as
 * appRoot. If restarted after a previous run, it would read all the data in its
 * previous incarnations. The actual doubly linkedlist operations are in
 * linkedlist.go.
 */

package main

import (
	"fmt"
	"go-pmem-transaction/transaction"
	"log"
	"runtime"
	"unsafe"
)

const (
	magicNum = 657071

	// pmem filesize must be multiple of 64 MB, specified by Go-pmem runtime
	dataSize = 64 * 1024 * 1024

	pmemOffset      = 0
	gcPercent       = 100
	rootDataEntries = 2
	rootDataHead    = 0
	rootDataTail    = 1
	firstDataValue  = 100
)

// Information about persistent memory region set as application root
type pmemHeader struct {

	// A magic constant stored at the beginning of the root section
	magic int

	// Persistent memory initialization size
	size int

	// The address at which persistent memory file is mapped
	mapAddr uintptr

	// Application Header, For this example, head & tail of doubly linked list
	appHeader [rootDataEntries]unsafe.Pointer

	// Transaction Log Header
	transactionLogHeadPtr unsafe.Pointer
}

// application root datastructure
var appPmemRoot *pmemHeader

var listHead, listTail *listNode

func initializeRoot(mapAddr unsafe.Pointer, size int) unsafe.Pointer {
	appPmemRoot = pnew(pmemHeader)
	appPmemRoot.magic = magicNum
	appPmemRoot.size = size
	appPmemRoot.mapAddr = uintptr(mapAddr)
	appPmemRoot.appHeader[rootDataHead] = unsafe.Pointer(createListHead(firstDataValue))
	appPmemRoot.appHeader[rootDataTail] = appPmemRoot.appHeader[rootDataHead]
	fmt.Println("[app_pmem_init] InitializeRoot, created listHead & listTail")
	runtime.PersistRange(unsafe.Pointer(appPmemRoot), unsafe.Sizeof(*appPmemRoot))
	return unsafe.Pointer(appPmemRoot)
}

func reInitializeRoot(appRootPtr unsafe.Pointer, size int) {
	appPmemRoot = (*pmemHeader)(appRootPtr)
	if appPmemRoot.magic != magicNum {
		log.Fatal("appRootPtr magic does not match!")
	}
	if appPmemRoot.size != size {
		log.Fatal("appRootPtr size does not match!")
	}
	fmt.Println("[app_pmem_init] got appRoot from prevRun")
	if appPmemRoot.appHeader[rootDataHead] == nil || appPmemRoot.appHeader[rootDataTail] == nil {
		log.Fatal("[app_pmem_init] didn't get listHead, listTail from prevRun")
	}
	fmt.Println("[app_pmem_init] got listHead, listTail from previous run")
}

func updateAppHeaderListHead(tx transaction.TX, newHead *listNode) {
	tx.Begin()
	tx.Log(&appPmemRoot.appHeader[rootDataHead])
	appPmemRoot.appHeader[rootDataHead] = unsafe.Pointer(newHead)
	tx.End()

	// PersistRange() on updated head is used here for testing the transaction
	// implementation. If the updated head has been persisted before a
	// transaction is over and there is a crash, updates should be successfully
	// rolled back. From application correctness point of view, the below
	// call is NOT needed.
	runtime.PersistRange(unsafe.Pointer(&appPmemRoot.appHeader[rootDataHead]), unsafe.Sizeof(appPmemRoot.appHeader[rootDataHead]))
}

func main() {
	fmt.Println("[app_pmem_init] starting")

	// pmem init TODO: Check if need to provide non-zero offset while init
	mapAddr := runtime.PmemInit("_testLog", dataSize, pmemOffset)
	if mapAddr == nil {
		log.Fatal("[app_pmem_init] Persistent memory initialization failed")
	}

	appRootPtr := runtime.GetRoot()
	if appRootPtr == nil { // indicates a first time initialization

		// Initialize application specific metadata which will be set as the
		// application root pointer.
		fmt.Println("[app_pmem_init] First run. Setting the appRoot")
		appRootPtr = initializeRoot(mapAddr, dataSize)

		// SetRoot() sets the app root pointer after making a pnew() call in
		// InitializeRoot(). Any updates to persistent memory until this point
		// can be done without transactional logging because without setting
		// the application root pointer, these updates will be lost in the next
		// run of the application.
		runtime.SetRoot(appRootPtr)
	} else {
		reInitializeRoot(appRootPtr, dataSize)
	}
	runtime.EnableGC(gcPercent)

	appPmemRoot.transactionLogHeadPtr = transaction.Init(appPmemRoot.transactionLogHeadPtr)
	runtime.PersistRange(unsafe.Pointer(&appPmemRoot.transactionLogHeadPtr), unsafe.Sizeof(appPmemRoot.transactionLogHeadPtr))
	listHead = (*listNode)(appPmemRoot.appHeader[rootDataHead])
	listTail = (*listNode)(appPmemRoot.appHeader[rootDataTail])

	startDlListOps()

	fmt.Println("[app_pmem_init] ending")
}
