package txTests

import (
	"fmt"
	"go-pmem-transaction/transaction"
	"log"
	"os"
	"runtime"
	"unsafe"
)

const (
	// pmem filesize must be multiple of 64 MB, specified by Go-pmem runtime
	dataSize   = 64 * 1024 * 1024
	magicNum   = 657071
	pmemOffset = 0
	gcPercent  = 100
)

// Information about persistent memory region set as application root
type pmemHeader struct {
	// A magic constant stored at the beginning of the root section
	magic int

	// Persistent memory initialization size
	size int

	// The address at which persistent memory file is mapped
	mapAddr uintptr

	// Transaction Log Header
	undoTxHeadPtr unsafe.Pointer
	redoTxHeadPtr unsafe.Pointer
}

// application root datastructure
var (
	appPmemRoot *pmemHeader
	setUpDone   bool
)

func initializeRoot(mapAddr unsafe.Pointer, size int) unsafe.Pointer {
	appPmemRoot = pnew(pmemHeader)
	appPmemRoot.magic = magicNum
	appPmemRoot.size = size
	appPmemRoot.mapAddr = uintptr(mapAddr)
	runtime.PersistRange(unsafe.Pointer(appPmemRoot),
		unsafe.Sizeof(*appPmemRoot))
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
	fmt.Println("got appRoot from prevRun")
}

func setup() {
	if setUpDone {
		return
	}
	os.Remove("tx_testFile")
	mapAddr := runtime.PmemInit("tx_testFile", dataSize, pmemOffset)
	if mapAddr == nil {
		log.Fatal("Persistent memory initialization failed")
	}
	appRootPtr := runtime.GetRoot()
	if appRootPtr == nil { // indicates a first time initialization

		// Initialize application specific metadata which will be set as the
		// application root pointer.
		fmt.Println("First run. Setting the appRoot")
		appRootPtr = initializeRoot(mapAddr, dataSize)

		// SetRoot() sets the app root pointer after making a pnew() call in
		// InitializeRoot(). Any updates to persistent memory until this point
		// can be done without transactional logging because without setting
		// the application root pointer, these updates will be lost in the next
		// run of the application.
		runtime.SetRoot(appRootPtr)
	} else {
		reInitializeRoot(appRootPtr, dataSize)
		fmt.Println("Got appRoot from previous run")
	}
	runtime.EnableGC(gcPercent)
	appPmemRoot.undoTxHeadPtr =
		transaction.Init(appPmemRoot.undoTxHeadPtr, "undo")
	appPmemRoot.redoTxHeadPtr =
		transaction.Init(appPmemRoot.redoTxHeadPtr, "redo")
	setUpDone = true
}
