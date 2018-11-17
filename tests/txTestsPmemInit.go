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
	pmemOffset = 0
	gcPercent  = 100
)

// Information about persistent memory region set as application root
type pmemHeader struct {
	// Transaction Log Header
	undoTxHeadPtr unsafe.Pointer
	redoTxHeadPtr unsafe.Pointer
}

// application root datastructure
var appPmemRoot *pmemHeader

func populateRoot() {
	appPmemRoot.undoTxHeadPtr =
		transaction.Init(appPmemRoot.undoTxHeadPtr, "undo")
	appPmemRoot.redoTxHeadPtr =
		transaction.Init(appPmemRoot.redoTxHeadPtr, "redo")
}

func init() {
	os.Remove("tx_testFile")
	appRootPtr, err := runtime.PmemInit("tx_testFile", dataSize, pmemOffset)
	if err != nil {
		log.Fatal("Persistent memory initialization failed")
	}
	if appRootPtr == nil { // first time initialization
		fmt.Println("First run. Setting the appRoot")
		appPmemRoot = pnew(pmemHeader)
		populateRoot()
		runtime.PersistRange(unsafe.Pointer(appPmemRoot),
			unsafe.Sizeof(*appPmemRoot))
		runtime.SetRoot(unsafe.Pointer(appPmemRoot))
	} else {
		fmt.Println("Got appRoot from previous run")
		appPmemRoot = (*pmemHeader)(appRootPtr)
		populateRoot()
	}
	runtime.EnableGC(gcPercent)
}
