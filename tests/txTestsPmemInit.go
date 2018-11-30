package txTests

import (
	"go-pmem-transaction/pmem"
	"os"
)

const (
	// pmem filesize must be multiple of 64 MB, specified by Go-pmem runtime
	dataSize   = 64 * 1024 * 1024
	pmemOffset = 0
	gcPercent  = 100
)

func init() {
	os.Remove("tx_testFile")
	pmem.Init("tx_testFile", dataSize, pmemOffset, gcPercent)
}
