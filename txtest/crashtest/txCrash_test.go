// +build crash

// This test needs to be run twice to check for data consistency after crash.
// Hence this test is not run by default and will only be run if a flag
// 'crash' is specified while running the tests.
//
// E.g.: ~/go-pmem/bin/go test -tags="crash" -v # run 1
// E.g.: ~/go-pmem/bin/go test -tags="crash" -v # run 2

package crashtest

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/vmware/go-pmem-transaction/pmem"
	"github.com/vmware/go-pmem-transaction/transaction"
)

type st struct {
	slice []int
}

// This test makes sure that undo transaction doesn't store any contents of a
// slice in volatile memory. If it does, on the restart-after-crash path, GC
// will zero out the volatile pointers stored in the log entries, and abort will
// segfault.
func TestUpdateCrash(t *testing.T) {
	firstInit := pmem.Init("testfile")
	var st1 *st
	if firstInit {
		st1 = (*st)(pmem.New("r1", st1))
		tx := transaction.NewUndoTx()
		tx.Begin()
		tx.Log(&st1.slice)
		//slicehdr is in pmem, but create slice contents in volatile memory
		st1.slice = make([]int, 3)
		tx.End()

		st2 := (*st)(pmem.Get("r1", st1))
		tx.Begin()

		tx.Log(&st2.slice, []int{10, 20, 30, 40})
		fmt.Println("[TestUpdateCrash]: Crashing now")
		log.Fatal("") // <-- CRASHHHHH!
	} else {
		fmt.Println("Testing abort when logged slice is in volatile memory")
		st2 := (*st)(pmem.Get("r1", st1))
		if len(st2.slice) != 3 {
			t.Errorf("want = %d, got = %d", 3, len(st2.slice))
		}
		if st2.slice != nil {
			t.Errorf("want = nil, got = %v", st2.slice)
		}
		fmt.Println("[TestUpdateCrash] successful")
		os.Remove("testfile")
	}
}
