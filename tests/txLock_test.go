package txTests

import (
	"fmt"
	"go-pmem-transaction/transaction"
	"sync"
	"testing"
	"time"
)

func TestUndoLogLock(t *testing.T) {
	fmt.Println("Testing UndoTx locking")
	m := new(sync.RWMutex)
	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.Lock(m)
	fmt.Println("Crash now & restart UndoLogLock to test no locks held" +
		"after crash")
	time.Sleep(5 * time.Second)
	transaction.Release(tx)
}

func TestRedoLogLock(t *testing.T) {
	fmt.Println("Testing RedoTx locking")
	m := new(sync.RWMutex)
	tx := transaction.NewRedoTx()
	tx.Begin()
	tx.Lock(m)
	fmt.Println("Crash now & restart RedoLogLock to see if no locks held" +
		"after crash")
	time.Sleep(5 * time.Second)
	transaction.Release(tx)
}
