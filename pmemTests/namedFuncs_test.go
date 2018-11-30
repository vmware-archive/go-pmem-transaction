package pmemTests

import (
	"fmt"
	"go-pmem-transaction/pmem"
	"go-pmem-transaction/transaction"
	"runtime/debug"
	"testing"
	"time"
)

const (
	// pmem filesize must be multiple of 64 MB, specified by Go-pmem runtime
	dataSize   = 64 * 1024 * 1024
	pmemOffset = 0
	gcPercent  = 100
)

type structPmemTest struct {
	a     int
	sptr  *structPmemTest
	slice []int
	b     bool
}

var (
	a       int
	slice1  []int
	struct1 structPmemTest
)

func init() {
	pmem.Init("tx_testFile", dataSize, pmemOffset, gcPercent)
}

func TestPNewPMake(t *testing.T) {
	tx := transaction.NewUndoTx()

	// named PNew with int type
	// cannot call pmem.PNew("region1", int) as this pmem package is outside
	// compiler
	fmt.Println("Testing PNew int region1")
	r1 := pmem.PNew("region1", a)
	b := (*int)(r1)
	tx.Begin()
	tx.Log(b)
	*b = 10
	tx.End() // since update is within log, no need to call persistRange
	assertEqual(t, *(*int)(r1), 10)
	r2 := pmem.PNew("region1", a)
	assertEqual(t, *(*int)(r2), 10) // Get back the same object

	// named PMake for slice of integers
	fmt.Println("Testing PMake []int region2")
	r3 := pmem.PMake("region2", slice1, 10)
	s1 := *(*[]int)(r3)
	tx.Begin()
	tx.Log(s1)
	s1[0] = 1
	tx.End()

	r4 := pmem.PMake("region2", slice1, 10) // Get back the same slice
	s2 := *(*[]int)(r4)
	assertEqual(t, s2[0], 1)
	assertEqual(t, len(s2), 10)

	// named PNew with struct type
	fmt.Println("Testing PNew struct region3")
	r5 := pmem.PNew("region3", struct1)
	st1 := (*structPmemTest)(r5)
	tx.Begin()
	tx.Log(st1)
	st1.a = 20
	st1.b = true
	st1.slice = pmake([]int, 100)
	tx.Log(st1.slice)
	st1.slice[0] = 11
	st1.slice[99] = 22
	tx.End()

	r7 := pmem.PNew("region3", struct1) // Get back the same struct
	st2 := (*structPmemTest)(r7)
	assertEqual(t, st2.a, 20)
	assertEqual(t, st2.b, true)
	assertEqual(t, len(st2.slice), 100)
	assertEqual(t, st2.slice[0], 11)
	assertEqual(t, st2.slice[99], 22)
	transaction.Release(tx)
	fmt.Println("Going to sleep for 10s. Crash here & run TestCrashRetention" +
		" to test crash consistency")
	time.Sleep(10 * time.Second)
}

func TestCrashRetention(t *testing.T) {
	fmt.Println("Getting region1 int")
	r1 := pmem.PNew("region1", a)
	assertEqual(t, *(*int)(r1), 10)

	fmt.Println("Getting region2 []int")
	r2 := pmem.PMake("region2", slice1, 10)
	s := *(*[]int)(r2)
	assertEqual(t, s[0], 1)
	assertEqual(t, len(s), 10)

	fmt.Println("Getting region3 struct")
	r1 = pmem.PNew("region3", struct1)
	st := (*structPmemTest)(r1)
	assertEqual(t, st.a, 20)
	assertEqual(t, st.b, true)
	assertEqual(t, len(st.slice), 100)
	assertEqual(t, st.slice[0], 11)
	assertEqual(t, st.slice[99], 22)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		debug.PrintStack()
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
