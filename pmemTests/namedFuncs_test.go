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

func init() {
	pmem.Init("tx_testFile", dataSize, pmemOffset, gcPercent)
}

func TestNewMake(t *testing.T) {
	tx := transaction.NewUndoTx()
	// named New with int type
	// cannot call pmem.New("region1", int) as this pmem package is outside
	// compiler
	fmt.Println("Testing New int region1")
	var a *int
	a = (*int)(pmem.New("region1", a))
	tx.Begin()
	tx.Log(a)
	*a = 10
	tx.End() // since update is within log, no need to call persistRange
	assertEqual(t, *a, 10)
	var b *int
	b = (*int)(pmem.New("region1", b))
	assertEqual(t, *b, 10)

	// named Make for slice of integers
	fmt.Println("Testing Make []float64 region2")
	var slice1 []float64
	slice1 = pmem.Make("region2", slice1, 10).([]float64)
	tx.Begin()
	tx.Log(slice1)
	slice1[0] = 1.1
	tx.End()
	var slice2 []float64
	slice2 = pmem.Make("region2", slice2, 10).([]float64) // Get back same slice
	assertEqual(t, slice2[0], 1.1)
	assertEqual(t, len(slice2), 10)

	// named New with struct type
	fmt.Println("Testing New struct region3")
	var st1 *structPmemTest
	st1 = (*structPmemTest)(pmem.New("region3", st1))
	tx.Begin()
	tx.Log(st1)
	st1.a = 20
	st1.b = true
	st1.slice = pmake([]int, 100)
	tx.Log(st1.slice)
	st1.slice[0] = 11
	st1.slice[99] = 22
	tx.End()

	var st2 *structPmemTest
	st2 = (*structPmemTest)(pmem.New("region3", st2)) // Get back the same struct
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
	var a *int
	a = (*int)(pmem.New("region1", a))
	assertEqual(t, *a, 10)

	fmt.Println("Getting region2 []float64")
	var s []float64
	s = pmem.Make("region2", s, 10).([]float64)
	assertEqual(t, s[0], 1.1)
	assertEqual(t, len(s), 10)

	fmt.Println("Getting region3 struct")
	var st *structPmemTest
	st = (*structPmemTest)(pmem.New("region3", st))
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
