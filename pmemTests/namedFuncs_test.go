package pmemTests

import (
	"errors"
	"fmt"
	"go-pmem-transaction/pmem"
	"go-pmem-transaction/transaction"
	"os"
	"os/exec"
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

func TestAPIs(t *testing.T) {
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
	b = (*int)(pmem.Get("region1", b))
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
	slice2 = pmem.GetSlice("region2", slice2, 10).([]float64) // Get same slice
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
	st2 = (*structPmemTest)(pmem.Get("region3", st2)) // Get back same struct
	assertEqual(t, st2.a, 20)
	assertEqual(t, st2.b, true)
	assertEqual(t, len(st2.slice), 100)
	assertEqual(t, st2.slice[0], 11)
	assertEqual(t, st2.slice[99], 22)

	fmt.Println("Testing Delete with int & region4")
	var c *int
	c = (*int)(pmem.New("region4", c))
	tx.Begin()
	tx.Log(c)
	*c = 100
	tx.End()
	if err := pmem.Delete("region4"); err != nil {
		assert(t)
	}
	err1 := pmem.Delete("region4") // This should return error
	assertEqual(t, err1.Error(), errors.New("No such object allocated before").
		Error())
	c = (*int)(pmem.New("region4", c))
	assertEqual(t, *c, 0) // above New() call would have allocated a new int
	pmem.Delete("region4")

	fmt.Println("Testing Update")
	var st3 *structPmemTest
	st3 = (*structPmemTest)(pmem.New("region4", st3))
	tx.Begin()
	tx.Log(st3)
	*st3 = *st2
	tx.End()
	assertEqual(t, st2.a, st3.a)
	assertEqual(t, st2.b, st3.b)
	assertEqual(t, st2.sptr, st3.sptr)
	assertEqual(t, len(st2.slice), len(st3.slice))
	assertEqual(t, st2.slice[0], st3.slice[0])
	assertEqual(t, st2.slice[99], st3.slice[99])
	var d *int
	if pmem.Get("region4", st3) != nil {
		if err := pmem.Delete("region4"); err != nil {
			assert(t)
		}

		d = (*int)(pmem.New("region4", d))
		*d = 1
		assertEqual(t, *d, 1)
	} else {
		assert(t)
	}
	if pmem.Get("region4", d) == nil {
		assert(t)
	}

	transaction.Release(tx)
	fmt.Println("Going to sleep for 10s. Crash here & run TestCrashRetention" +
		" to test crash consistency")
	time.Sleep(10 * time.Second)
}

func TestCrashRetention(t *testing.T) {
	fmt.Println("Getting region1 int")
	var a *int
	a = (*int)(pmem.Get("region1", a))
	assertEqual(t, *a, 10)

	fmt.Println("Getting region2 []float64")
	var s []float64
	s = pmem.GetSlice("region2", s, 10).([]float64)
	assertEqual(t, s[0], 1.1)
	assertEqual(t, len(s), 10)

	fmt.Println("Getting region3 struct")
	var st *structPmemTest
	st = (*structPmemTest)(pmem.Get("region3", st))
	assertEqual(t, st.a, 20)
	assertEqual(t, st.b, true)
	assertEqual(t, len(st.slice), 100)
	assertEqual(t, st.slice[0], 11)
	assertEqual(t, st.slice[99], 22)

	fmt.Println("Getting region4 which was updated to int")
	var b *int
	b = (*int)(pmem.Get("region4", b))
	assertEqual(t, *b, 1)
}

func apiCrash1() {
	var a *int
	a = (*int)(pmem.New("region5", a))
	var b *float64
	b = (*float64)(pmem.Get("region5", b))
}

func apiCrash2() {
	var a []int
	a = pmem.Make("region6", a, 10).([]int)
	var b []float64
	b = pmem.GetSlice("region6", b).([]float64)
}

func apiCrash3() {
	var a *int
	a = (pmem.Make("region7", a, 10)).(*int)
}

func TestAPICrash1(t *testing.T) {
	fmt.Println("Testing API crash for New() & Get()")
	if os.Getenv("CRASH_RUN") == "1" {
		apiCrash1()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestAPICrash1")
	cmd.Env = append(os.Environ(), "CRASH_RUN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		if pmem.Delete("region5") != nil {
			assert(t)
		}
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestAPICrash2(t *testing.T) {
	fmt.Println("Testing API crash for Make() & GetSlice()")
	if os.Getenv("CRASH_RUN") == "1" {
		apiCrash2()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestAPICrash2")
	cmd.Env = append(os.Environ(), "CRASH_RUN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		if pmem.Delete("region6") != nil {
			assert(t)
		}
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestAPICrash3(t *testing.T) {
	fmt.Println("Testing API crash for incorrect args to Make()")
	if os.Getenv("CRASH_RUN") == "1" {
		apiCrash3()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestAPICrash3")
	cmd.Env = append(os.Environ(), "CRASH_RUN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		if pmem.Delete("region7") == nil { // region 7 should not be created
			assert(t)
		}
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestAPICrash4(t *testing.T) {
	fmt.Println("Testing API crash for New() call with already existing " +
		"object name")
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("New() panicked successfully with msg:", r)
			pmem.Delete("region8")
		}
	}()
	var a *int
	a = (*int)(pmem.New("region8", a))
	var b *int
	b = (*int)(pmem.New("region8", b))
}

func TestAPICrash5(t *testing.T) {
	fmt.Println("Testing API crash for Make() call with already existing " +
		"object name")
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Make() panicked successfully with msg:", r)
			pmem.Delete("region9")
		}
	}()
	var s1 []int
	s1 = pmem.Make("region9", s1, 10).([]int)
	var s2 []int
	s2 = pmem.Make("region9", s2, 10).([]int)
}

func assert(t *testing.T) {
	assertEqual(t, 0, 1)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		debug.PrintStack()
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
