package txTests

import (
	"fmt"
	"go-pmem-transaction/transaction"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

type structLogTest struct {
	i     int
	iptr  *int
	slice []int
}

var (
	j       *int
	b       *bool
	slice1  []int
	slice2  []int
	struct1 *structLogTest
	struct2 *structLogTest
)

func BenchmarkLogInt(b *testing.B) {
	setup()
	j = pnew(int)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(j)
		*j = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkLogSlice(b *testing.B) {
	setup()
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 100)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(struct1.slice)
		struct1.slice[i%100] = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkLogStruct(b *testing.B) {
	setup()
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 100)
	tx := transaction.NewUndoTx()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx.Begin()
		tx.Log(struct1)
		struct1.i = i
		tx.Log(struct1.slice)
		struct1.slice[i%100] = i
		tx.End()
	}
	transaction.Release(tx)
}

func BenchmarkRawInt(b *testing.B) {
	setup()
	j = pnew(int)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		*j = i
		runtime.PersistRange(unsafe.Pointer(j), unsafe.Sizeof(*j))
	}
}

func BenchmarkRawSlice(b *testing.B) {
	setup()
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		struct1.slice[i%100] = i
		runtime.PersistRange(unsafe.Pointer(&struct1.slice[0]),
			(uintptr)(len(struct1.slice)*
				(int)(unsafe.Sizeof(struct1.slice[0]))))
	}
}

func BenchmarkRawStruct(b *testing.B) {
	setup()
	struct1 = pnew(structLogTest)
	struct1.slice = pmake([]int, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		struct1.i = i
		struct1.slice[i%100] = i
		runtime.PersistRange(unsafe.Pointer(struct1), unsafe.Sizeof(*struct1))
		runtime.PersistRange(unsafe.Pointer(&struct1.slice[0]),
			(uintptr)(len(struct1.slice)))
	}
}

func resetData() {
	b = pnew(bool)
	j = pnew(int)
	slice1 = pmake([]int, 100, 100)
	slice2 = pmake([]int, 100, 100)
	struct1 = pnew(structLogTest)
	struct2 = pnew(structLogTest)
	struct1.i = 1
	struct2.i = 2
}

func TestLogBasic(t *testing.T) {
	setup()
	resetData()
	undoTx := transaction.NewUndoTx()
	fmt.Println("Testing basic data type commit.")
	undoTx.Begin()
	undoTx.Log(b)
	undoTx.Log(j)
	*b = true
	*j = 10
	undoTx.End()
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)

	fmt.Println("Testing basic data type abort.")
	undoTx.Begin()
	undoTx.Log(b)
	undoTx.Log(j)
	*b = false
	*j = 0
	transaction.Release(undoTx) // Calls abort internally
	assertEqual(t, *b, true)
	assertEqual(t, *j, 10)

	fmt.Println("Testing data structure commit.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(struct1)
	struct1.i = 10
	struct1.iptr = &struct1.i
	struct1.slice = slice1
	undoTx.Log(struct1.slice)
	slice1[0] = 11
	undoTx.End()
	assertEqual(t, struct1.i, 10)
	assertEqual(t, *struct1.iptr, 10)
	assertEqual(t, struct1.slice[0], slice1[0])
	undoTx.Begin()
	undoTx.Log(struct2)
	struct2.slice = slice2
	undoTx.Log(struct1)
	*struct1 = *struct2
	struct1.iptr = &struct1.i
	undoTx.Log(struct1.slice)
	slice2[0] = 22
	undoTx.End()
	assertEqual(t, struct1.i, struct2.i)
	assertEqual(t, *struct1.iptr, struct2.i)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing data structure abort.")
	undoTx.Begin()
	undoTx.Log(struct1)
	struct1.i = 10
	struct1.iptr = nil
	undoTx.Log(struct1.slice)
	struct1.slice = slice1
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, struct1.iptr, &struct1.i)
	assertEqual(t, struct1.slice[0], slice2[0])
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(struct1.iptr)
	struct1.i = 100
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	struct1.iptr = j
	undoTx.End()
	undoTx.Begin()
	undoTx.Log(struct1)
	*(struct1.iptr) = 1000 // redirect update will not rollback
	undoTx.Log(struct2)
	struct2.i = 3
	struct2.iptr = &struct2.i
	struct2.slice = slice1
	*struct1 = *struct2
	transaction.Release(undoTx)
	assertEqual(t, struct1.i, 2)
	assertEqual(t, *struct1.iptr, 1000)
	assertEqual(t, struct1.slice[0], slice2[0])

	fmt.Println("Testing slice commit.")
	undoTx = transaction.NewUndoTx()
	undoTx.Begin()
	undoTx.Log(slice1)
	slice1[99] = 99
	undoTx.End()
	assertEqual(t, slice1[99], 99)

	fmt.Println("Testing slice abort.")
	undoTx.Begin()
	undoTx.Log(slice1[:10])
	slice2[9] = 9
	slice2[10] = 10 // out of range update will not rollback
	copy(slice1, slice2)
	transaction.Release(undoTx)
	assertEqual(t, slice1[9], 0)
	assertEqual(t, slice1[10], 10)
	assertEqual(t, slice1[99], 0)
}

func crashUndoLog() {
	undoTx := transaction.NewUndoTx()
	undoTx.Begin()
	for i := 0; i < transaction.SENTRYSIZE+1; i++ {
		undoTx.Log(struct1)
	}
	undoTx.End()
	transaction.Release(undoTx)
}

func crashLargeUndoLog() {
	largeUndoTx := transaction.NewLargeUndoTx()
	largeUndoTx.Begin()
	for i := 0; i < transaction.LENTRYSIZE+1; i++ {
		largeUndoTx.Log(struct2)
	}
	largeUndoTx.End()
	transaction.Release(largeUndoTx)
}

func TestLogCrash1(t *testing.T) {
	setup()
	resetData()
	fmt.Println("Testing log crash due to TX being too big for UndoTx")
	if os.Getenv("CRASH_RUN") == "1" {
		crashUndoLog()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLogCrash1")
	cmd.Env = append(os.Environ(), "CRASH_RUN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestLogCrash2(t *testing.T) {
	setup()
	resetData()
	fmt.Println("Testing log crash due to TX being too big for LargeUndoTx")
	if os.Getenv("CRASH_RUN") == "1" {
		crashLargeUndoLog()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLogCrash2")
	cmd.Env = append(os.Environ(), "CRASH_RUN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestConcurrentLog(t *testing.T) {
	setup()
	resetData()
	fmt.Println("Testing concurrent logging")
	m1 := new(sync.RWMutex)
	m2 := new(sync.RWMutex)
	var wg sync.WaitGroup
	fmt.Println("Before:", struct1.i, struct2.i)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			undoTx := transaction.NewUndoTx()
			undoTx.Begin()

			// This is a bad way of taking locks inside transaction.
			// Used this way here for testing
			m1.Lock()
			undoTx.Log(struct1)
			struct1.i = i
			m1.Unlock()
			m2.Lock()
			undoTx.Log(struct2)
			struct2.i = i
			m2.Unlock()
			undoTx.End()

			transaction.Release(undoTx)
		}(i)
	}
	wg.Wait()
	assertEqual(t, struct1.i, struct2.i)
	fmt.Println("After:", struct1.i, struct2.i)	
}
