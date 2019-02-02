This document explains the usage and functionality of transaction package.

The package implements undo logging and redo logging. So, we currently support 
undo and redo transactions. Any other kind of transaction can be implemented 
given it satisfies the `TX` interface. The package has been implemented to be 
used in conjunction with the persistent memory (pmem) changes being made to Go 
runtime in a separate [project](https://gitlab.eng.vmware.com/afg/go-pmem).

The transaction package is initialized through function
`Init(logHeadPtr unsafe.Pointer, logType string)` . If the first argument is
not nil, application already has some data stored in the transaction logs in one
of its previous runs. In this case, recovery is performed to restore consistent
state of stored application data. The 2nd argument specifies whether the user
wants a "undo" log or "redo" log. 
Example usage: `transaction.Init(nil, "undo")`
This function is internally called by `Init()` function of `pmem` 
[package](https://gitlab.eng.vmware.com/afg/go-pmem-transaction/tree/master/pmem)
and need not be called by applications explicitly.

The transaction variables can be initialized using package functions
`transaction.NewUndoTx()` or `transaction.NewRedoTx()`

The `TX` interface requires the following methods to be implemented:

1. `Begin() error`
This marks the beginning of a new transaction. Nested transactions are supported
and can be started by calling `Begin()` before the outer transaction completes.

2. `End() error`
This marks the end of the ongoing transaction and is equivalent to committing a 
transaction. On the end of a transaction, all the changes known to the log are 
persisted to pmem. Note that updates to inner transactions are not persisted if 
there is a crash before the outer transaction is complete. For undo transaction,
calling `End()` makes sure all updates to variables logged using `Log()` are 
persisted to pmem. If a sliceheader was logged, any changes to the sliceheader 
and any changes to the elements of slice are persisted to pmem as well. All the 
data stored in the undo log is discarded after everything has been persisted by 
setting the `tail` of log to zero. If there was a crash when `End()` was being 
executed, a subsequent application restart would call `abort()` which reverts 
all the updates made.

For redo transactions, calling `End()` causes all updates in the redo log to be
persisted to pmem. Once this is done, the transaction is marked committed. All
the updates in the redo log are now transferred to the program variables. If 
there is a crash, a subsequent application restart checks the value of 
`committed` variable. If marked true, all the updates in the redo log are 
transferred to the program variables. Since we support value-based logging, this
operation is idempotent and works across multiple crashes. If `committed` is 
false, all updates in redo log are dropped by calling `abort()`

3. `Log(...interface{}) error`
For undo transactions, the expected syntax is `Log(ptr)` or `Log(slice)`. If 
`Log()` is called with more than one argument for undo transactions, we return 
an error immediately. Undo log captures the state of the variable before the 
update and allows the update in-place. So, to successfully log a data structure 
in undo log, we need the address and the size of the data structure. The size of
the data structure is obtained from Go’s type system inside this function. So, 
only one argument needs to be passed. Typical usage for undo tx looks like:
```go
tx.Begin()
tx.Log(&node)
node.next = newNode
node.val = newVal
tx.Log(&myStruct.i)
myStruct.i = 2
tx.Log(mySlice)
mySlice[2] = 100
tx.End()
```

For redo transactions, the expected syntax is `Log(ptr, new value)` or 
`Log (old slice, new slice)`. If `Log()` is called with more/less than two 
arguments for redo transactions, or if there is a mismtach in the type of the 
old and new value, an error is returned immediately. Redo log creates a new
copy of the variable on the first update and flushes changes to program data
structures only on transaction commit. So, to log a data structure in redo log, 
the new value needs to be provided too. Typical usage for redo tx looks like:

```go
tx.Begin()
tx.Log(&node.next, newNode)
tx.Log(&node.val, newVal)
tx.Log(&myStruct.i, 2)
newSlice[2] = 100
tx.Log(mySlice, newSlice)
tx.End()
```

4. `ReadLog(interface{}) interface{}`
Updates in a redo transaction are not made in-place. As such, any Log() call in 
a redo transaction creates a new copy of the variable. If the application wants 
to read the latest update made within the transaction, reading the variable 
gives old data. `ReadLog()` method does this. It expects the pointer to the 
variable whose latest value is needed. If the pointer was never logged before
and not stored in the redo log before, the latest value at the memory location
is returned.

All the updates to variables in an undo logging mechanism are made in-place.
So, the latest updates can be read by directly reading the variable.
So, this method is not supported for undo transactions. Currently, we return an
empty interface if this method is called with undo transactions.

5. `RLock(*sync.RWMutex)`/ `WLock(*sync.RWMutex)` / `Lock(*sync.RWMutex)`
Updates within a transaction should not be visible outside until the transaction
is committed. This is the Isolation property of transactions. In our case, users
have two options: 
a) Acquire all the locks before the transaction begins, and release all the 
locks after the transaction is over. Thus a typical usage pattern would be: 
```go
m1 := new(sync.RWMutex)
m2 := new(sync.RWMutex)
m1.Lock()
m2.Lock()
tx.Begin()
// Application code
// may include a function call
// This should not include acquiring locks and then updating data structures
// through transaction since the update would be visible before tx is committed.
// If there is a crash here, tx would revert this update but it 
// has already been seen, causing inconsistency.
tx.End()
m1.Unlock()
m2.Unlock()
```
This approach is similar to strict 2-Phase Locking of database transactions.

b) Acquire locks through `RLock()`/`WLock()`/`Lock()` provided by transactions.
Calling `WLock` or `Lock` acquires the write lock on the RWMutex, wherease 
`RLock` acquires the read lock. All these calls also store the address of the 
mutex. All the locks acquired within a transaction are released when the 
transaction completes successfully or if it is aborted. Using this provides the 
application the flexibility to acquire locks just before data strcutures are 
accessed. A typical usage pattern would be:
```go
m1 := new(sync.RWMutex)
m2 := new(sync.RWMutex)
tx.Begin()
tx.Lock(m1)
tx.Lock(m2)
// Application code
tx.End()
```
This approach is similar to 2-Phase Locking of database transactions.

6. `abort() error`
This method is not accessible to users of the package, but as the name suggests
it would abort an ongoing transaction, reverting all the updates if the 
transaction has not been committed. If the transaction is already marked 
committed (This path is taken only for redo transactions), all updates are 
retried. To trigger `abort()`, users can instead use the package function 
`transaction.Release(tx)`

7. `Exec(...interface{}) ([]reflect.Value, error)`
This method allows users to call functions that would be executed within a 
transaction, and can be particularly useful for closures. Typical use would be:
```go
func add(tx transaction.TX, a *int, b *int) {
	tx.Log(a)
	a = a + b
    return
}
// code
// code
retVal, err = tx.Exec(add, a, b)
fmt.Println((int)(retVal[0].Int()))
```

OR

```go
// code
_, err = tx.Exec(func(tx transaction.TX) {
	tx.Log(&a)
	a = a + b // a & b are variables outside this closure
})
```

More usage of transactions can be seen in the **tests/** directory.