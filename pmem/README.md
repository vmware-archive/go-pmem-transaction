This document explains the functions and concepts of `pmem` package which 
provides a way for Go applications to access persistent memory. It 
has been written to work in conjunction with the Go trasaction 
[package](https://github.com/vmware/go-pmem-transaction/tree/master/transaction)
and changes made to Go compiler to support persistent memory in a separate 
[project](https://github.com/jerrinsg/go-pmem).

Any data in volatile memory is lost on restart, so volatile pointers pointing to
data in non-volatile memory (V-to-NV pointers) will be lost too. This leaves 
NV-to-NV pointers as the only way for applications to access pmem across 
restarts. We allow the applications to retrieve some of these NV-to-NV pointers 
through string names. These can then be used to navigate other objects stored in
pmem. We call these objects “named objects” and they can be pointers/Go slices.

The following functions are accessible to the users of this package:

1. `Init(fileName string) error`
Expects a filename as its only argument. This is the name of the persistent 
memory file that the application will be using. This function takes care of 
detecting if the application crashed in the past, and if there are any 
incomplete updates stored in the transaction logs. Based on whether these 
updates were committed or not, the updates are applied/dropped respectively. 
This function internally calls `transaction.Init()` of transaction 
[package](https://github.com/vmware/go-pmem-transaction/tree/master/transaction).
It returns an error to indicate if persistent memory initialization was
successful or not.
Example:
```go
err := pmem.Init("myTestFile")
if err != nil {
    // Persistent memory initialization failed
}
```

2. `New(name string, intf interface{}) unsafe.Pointer`
This function is similar to using Go's new() function. It allocates space for a 
data structure and returns pointer to it. The data structure is passed as the 
2nd argument, and the pointer is returned as an unsafe pointer to allow any user
data structure to be created. In addition, it provides a way to create a name to
NV pointer mapping. Using this name, the application can retreive a NV-pointer 
in subsequent restarts.
If the named object already exists in persistent memory, this function returns
the pointer to that object, and does not create a new object. But if the named
object is of a different type than the second argument, this function crashes.
```go
var a *int
a = (*int)(pmem.New("region1", a))
*a = 10 //Space for a allocated inside New()
var st1 *myStruct
st1 = (*myStruct)(pmem.New("myObject", st1))
var st2 *myStruct
st2 = (*myStruct)(pmem.New("myObject", st2)) // retrieves the same struct as st1
```
If the 2nd argument is a slice, the function crashes.


3. `Make(name string, intf ...interface{}) interface{}`
Similar to New(), except this allows applications to create slices with names, 
or a name-to-nonvolatile slice mapping. This function is similar to Go's make().
It crashes if the 2nd argument is not a slice. The length of slice is passed as
the 3rd argument. Similar to New(), if the named slice already exists, this
function returns that slice, and does not create a new slice. But if the named
object is of a different type than the second argument, this function crashes.
Example:
```go
var sl1 []float64
sl1 = pmem.Make("region2", sl1, 10).([]float64) // len(sl1) = 10
sl1[0] = 1.1
var sl2 []float64
sl2 = pmem.Make("region2", sl2, 10).([]float64) // sl2 is the same as sl1
```

4. `Delete(name string) error`
Allows applications to delete a named object. The name can be reused to create
a new data structure or slice. References to the existing object are removed
internally, so the non-volatile data structure/slice can be garbage collected.
Returns an error if no object with the name existed before.