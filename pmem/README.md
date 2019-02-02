This document explains the functions and concepts of `pmem` package which 
provides a way for Go applications to access persistent memory. It 
has been written to work in conjunction with the Go trasaction 
[package](https://gitlab.eng.vmware.com/afg/go-pmem-transaction/tree/master/transaction)
and changes made to Go compiler to support persistent memory in a separate 
[project](https://gitlab.eng.vmware.com/afg/go-pmem). 

Any data in volatile memory is lost on restart, so volatile pointers pointing to
data in non-volatile memory (V-to-NV pointers) will be lost too. This leaves 
NV-to-NV pointers as the only way for applications to access pmem across 
restarts. We allow the applications to retrieve some of these NV-to-NV pointers 
through string names. These can then be used to navigate other objects stored in
pmem. We call these objects “named objects” and they can be pointers/Go slices.

The following functions are accessible to the users of this package:

1. `Init(fileName string) bool`
Expects a filename as its only argument. This is the name of the persistent 
memory file that the application will be using. This function takes care of 
detecting if the application crashed in the past, and if there are any 
incomplete updates stored in the transaction logs. Based on whether these 
updates were committed or not, the updates are applied/dropped respectively. 
This function internally calls `transaction.Init()` of transaction 
[package](https://gitlab.eng.vmware.com/afg/go-pmem-transaction/tree/master/transaction)
Example:
```go
if pmem.Init("myTestFile") {
    // true meaning first time application run
} else {
    // myTestFile already exists, and this is application restart
}
```

2. `New(name string, intf interface{}) unsafe.Pointer`
This function is similar to using Go's new() function. It allocates space for a 
data structure and returns pointer to it. The data structure is passed as the 
2nd argument, and the pointer is returned as an unsafe pointer to allow any user
data structure to be created. In addition, it provides a way to create a name to
NV pointer mapping. Using this name, the application can retreive a NV-pointer 
in subsequent restarts.
If the application tries to create an object with a name that already exists,
this function panics. Example use:
```go
var a *int
a = (*int)(pmem.New("region1", a))
*a = 10 //Space for a allocated inside New()
var st1 *myStruct
st1 = (*myStruct)(pmem.New("myObject", st1))
```
If the 2nd argument is a slice, the function crashes.

3. `func Get(name string, intf interface{}) unsafe.Pointer`
Data structures created using `New()` can be retrieved using this function. If
the returned value is nil, no data structure with the given name exists. If a 
data structure with the given name exists, but is of different type than the
2nd argument, the function crashes. If the 2nd argument is a slice, the function
crashes. Example use:
```go
var b *int
b = (*int)(pmem.Get("region1", b)) // region1 created before in example of New() 
if b == nil {
    // region1 doesn't exist
} else {
    // region1 exists, b = 10. Continuing from example of New()
}
```

4. `Make(name string, intf ...interface{}) interface{}`
Similar to New(), except this allows applications to create slices with names, 
or a name-to-nonvolatile slice mapping. This function is similar to Go's make().
It crashes if the 2nd argument is not a slice. The length of slice is passed as
the 3rd argument. Similar to New(), this function panics if the application 
tries to create an object with a name that already exists. Example:
```go
var slice1 []float64
slice1 = pmem.Make("region2", slice1, 10).([]float64) // len(slice1) = 10
slice1[0] = 1.1
```

5. `GetSlice(name string, intf ...interface{}) interface{}`
Similar to `Get()`, except this allows applications to retrieve slices created
using `Make()`. This function returns a nil interface if no slice with the given
name exists. If a slice with the given name exists, but is of different type 
than the 2nd argument, the function crashes. If the 2nd argument is not a slice,
the function crashes. Example use:
```go
var slice2 []float64
slice2 = pmem.GetSlice("region2", slice2).([]float64) // Get same slice
// slice2[0] = 1.1 ... continuing from example for Make()
```

6. `Delete(name string) error`
Allows applications to delete a named object. The name can be reused to create
a new data structure or slice. References to the existing object are removed
internally, so the non-volatile data structure/slice can be garbage collected.
Returns an error if no object with the name existed before.