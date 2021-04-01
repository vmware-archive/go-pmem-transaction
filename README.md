

# go-pmem-transaction

## Overview
A library to make persistent memory accessible to developers of Go language. More details about Persistent memory can be found [here](https://docs.pmem.io/). This repostiory has two Go packages (pmem & transaction).

### Prerequisites
To use these packages, you need new extensions to the Go language. These changes are maintained in a separate repository [here](
https://github.com/jerrinsg/go-pmem).

### Build & Run

1. Download Go source code by cloning the [modified Go source code](https://github.com/jerrinsg/go-pmem).
2. Build the Go distribution by running (For linux):
```
$ cd src
$ ./all.bash
```
You can also follow the general instructions for building Go from its source code found [here](https://golang.org/doc/install/source#install).

3. `go get -u github.com/vmware/go-pmem-transaction/...`
4. Make sure to build these packages (and applications using these packages) using the Go binary built in step 2.

## Documentation
This repository provides two packages: 
1. *pmem* package that provides access to persistent memory. The pmem package allows users to create data structures that reside in persistent memory and get pointers to these data structures that reside in persistent memory as well.

2. The *transaction* package provides undo and redo transaction logging APIs to allow for crash-consistent updates. 

Individual READMEs for these packages can be found in the package directories or here:
1. [pmem README](https://github.com/vmware/go-pmem-transaction/blob/master/pmem/README.md)
2. [transaction README](https://github.com/vmware/go-pmem-transaction/blob/master/transaction/README.md)

## Contributing

The go-pmem-transaction project team welcomes contributions from the community. Before you start working with go-pmem-transaction, please read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on as an open-source patch. For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License
go-pmem-transaction is availabe under BSD-3 license.
