// A simple linked list application that shows the usage of the pmem and
// transaction package. On the first invocation, it creates a named object
// called dbRoot which holds pointers to the first and last element in the
// linked list. On each run, a new node is added to the linked list and all
// contents of the list are printed.

package main

import (
	"flag"
	"math/rand"
	"time"

	"github.com/vmware/go-pmem-transaction/pmem"
	"github.com/vmware/go-pmem-transaction/transaction"
)

const (
	// A magic number used to identify if the root object initialization
	// completed successfully.
	magic = 0x1B2E8BFF7BFBD154
)

// Structure of each node in the linked list
type entry struct {
	id   int
	data []byte
	next *entry
}

// The root object that stores pointers to the elements in the linked list
type root struct {
	magic int
	head  *entry
	tail  *entry
}

// Function to generate a random byte slice in persistent memory of length n
func randString(n int) []byte {
	b := pmake([]byte, n) // transaction here

	txn("undo") {
	for i := range b {
		b[i] = byte(rand.Intn(26) + 65)
	}
	}

	return b
}

// A function that populates the contents of the root object transactionally
func populateRoot(rptr *root) {
	txn("undo") {
	rptr.magic = magic
	rptr.head = nil
	rptr.tail = nil
	}
}

// Adds a node to the linked list and updates the tail (and head if empty)
// All data updates are handled transactionally
func addNode(rptr *root) {
	entry := pnew(entry)

	txn("undo") {
	entry.id = rand.Intn(100)
	entry.data = randString(10)
	if rptr.head == nil {
		rptr.head = entry
	} else {
		rptr.tail.next = entry
	}
	rptr.tail = entry
	}
}

// Print all the nodes currently in the linked list
func printNodes(rptr *root) {
	entry := rptr.head
	for entry != nil {
		println("id = ", entry.id, " data = ", string(entry.data))
		entry = entry.next
	}
}

var (
	pmemFile = flag.String("file", "testfile", "pmem file name")
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()
	firstInit := pmem.Init(*pmemFile)
	var rptr *root
	if firstInit {
		// Create a new named object called dbRoot and point it to rptr
		rptr = (*root)(pmem.New("dbRoot", rptr))
		populateRoot(rptr)
	} else {
		// Retrieve the named object dbRoot
		rptr = (*root)(pmem.Get("dbRoot", rptr))
		if rptr.magic != magic {
			// An object named dbRoot exists, but its initialization did not
			// complete previously.
			populateRoot(rptr)
		}
	}
	addNode(rptr)    // Add a new node in the linked list
	printNodes(rptr) // Print out the contents of the linked list
}
