/* With undo logging, application should look like this:
 *
 *	transaction.Init() -- Should be done once in app's lifetime during app start
 *
 *	tx := NewUndoTx()
 *	tx.Begin()
 *
 *	tx.Log(&S)
 *  S := 10
 *  tx.Log(&P)
 *  P := "hello"
 *
 *	tx.End()
 *	transaction.Release(tx)
 */

package main

import (
	"fmt"
	"go-pmem-transaction/transaction"
	"runtime"
	"time"
	"unsafe"
)

// Doubly linked list. Only supports pushFront & popFront for now.
// In each of these updates, head will be changed.
type (
	listNode struct {
		prev *listNode
		next *listNode
		val  int
	}
)

var head, tail *listNode // head & tail of the linked list

func createListHead(val int) *listNode {
	fmt.Println("[dl_list] creating head ", val)
	tmpNode := pnew(listNode)
	tmpNode.val = 100
	runtime.PersistRange(unsafe.Pointer(tmpNode), uintptr(unsafe.Sizeof(*tmpNode)))
	return tmpNode
}

func pushFront(val int) { // add new node to head & update head
	fmt.Println("[dl_list] pushFront: ", val)
	tmp := head
	node := pnew(listNode)

	tx := transaction.NewUndoTx()
	tx.Begin()
	tx.Log(&node.val)
	node.val = val

	tx.Log(&head)
	head = node // Bad design, updating head early followed by a sleep
	// so we can fail & see if transaction library works

	updateAppHeaderListHead(tx, head)
	fmt.Println("[dl_list] FAIL HERE to test crash consistency")
	timeToSleep() // <-- Test point. If fail here, we lose all data without
	// crash consistency : Forward traversal would only see node created above,
	// while backward traversal will not see this node.

	tx.Log(&node.next)
	node.next = tmp
	tx.Log(&tmp.prev)
	tmp.prev = node

	tx.End()
	updateAppHeaderListHead(tx, head)
	transaction.Release(tx)
	timeToSleep()
}

func popFront() { // remove node from head & update head
	if head == nil {
		fmt.Println("[dl_list] popFront, List Empty")
	}
	fmt.Println("[dl_list] popFront, val: ", head.val)

	tx := transaction.NewUndoTx()
	tx.Begin()

	tx.Log(&head)
	head = head.next
	updateAppHeaderListHead(tx, head)
	fmt.Println("[dl_list] FAIL HERE to test crash consistency")
	timeToSleep() // If fail here, old head would still be pointed to by prev
	// pointer of new head. GC should not delete this node. Backward traversal
	// would still see this node, but forward traversal would not.

	tx.Log(&head.prev)
	head.prev = nil
	updateAppHeaderListHead(tx, head)

	tx.End()
	transaction.Release(tx)
	timeToSleep()
}

func printForward() {
	fmt.Println("[dl_list] printForward: ")
	for node := head; node != nil; node = node.next {
		fmt.Println("[dl_list] curr val: ", node.val)
	}
}

func printBackward() {
	fmt.Println("[dl_list] printBackward: ")
	for node := tail; node != nil; node = node.prev {
		fmt.Println("[dl_list] curr val: ", node.val)
	}
}

func timeToSleep() {
	fmt.Println("Sleep for 2 seconds")
	time.Sleep(2 * time.Second)
}

func startDlListOps() {

	// get head & tail
	head = listHead
	tail = listTail

	fmt.Println("[dl_list] printForward the list before any changes:")
	printForward()

	pushFront(200)
	pushFront(300)
	pushFront(400)
	pushFront(500)

	// popFront()
	printForward()

	printBackward()
}
