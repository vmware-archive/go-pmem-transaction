package transaction

import (
	"errors"
	"log"
	"sync"
)

type (
	readonlyTx struct {
		level   int
		fakelog bool
		rlocks  []*sync.RWMutex
	}
)

func NewReadonly() TX {
	t := new(readonlyTx)
	t.fakelog = false
	t.rlocks = make([]*sync.RWMutex, 0, 3)
	return t
}

func releaseReadonly(t *readonlyTx) {
	t.Abort()
}

func (t *readonlyTx) Log(data interface{}) error {
	if t.fakelog {
		// may call Log function from operations to tmporary objects in pmem, just ignore them.
		return nil
	}
	log.Fatal("tx.readonly: cannot log in readonly transaction!")
	return errors.New("tx.readonly: cannot log in readonly transaction!")
}

func (t *readonlyTx) FakeLog(interface{}) {
	// allow logging api
	t.fakelog = true
}

func (t *readonlyTx) Begin() error {
	t.level += 1
	return nil
}

func (t *readonlyTx) Commit() error {
	if t.level == 0 {
		log.Fatal("tx.readonly: no transaction to commit!")
	}
	t.level--
	if t.level == 0 {
		t.unLock()
	}
	return nil
}

func (t *readonlyTx) Abort() error {
	t.unLock()
	t.level = 0
	return nil
}

func (t *readonlyTx) RLock(m *sync.RWMutex) {
	m.RLock()
	t.rlocks = append(t.rlocks, m)
}

func (t *readonlyTx) WLock(m *sync.RWMutex) {
	log.Fatal("tx.readonly: cannot hold write lock in readonly transaction!")
}

func (t *readonlyTx) Lock(m *sync.RWMutex) {
	t.RLock(m)
}

func (t *readonlyTx) unLock() {
	for _, m := range t.rlocks {
		//log.Println("Log ", t.id, " runlocking ", m)
		m.RUnlock()
	}
	t.rlocks = t.rlocks[0:0]
}
