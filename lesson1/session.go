package nachdb

import (
	"errors"
	"fmt"
	"sync"
)

type Txn struct {
	Id             uint64
	SnapMin        uint64
	SnapMax        uint64
	ConcurrentSnap []uint64
	Mods           []*Mod
}

type Session struct {
	Id uint64

	InTxn bool
	Txn

	Database *Database
	sync.Mutex
}

func (session *Session) Write(key string, value int) error {
	if !session.InTxn {
		return errors.New("Cannot write. Not in a transaction.")
	}

	updateChain := session.Database.GetUpdateChain(key)

	updateChain.Lock()
	defer updateChain.Unlock()

	if updateChain.Head != nil {
		if !session.IsVisible(updateChain.Head.TxnId) {
			return fmt.Errorf("WriteConflict. Key: %v", key)
		}
	}

	var newMod *Mod
	var verb Verb
	if updateChain.Head == nil || updateChain.Head.Verb == DeleteMod {
		verb = InsertMod
	} else {
		verb = UpdateMod
	}

	newMod = &Mod{session.Txn.Id, value, verb, nil, nil}

	updateChain.Add(newMod)
	return nil
}

func (session *Session) Read(key string) (int, error) {
	if !session.InTxn {
		return 0, errors.New("Cannot read. Not in a transaction.")
	}

	updateChain := session.Database.GetUpdateChain(key)
	updateChain.Lock()
	defer updateChain.Unlock()

	mod := updateChain.Head
	for {
		if mod == nil {
			return 0, NOT_FOUND
		}

		if session.IsVisible(mod.TxnId) {
			return mod.Value, nil
		}

		mod = mod.Next
	}

	panic("Unreachable.")
}

func (session *Session) Rollback() error {
	session.Lock()
	defer session.Unlock()

	if !session.InTxn {
		return errors.New("Cannot rollback. Not in a transaction.")
	}

	session.InTxn = false
	for _, mod := range session.Txn.Mods {
		mod.TxnId = 0
	}

	return nil
}

func (session *Session) Commit() error {
	session.Lock()
	defer session.Unlock()

	if !session.InTxn {
		return errors.New("Cannot commit. Not in a transaction.")
	}

	session.InTxn = false
	return nil
}
