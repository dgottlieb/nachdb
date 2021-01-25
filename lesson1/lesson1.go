package nachdb

import (
	"errors"
	"sync"
)

type Database struct {
	GlobalTxn GlobalTxn

	// An embedded type. Provides a `Database.Lock` and `Database.Unlock` method.
	sync.Mutex
	NextSessionId uint64
	Data          map[string]*UpdateChain
}

type GlobalTxn struct {
	// An embedded type. Provides a `GlobalTxn.Lock` and `GlobalTxn.Unlock` method.
	sync.Mutex
	NextTxnId uint64
	Sessions  []*Session
}

type Session struct {
	// The Session's ID. It's management is provided by the existing code.
	Id uint64

	// An embedded type. Provides a `Session.Lock` and `Session.Unlock` method.
	sync.Mutex
	// `InTxn` and `Txn` are to be modified inside `BeginTxn`.
	InTxn bool
	Txn   Txn

	Database *Database
}

type Txn struct {
	// The ID of the current transaction.
	Id uint64

	SnapMin        uint64
	SnapMax        uint64
	ConcurrentSnap []uint64
	Mods           []*Mod
}

// Begin a transaction in snapshot isolation.
func (session *Session) BeginTxn() error {
	panic("Unimplemented")
}

// Given a `txnId` on a document's version, return whether this version is visible to the current
// transaction. The implementation may assume the session is already in a transaction
// (`session.InTxn == true`).
func (session *Session) IsVisible(txnId uint64) bool {
	panic("Unimplemented")
}

func (session *Session) Rollback() error {
	session.Lock()
	defer session.Unlock()

	if !session.InTxn {
		return errors.New("Cannot rollback. Not in a transaction.")
	}

	for _, mod := range session.Txn.Mods {
		mod.TxnId = 0
	}

	session.InTxn = false
	session.Txn.Reset()

	return nil
}

func (session *Session) Commit() error {
	session.Lock()
	defer session.Unlock()

	if !session.InTxn {
		return errors.New("Cannot commit. Not in a transaction.")
	}

	session.InTxn = false
	session.Txn.Reset()
	return nil
}

func (txn *Txn) Reset() {
	txn.Id = 0
	txn.SnapMin = 0
	txn.SnapMax = 0
	txn.ConcurrentSnap = make([]uint64, 0)
	txn.Mods = make([]*Mod, 0)
}
