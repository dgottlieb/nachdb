package nachdb

import (
	"errors"
	"fmt"
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

	FirstModTimestamp uint64
	ModTimestamp      uint64
	ReadTimestamp     uint64
}

func (session *Session) TimestampTransaction(ts uint64) error {
	if session.Txn.FirstModTimestamp == 0 {
		session.Txn.FirstModTimestamp = ts
	}

	if ts < session.Txn.FirstModTimestamp {
		return fmt.Errorf(
			"Cannot set the a write timestamp earlier than the transaction's first write timestamp. Was: %v Input: %v",
			session.Txn.FirstModTimestamp,
			ts,
		)
	}

	session.Txn.ModTimestamp = ts
	return nil
}

// Begin a transaction in snapshot isolation.
func (session *Session) BeginTxnWithReadTs(ts uint64) error {
	if err := session.BeginTxn(); err != nil {
		return err
	}

	if ts == 0 {
		return errors.New("Timestamps must not be zero.")
	}

	session.Txn.ReadTimestamp = ts

	return nil
}

// Given a `txnId` on a document's version, return whether this version is visible to the current
// transaction. The implementation may assume the session is already in a transaction
// (`session.InTxn == true`).
func (session *Session) IsVisible(txnId uint64, ts uint64) bool {
	session.Lock()
	defer session.Unlock()

	if session.Txn.Id == txnId {
		return true
	}

	if session.Txn.ReadTimestamp > 0 && session.Txn.ReadTimestamp < ts {
		return false
	}

	switch {
	case txnId <= session.Txn.SnapMin:
		return true
	case txnId >= session.Txn.SnapMax:
		return false
	}

	for _, concurrentId := range session.Txn.ConcurrentSnap {
		if txnId == concurrentId {
			return false
		}
	}

	return true
}
func (session *Session) Rollback() error {
	session.Lock()
	defer session.Unlock()

	if !session.InTxn {
		return errors.New("Cannot rollback. Not in a transaction.")
	}

	for _, mod := range session.Txn.Mods {
		mod.TxnId = ROLLED_BACK
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

	if session.Txn.ModTimestamp > 0 {
		for _, mod := range session.Txn.Mods {
			if mod.Ts == 0 {
				mod.Ts = session.Txn.ModTimestamp
			} else {
				break
			}
		}
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
	txn.ModTimestamp = 0
	txn.ReadTimestamp = 0
}
