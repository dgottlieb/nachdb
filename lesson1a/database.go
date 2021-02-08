package nachdb

import (
	"errors"
	"fmt"
)

var NOT_FOUND error = errors.New("Key not found.")
var WRITE_CONFLICT error = errors.New("Write conflict.")

func NewDatabase() *Database {
	return &Database{
		GlobalTxn: GlobalTxn{
			NextTxnId: 1,
			Sessions:  make([]*Session, 0),
		},
		Data: make(map[string]*UpdateChain),
	}
}

func (database *Database) NewSession() *Session {
	database.Lock()
	defer database.Unlock()
	ret := &Session{
		Id:    database.NextSessionId,
		InTxn: false,
		Txn: Txn{
			Id:             0,
			SnapMin:        0,
			SnapMax:        0,
			ConcurrentSnap: make([]uint64, 0),
			Mods:           make([]*Mod, 0),
		},

		Database: database,
	}

	database.NextSessionId++
	database.GlobalTxn.Sessions = append(database.GlobalTxn.Sessions, ret)

	return ret
}

func (database *Database) GetUpdateChain(key string) *UpdateChain {
	database.Lock()
	defer database.Unlock()

	updateChain, exists := database.Data[key]
	if !exists {
		updateChain = NewUpdateChain(key)
		database.Data[key] = updateChain
	}

	return updateChain
}

func (database *Database) DumpKey(key string) string {
	database.Lock()
	defer database.Unlock()

	updateChain, exists := database.Data[key]
	if !exists {
		return fmt.Sprintf("Key: %v DNE", key)
	}

	return updateChain.Dump()
}
