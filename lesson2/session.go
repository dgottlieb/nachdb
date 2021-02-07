package nachdb

import (
	"errors"
	"fmt"
)

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
