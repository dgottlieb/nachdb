package nachdb

import (
	"errors"
)

func (session *Session) BeginTxn() error {
	session.Lock()
	defer session.Unlock()

	if session.InTxn {
		return errors.New("Already in a transaction.")
	}

	session.InTxn = true

	globalTxn := &session.Database.GlobalTxn
	globalTxn.Lock()
	defer globalTxn.Unlock()

	session.Txn.SnapMax = globalTxn.NextTxnId
	session.Txn.Id = globalTxn.NextTxnId
	globalTxn.NextTxnId++

	snapMin := globalTxn.NextTxnId
	for _, otherSession := range globalTxn.Sessions {
		if session == otherSession {
			continue
		}

		otherSession.Lock()
		if otherSession.InTxn {
			session.Txn.ConcurrentSnap = append(session.Txn.ConcurrentSnap, otherSession.Txn.Id)
			if otherSession.Txn.Id < snapMin {
				snapMin = otherSession.Txn.Id
			}
		}
		otherSession.Unlock()
	}

	session.Txn.SnapMin = snapMin - 1
	return nil
}

func (session *Session) Write(key string, value int) error {
	if !session.InTxn {
		return errors.New("Cannot write. Not in a transaction.")
	}

	updateChain := session.Database.GetUpdateChain(key)

	updateChain.Lock()
	defer updateChain.Unlock()

	for mod := updateChain.Head; mod != nil; mod = mod.Next {
		if mod.TxnId == ROLLED_BACK {
			// Pass over rolled back updates.
			continue
		}

		if !session.IsVisible(mod.TxnId, mod.Ts) {
			return WRITE_CONFLICT
		} else {
			break
		}
	}

	var newMod *Mod
	var verb Verb
	if updateChain.Head == nil || updateChain.Head.Verb == DeleteMod {
		verb = InsertMod
	} else {
		verb = UpdateMod
	}

	newMod = &Mod{session.Txn.Id, session.Txn.ModTimestamp, value, verb, nil, nil}
	session.Txn.Mods = append(session.Txn.Mods, newMod)

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

	for mod := updateChain.Head; mod != nil; mod = mod.Next {
		if session.IsVisible(mod.TxnId, mod.Ts) {
			return mod.Value, nil
		}
	}

	return 0, NOT_FOUND
}
