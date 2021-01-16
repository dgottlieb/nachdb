package nachdb

import "errors"

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

func (session *Session) IsVisible(txnId uint64) bool {
	session.Lock()
	defer session.Unlock()

	switch {
	case session.Txn.Id == txnId:
		return true
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
