package nachdb

import (
	"errors"
	"fmt"
)

type RunState struct {
	Database *Database
	Names    map[string]*Session
}

type Action func(runState *RunState) error

type TS uint64

func NewRunState() *RunState {
	return &RunState{
		Database: NewDatabase(),
		Names:    make(map[string]*Session),
	}
}

func (rs *RunState) GetSession(name string) *Session {
	if session, exists := rs.Names[name]; exists {
		return session
	}

	session := rs.Database.NewSession()
	rs.Names[name] = session
	return session
}

func Error(do Action) Action {
	return func(rs *RunState) error {
		err := do(rs)
		if err == nil {
			return errors.New("Should have received an error.")
		}
		return nil
	}
}

func ExpectError(do Action, expectedErr error) Action {
	return func(rs *RunState) error {
		err := do(rs)
		if err != expectedErr {
			return fmt.Errorf("Didn't receive expected error. Expected: %v Received: %v", expectedErr, err)
		}

		return nil
	}
}

func Begin(name string) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.BeginTxn(); err != nil {
			return err
		}

		return nil
	}
}

func BeginWithReadTs(name string, ts TS) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.BeginTxnWithReadTs(uint64(ts)); err != nil {
			return err
		}

		return nil
	}
}

func Insert(name string, key string, value int) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.Write(key, value); err != nil {
			return err
		}

		return nil
	}
}

func Commit(name string) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.Commit(); err != nil {
			return err
		}

		return nil
	}
}

func Rollback(name string) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.Rollback(); err != nil {
			return err
		}

		return nil
	}
}

func Timestamp(name string, ts uint64) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.TimestampTransaction(ts); err != nil {
			return err
		}

		return nil
	}
}

func AssertReadAt(ts TS, key string, expectedValue int) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession("one_time_reader")
		if err := sess.BeginTxnWithReadTs(uint64(ts)); err != nil {
			return err
		}
		defer sess.Rollback()

		val, err := sess.Read(key)
		if err != nil {
			return err
		}

		if val != expectedValue {
			return NewStackError("Expected: %v Read: %v at TS: %v", expectedValue, val, ts)
		}

		return nil
	}
}

func AssertNilReadAt(ts TS, key string) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession("one_time_reader")
		if err := sess.BeginTxnWithReadTs(uint64(ts)); err != nil {
			return err
		}
		defer sess.Rollback()

		val, err := sess.Read(key)
		if err == NOT_FOUND {
			return nil
		}

		return NewStackError("Expected: NOT_FOUND Read: %v at TS: %v\nChain: %v", val, ts, rs.Database.DumpKey(key))
	}
}
