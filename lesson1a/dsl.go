package nachdb

import (
	"errors"
)

type RunState struct {
	Database *Database
	Names    map[string]*Session
}

type Action func(runState *RunState) error

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

func Begin(name string) Action {
	return func(rs *RunState) error {
		sess := rs.GetSession(name)
		if err := sess.BeginTxn(); err != nil {
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
