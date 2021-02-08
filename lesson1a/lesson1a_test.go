package nachdb

import (
	"fmt"
	"testing"
)

func doTest(test *testing.T, things []Action) {
	rs := NewRunState()
	for idx, do := range things {
		if err := do(rs); err != nil {
			test.Error(NewStackError("Line: %v Action: %+v Err: %v", idx+1, do, err))
		}
	}
}

func TestInsert(test *testing.T) {
	doTest(test, []Action{
		Begin("Alice"),
		Insert("Alice", "A", 1),
		Begin("Bob"),
		Insert("Bob", "B", 2),
		Commit("Alice"),
		Commit("Bob"),
	})
}

func TestWriteConflict(test *testing.T) {
	doTest(test, []Action{
		Begin("Alice"),
		Insert("Alice", "A", 1),
		Begin("Bob"),
		ExpectError(Insert("Bob", "A", 2), WRITE_CONFLICT),
	})
}

func TestMultiTimestampNormal(test *testing.T) {
	doTest(test, []Action{
		Begin("Alice"),
		Timestamp("Alice", 10),
		Insert("Alice", "A", 10),
		Timestamp("Alice", 20),
		Insert("Alice", "B", 20),
		Timestamp("Alice", 30),
		Insert("Alice", "B", 30),
		Commit("Alice"),
		AssertReadAt(TS(10), "A", 10),
		AssertReadAt(TS(20), "B", 20),
		AssertReadAt(TS(30), "B", 30),
		AssertNilReadAt(TS(5), "A"),
	})
}

func TestDoubleRawInsert(test *testing.T) {
	doTest(test, []Action{
		Begin("Alice"),
		Insert("Alice", "A", 10),
		Timestamp("Alice", 10),
		Insert("Alice", "Oplog_A", 10),
		Insert("Alice", "B", 20),
		Timestamp("Alice", 20),
		Insert("Alice", "Oplog_B", 20),
		Commit("Alice"),
		AssertNilReadAt(TS(10), "A"),
		AssertReadAt(TS(20), "A", 10),
		AssertReadAt(TS(10), "Oplog_A", 10),
		AssertReadAt(TS(10), "B", 20),
		AssertReadAt(TS(20), "Oplog_B", 20),
	})
}

func TestSelfWriteConflict(test *testing.T) {
	doTest(test, []Action{
		Begin("Alice"),
		Timestamp("Alice", 20),
		Insert("Alice", "A", 20),
		Commit("Alice"),
		BeginWithReadTs("Alice", 10),
		ExpectError(Insert("Alice", "A", 30), WRITE_CONFLICT),
	})
}

func TestReadYourOwnWrite(test *testing.T) {
	var db *Database = NewDatabase()
	var alice *Session = db.NewSession()

	// Ignore error handling.
	_ = alice.BeginTxn()
	alice.Write("Key", 1)
	if val, err := alice.Read("Key"); err == NOT_FOUND || val != 1 {
		panic(fmt.Sprintf("Failed reading own write. Val: %v", val))
	}

	// Ignore error handling.
	_ = alice.Commit()
}
