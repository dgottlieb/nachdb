package nachdb

import "testing"

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
		Error(Insert("Bob", "A", 2)),
	})
}
