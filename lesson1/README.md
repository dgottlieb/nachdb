# Introduction

The first lesson will demonstrate how time (i.e: Transaction IDs) is managed in WT to implement **snapshot isolation**. This database only maps strings to integers and, for simplicity, has no notion of collections, tables nor indexes. We will start by implementing two functions:

* `Session::BeginTxn() error`
* `Session::IsVisible(txnId uint64) bool`

When those methods are completed, `go test` can be run from within the `lesson1` folder to see if you've successfully implemented snapshot isolation!

The files for this lesson are self-contained in this `lesson1` folder. The files and their contents:

* `lesson1.go` includes the methods to implement as well as some type definitions (for perceived convenience).
* `database.go` has methods on the `Database` type.
* `session.go` has methods on the `Session` type.
* `update_chain.go` has type definitions and methods for the `UpdateChain`.
* The following files are for testing:
 * `dsl.go` defines a more concise language for writing unit tests.
 * `slogger.go` defines a type of Go error that can provide a stack trace.
 * `lesson1_test.go` contains the logical tests for your implementation to pass.

# The Data Types
## Database
The top level structure is a `Database`. It's constructed via `NewDatabase() *Database`. A database serves two main purposes:

* Is a container for all of the key/value pairs (stored as a map of strings to UpdateChains).
* Embeds a `GlobalTxn` type which wraps together time generation (`NextTxnId`) and open `Session`s.

## Session
A session represents a single-threaded client, just like a MongoDB session. A new session can be created `Database::NewSession *Session`. A session must begin a transaction (`Session::BeginTxn`) before it may perform any `Session::Write`s and `Session::Read`s.

A session has a "back-pointer" to the `Database` object. It can be accessed as `session.Database`.

## UpdateChain
An `UpdateChain` represents all the values that have been written to a key. This list is stored as a linked list. This exercise builds an MVCC (multi-version concurrency control) database. This means, instead of keeping a single value for each key (which requires relatively coarse locking for snapshot isolation), writers create a new version (update) for each document they write to. Readers walk the `UpdateChain` and return the appropriate version.

# The Exercise
First we will implement `Session::BeginTxn`. Beginning a transaction should behave as follows:

* Input validation: it's illegal to begin a transaction when already in a transaction.
* Generate a transaction id to be used for its own writes (copied onto a `Mod.TxnId`). The `Database.GlobalTxn.NextTxnId` is provided for this purpose. Reminder: the `Database` can be accessed via `session.Database`.
 * This id should be saved into `session.Txn.Id`.
* Acquire a snapshot. The transaction must determine, by looking at the state captured on `Database.GlobalTxn`, which document versions should be considered visible under snapshot isolation. `Database.GlobalTxn` should contain all the data necessary to acquire a snapshot.
 * It's expected that this will write values for `Txn.SnapMin`, `Txn.SnapMax` and `Txn.ConcurrentSnap`([Link for appending to slices (i.e: std::vector or ArrayList) in Go])](https://tour.golang.org/moretypes/15).
 * A transaction in snapshot isolation must be able to read its own (currently uncommitted) writes.

Second we will implement `Session.IsVisible(txnId uint64)`:

* The input `txnId` is a transaction id of a committed or currently running transaction. Rolled back transactions use `TxnId == 0` and should never be considered visible.
* Based on the values `Session::BeginTxn` stores into `Session.Txn` and the state changes `Session::BeginTxn` makes on `Database.GlobalTxn`, the visibility of the input `txnId` can be deduced.

Definitions for `Session::Commit` and `Session::Rollback` are provided. A working solution can be derived without editting those methods. It may be important to see their definition though as each method in `lesson1.go` must work together to implement transactions that run at snapshot isolation.

As such, feel free to change their definitions (but not their signatures). The `GlobalTxn` and `Txn` data types may also withstand some alterations without resulting in a compilation error.

# Instruction/Hints
One way to implement snapshot isolation is to have `GlobalTxn` track every `TxnId` that has ever committed in a set. Beginning a transaction could then take a lock and copy the set of committed transactions. A call to `IsVisible` returns true iff, the input `txnId` is in th set.

The example implementation obviously suffers in performance as the program commits more and more transactions. In this lesson (with the provided definitions for `Session::Commit` and `Session::Rollback`) there's a optimization the data types suggest.

* `SnapMin` is a number such that all smaller `TxnId`s have either committed or rolled back. Rolling back a transaction changes all `Mod.TxnId` values to 0. Thus a non-zero `TxnId < (<=?) SnapMin` can safely be considered visible.
* `SnapMax` is the next transaction id to be generated. This transaction obviously cannot observe any writes made by a transaction that hasn't yet begun.
* `ConcurrentSnap` is a slice (or ideally a set) of `TxnId`s for sessions that are currently in an active transaction. Initializing this requires looking at all `Database.GlobalTxn.Sessions`. `SnapMin` also requires a scan given the current data structure.


# Basic Application Example
The following can be written into `lesson1_test.go` and executed with `go test -run TestReadYourOwnWrite`.
```
func TestReadYourOwnWrite(test *testing.T) {
	var db *Database = NewDatabase()
	var alice *Session = db.NewSession()

	// Ignore error handling.
	_ = alice.BeginTxn()
	alice.Insert("Key", 1)
	if val := alice.Read("Key"); val != 1 {
		panic(fmt.Sprintf("Failed reading own write. Val: %v", val))
	}

	// Ignore error handling.
	_ = alice.Commit()
}
```

# Go Gotchas
Variables in Go are explicitly explicitly typed as either values or pointers (or interfaces, which the exercise is not yet leveraging). For example, the `Database.GlobalTxn` type is a value type. If a `Session` function wanted to access that member and found typing `session.Database.GlobalTxn` to be too long, it might be convenient to assign that structure to a local variable, e.g:
```
var globalTxn GlobalTxn = session.Database.GlobalTxn
// or
globalTxn := session.Database.GlobalTxn
```
However that would be a bug. That would make a copy of the structure into the function scope. Thus any reads/writes to the local copy would be isolated from the memory that the code is intending to update. Instead, get the address of the value:
```
var globalTxn *GlobalTxn = &session.Database.GlobalTxn
// or
globalTxn := &session.Database.GlobalTxn
```
