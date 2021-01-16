package nachdb

import (
	"fmt"
	"strings"
	"sync"
)

type Verb int

const (
	InsertMod Verb = iota
	UpdateMod
	DeleteMod
)

func (verb Verb) String() string {
	switch verb {
	case InsertMod:
		return "Insert"
	case UpdateMod:
		return "Update"
	case DeleteMod:
		return "Delete"
	default:
		panic("Bad verb")
	}
}

type UpdateChain struct {
	Key  string
	Head *Mod

	sync.Mutex
}

func NewUpdateChain(key string) *UpdateChain {
	return &UpdateChain{Key: key}
}

func (chain *UpdateChain) Add(mod *Mod) {
	mod.Next = chain.Head
	if chain.Head != nil {
		chain.Head.Prev = mod
	}
	chain.Head = mod
}

func (chain *UpdateChain) Dump() string {
	ret := &strings.Builder{}
	fmt.Fprintf(ret, "Key: %v", chain.Key)

	node := chain.Head
	for node != nil {
		fmt.Fprintf(ret, " -> %v", node)
		node = node.Next
	}

	return ret.String()
}

type Mod struct {
	TxnId uint64
	Value int
	Verb  Verb

	Next *Mod
	Prev *Mod
}

func (mod *Mod) String() string {
	return fmt.Sprintf("TxnId: %v Value: %v %v", mod.TxnId, mod.Verb, mod.Value)
}
