package store

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c commandOptimized
	c.Deserialize(l.Data)

	switch c.Op {
	case OpTypeSet:
		return f.applySet(c.Key, c.Value)
	case OpTypeDelete:
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %v", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string][]byte)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	if err := gob.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.m[key] = value

	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.m, key)

	return nil
}
