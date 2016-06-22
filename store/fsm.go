package store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// decode gob to command
func decodeCommand(b []byte) (*command, error) {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	var c command
	err := dec.Decode(&c)
	return &c, err
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {

	c, err := decodeCommand(l.Data)
	if err != nil {
		return err
	}

	switch c.Op {
	case opTypeSet:
		return f.applySet(c.Key, c.Value)
	case opTypeDelete:
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the datastore.
	o := f.m.Snapshot()
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := NewInMemDatastore()
	if err := json.NewDecoder(rc).Decode(&o.m); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

// Actually apply to storage
func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m.Set(key, value)
}

// Actually apply to storage
func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m.Delete(key)
}
