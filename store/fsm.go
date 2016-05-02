package store

import (
	"encoding/gob"
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/raft"
)

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) (ret interface{}) {
	var c commandOptimized
	c.Deserialize(l.Data)

	switch c.Op {
	case OpTypeSet:
		ret = f.applySet(c.Key, c.Value)
	case OpTypeDelete:
		ret = f.applyDelete(c.Key)
	case OpTypeJoin:
		ret = f.raft.AddPeer(c.Key)
	default:
		ret = fmt.Errorf("unrecognized command op: %v", c.Op)
		log.Errorln(ret)
	}
	return
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o := f.m.Snapshot()
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := f.m.New()
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

	//f.m[key] = value
	//return nil
	return f.m.Set(key, value)
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	//delete(f.m, key)
	//return nil
	return f.m.Delete(key)
}
