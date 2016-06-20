package store

import (
	//"encoding/gob"
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/raft"
)

type fsmResp struct {
	err  error
	data []byte
}

func (fr *fsmResp) Error() error {
	return fr.err
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c raftCommand
	c.Deserialize(l.Data)

	resp := &fsmResp{}

	switch c.Op {
	case OpTypeSet:
		resp.err = f.applySet(c.Namespace, c.Key, c.Value)
	case OpTypeDelete:
		resp.err = f.applyDelete(c.Namespace, c.Key)
	case OpTypeNamespaceCreate:
		resp.err = f.applyNamespaceCreate(c.Namespace)
	case OpTypeNamespaceDelete:
		resp.err = f.applyNamespaceDelete(c.Namespace)
	default:
		resp.err = fmt.Errorf("unsupported raft command op: %v", c.Op)
	}

	if resp.err != nil {
		log.Warningln("[raft]", resp.err)
	}

	return resp
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o, err := f.m.Snapshot()
	if err == nil {
		return &fsmSnapshot{store: o}, nil
	}
	return nil, err
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return f.m.Restore(rc)
}

func (f *fsm) applySet(ns []byte, key []byte, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.m.Set(ns, key, value)
}

func (f *fsm) applyDelete(ns []byte, key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.m.Delete(ns, key)
}

func (f *fsm) applyNamespaceCreate(ns []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	//return f.m.Delete(key)

	return f.m.CreateNamespace(ns)
}
func (f *fsm) applyNamespaceDelete(ns []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	//return f.m.Delete(key)
	return f.m.DeleteNamespace(ns)
}

/*
func (f *fsm) applyNamespaceExists(ns []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	b := f.m.NamespaceExists(ns)
	if b {
		return []byte("true"), nil
	}
	return []byte("false"), nil
	//return nil, fmt.Errorf("not exists: %s", ns)
}

func (f *fsm) applyGet(ns []byte, key []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.m.Get(ns, key)
}
*/
