package store

import (
	"fmt"

	"github.com/hashicorp/raft"
)

// Get returns the 'local' data for the given key from the backend KVStore
func (s *Store) Get(ns []byte, key []byte, lvl ConsistencyLevel) (b []byte, err error) {
	//s.mu.Lock()
	//defer s.mu.Unlock()

	var nsExists bool
	if nsExists, _ = s.NamespaceExists(ns, NoneConsistency); nsExists {

		switch lvl {
		case NoneConsistency:
			b, err = s.m.Get(ns, key)
			//fmt.Printf("%s, %s\n", b, err)
		case WeakConsistency:
			if s.raft.State() == raft.Leader {
				b, err = s.m.Get(ns, key)
			} else {
				err = fmt.Errorf("node not leader")
			}
		case StrongConsistency:
			if s.raft.State() == raft.Leader {
				b, err = s.m.Get(ns, key)
			} else {
				c := raftCommand{Op: OpTypeGet, Namespace: ns, Key: key}
				b, err = s.forwardRequestToLeader(c.Serialize())

				fmt.Printf("%s, err=%s\n", b, err)
			}

		default:
			err = fmt.Errorf("Invalid consistency level: %d", lvl)
		}

	} else {
		err = fmt.Errorf("Namespace not found: %s", ns)
	}

	return
}

func (s *Store) NamespaceExists(ns []byte, lvl ConsistencyLevel) (b bool, err error) {
	switch lvl {
	case NoneConsistency:
		b = s.m.NamespaceExists(ns)
	case WeakConsistency:
		if s.raft.State() == raft.Leader {
			b = s.m.NamespaceExists(ns)
		} else {
			err = fmt.Errorf("node not leader")
		}
	case StrongConsistency:
		if s.raft.State() == raft.Leader {
			b = s.m.NamespaceExists(ns)
		} else {

			c := raftCommand{Op: OpTypeNamespaceExists, Namespace: ns}
			var resp []byte
			if resp, err = s.forwardRequestToLeader(c.Serialize()); err == nil {
				if string(resp) == "true" {
					b = true
				} else {
					b = false
				}
			}
			//err = fmt.Errorf("TBI: foward to leader")

		}
	default:
		b = false
		err = fmt.Errorf("Invalid consistency level: %d", lvl)
	}
	return
}

// Set sets the value for the given key by applying to the log.
func (s *Store) Set(ns []byte, key []byte, value []byte) (err error) {
	if s.m.NamespaceExists(ns) {
		c := raftCommand{
			Op:        OpTypeSet,
			Namespace: ns,
			Key:       key,
			Value:     value,
		}
		_, err = s.applyRaftLog(c.Serialize())
	} else {
		err = fmt.Errorf("Namespace not found: %s", ns)
	}
	return
}

// Delete deletes the given key.
func (s *Store) Delete(ns []byte, key []byte) (err error) {
	if s.m.NamespaceExists(ns) {
		c := raftCommand{
			Op:        OpTypeDelete,
			Namespace: ns,
			Key:       key,
		}
		_, err = s.applyRaftLog(c.Serialize())

	} else {
		err = fmt.Errorf("Namespace not found: %s", ns)
	}
	return
}

func (s *Store) DeleteNamespace(ns []byte) (err error) {
	if s.m.NamespaceExists(ns) {
		c := raftCommand{
			Op:        OpTypeNamespaceDelete,
			Namespace: ns,
		}
		_, err = s.applyRaftLog(c.Serialize())
	} else {
		err = fmt.Errorf("Namespace not found: %s", ns)
	}
	return
}

func (s *Store) CreateNamespace(ns []byte) (err error) {
	if !s.m.NamespaceExists(ns) {
		c := raftCommand{
			Op:        OpTypeNamespaceCreate,
			Namespace: ns,
		}
		_, err = s.applyRaftLog(c.Serialize())
	} else {
		err = fmt.Errorf("Namespace exists: %s", ns)
	}
	return
}
