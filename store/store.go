// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// The key-value store for the system.
type InMemDatastore struct {
	m map[string]string
}

func NewInMemDatastore() *InMemDatastore {
	return &InMemDatastore{m: map[string]string{}}
}

func (im *InMemDatastore) Get(key string) (string, error) {
	if _, ok := im.m[key]; ok {
		return im.m[key], nil
	}
	return "", fmt.Errorf("not found: %s", key)
}

func (im *InMemDatastore) Set(key, value string) error {
	im.m[key] = value
	return nil
}

func (im *InMemDatastore) Delete(key string) error {
	if _, ok := im.m[key]; !ok {
		return fmt.Errorf("not found: %s", key)
	}
	delete(im.m, key)
	return nil
}

// Clone datastore by copying current data to a new instance
// and return the new instance
func (im *InMemDatastore) Snapshot() *InMemDatastore {
	o := NewInMemDatastore()
	for k, v := range im.m {
		o.m[k] = v
	}
	return o
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	mu sync.Mutex

	m *InMemDatastore // The key-value store for the system.

	raft      *raft.Raft      // The consensus mechanism
	peerStore *raft.JSONPeers // List of peers

	logger *log.Logger
}

// New returns a new Store.
func New() *Store {
	return &Store{
		m:      NewInMemDatastore(),
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.RaftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	s.peerStore = raft.NewJSONPeers(s.RaftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, s.peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	return nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Get(key)
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    opTypeSet,
		Key:   key,
		Value: value,
	}

	return s.applyRaftLog(c)
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  opTypeDelete,
		Key: key,
	}

	return s.applyRaftLog(c)
}

func (s *Store) applyRaftLog(c *command) error {

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(buf.Bytes(), raftTimeout)
	if err, ok := f.(error); ok {
		return err
	}
	return nil
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}
