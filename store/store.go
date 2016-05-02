// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	//cfg HraftConfig

	m                 KVStore    // The key-value store for the system (local).
	raft              *raft.Raft // The consensus mechanism
	rpcServer         *RPCServer // Forwards to leader.
	disableForwarding bool       // should forwarding be turned off (default on)

	mu sync.Mutex
}

// New returns a new Store.
func New() *Store {
	return &Store{
		m:                 InMemKvStore{}, // temporarily here
		disableForwarding: false,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle, enableRaftLogging bool) error {

	os.MkdirAll(s.RaftDir, 0700)

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	//  Disable raft logger
	if !enableRaftLogging {
		config.LogOutput = ioutil.Discard
	} else {
		log.Infoln("Raft logging: ON")
	}

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.RaftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		log.Infoln("Enabling single-node mode")
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
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logAndStableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	log.Infof("Starting raft on: %s", s.RaftBind)
	ra, err := raft.NewRaft(config, (*fsm)(s), logAndStableStore, logAndStableStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	// Instanticate rpc server
	if !s.disableForwarding {
		var rpcBindAddr string
		if rpcBindAddr, err = getRpcBindAddr(s.RaftBind); err == nil {
			if s.rpcServer, err = NewRPCServer(rpcBindAddr); err == nil {
				s.rpcServer.Start(s.applyRaftLog)
			}
		}
	}

	return err
}

// Get returns the 'local' value for the given key.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.m.Get(key)
}

// Set sets the value for the given key by applying to the log.
func (s *Store) Set(key string, value []byte) error {

	c := commandOptimized{
		Op:    OpTypeSet,
		Key:   key,
		Value: value,
	}
	b := c.Serialize()

	return s.applyRaftLog(b)
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {

	c := commandOptimized{
		Op:  OpTypeDelete,
		Key: key,
	}
	b := c.Serialize()

	return s.applyRaftLog(b)
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) (err error) {
	log.Infof("[store] Received join request for remote node as %s", addr)

	if s.raft.State() == raft.Leader {
		f := s.raft.AddPeer(addr)
		err = f.Error()
	} else if !s.disableForwarding {

		co := commandOptimized{
			Op:  OpTypeJoin,
			Key: addr,
		}
		b := co.Serialize()
		err = s.forwardLogToLeader(b)
	} else {
		err = fmt.Errorf("node is not the leader")
	}
	/*
		f := s.raft.AddPeer(addr)
		if f.Error() != nil {
			return f.Error()
		}
	*/
	if err == nil {
		log.Infof("[store] Node at %s joined successfully", addr)
	}
	//return nil
	return
}

func (s *Store) forwardLogToLeader(b []byte) error {
	log.Infof("Forwarding to leader: %s", s.raft.Leader())

	leaderRpcAddr, err := getRpcBindAddr(s.raft.Leader())
	if err == nil {
		log.Debugf("Leader RPC address: %s", leaderRpcAddr)
		var conn net.Conn
		if conn, err = net.Dial("tcp", leaderRpcAddr); err == nil {
			_, err = requestResponseRpc(conn, b)
		}
	}
	return err
}

func (s *Store) applyRaftLog(b []byte) error {
	if s.raft.State() == raft.Leader {
		// Apply to leader log
		f := s.raft.Apply(b, raftTimeout)
		if err, ok := f.(error); ok {
			return err
		}
		return nil

	}

	if s.disableForwarding {
		return fmt.Errorf("node is not the leader")
	}
	// Forward to leader
	return s.forwardLogToLeader(b)
}
