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

	"github.com/euforia/hraftd/mux"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second

	muxRaftHeader = 1
	muxRpcHeader  = 2
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	cfg HraftConfig

	m KVStore // The key-value store for the system (local).

	raft *raft.Raft // The consensus mechanism

	crpc *ClusterRPC // Cluster communication. e.g. joins

	mu sync.Mutex

	peerStore *raft.JSONPeers
}

// New returns a new Store.
func New(kvstore KVStore) *Store {
	return &Store{
		m: kvstore,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
//func (s *Store) Open(enableSingle, enableRaftLogging bool) error {
func (s *Store) Open(cfg *HraftConfig) error {

	s.cfg = *cfg

	os.MkdirAll(s.cfg.RaftDataDir, 0700)

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	//  Disable raft logger

	if !s.cfg.EnableRaftLogging {
		config.LogOutput = ioutil.Discard
	} else {
		log.Infoln("Enabling raft logs")
	}

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.cfg.RaftDataDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if s.cfg.JoinAddr == "" && len(peers) <= 1 {
		log.Infoln("Enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// mux
	ln, err := net.Listen("tcp", s.cfg.RaftBindAddr)
	if err != nil {
		return err
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.cfg.RaftBindAddr)
	if err != nil {
		return err
	}

	muxTrans := mux.NewMuxedTransport(ln, addr)
	go muxTrans.Serve()

	raftTn := muxTrans.Listen(muxRaftHeader)

	transport := raft.NewNetworkTransport(raftTn, 3, 10*time.Second, config.LogOutput)

	// Create peer storage.
	s.peerStore = raft.NewJSONPeers(s.cfg.RaftDataDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.cfg.RaftDataDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logAndStableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.cfg.RaftDataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	log.Infof("[raft] Starting raft on: %s", s.cfg.RaftBindAddr)
	ra, err := raft.NewRaft(config, (*fsm)(s), logAndStableStore, logAndStableStore, snapshots, s.peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	knownPeers, _ := s.peerStore.Peers()
	log.Infof("[raft] Known peers: %v", knownPeers)

	// Instanticate rpc server
	if s.cfg.EnableLeaderForward {
		s.crpc = NewClusterRPC(muxTrans.Listen(muxRpcHeader))
		go s.crpc.Serve(s.applyRaftLog)
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
	log.Infof("[raft] Received join request for remote node as %s", addr)

	if s.raft.State() == raft.Leader {
		f := s.raft.AddPeer(addr)
		err = f.Error()
	} else if s.cfg.EnableLeaderForward {
		co := commandOptimized{
			Op:  OpTypeJoin,
			Key: addr,
		}
		b := co.Serialize()
		err = s.forwardLogToLeader(b)
	} else {
		err = fmt.Errorf("node is not the leader")
	}

	if err == nil {
		log.Infof("[raft] Node at %s joined successfully", addr)
	}
	return
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

	if !s.cfg.EnableLeaderForward {
		return fmt.Errorf("node is not the leader")
	}
	// Forward to leader
	return s.forwardLogToLeader(b)
}

func (s *Store) forwardLogToLeader(b []byte) (err error) {
	if s.raft.Leader() != "" {
		log.Debugf("[store] Forwarding to leader: %s", s.raft.Leader())

		var conn net.Conn
		if conn, err = s.crpc.Dial(s.raft.Leader(), 10*time.Second); err == nil {
			_, err = requestResponseRpc(conn, b)
		}

	} else {
		err = fmt.Errorf("No known leader. Not forwarding request")
	}
	return
}
