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

type ConsistencyLevel uint8

const (
	NoneConsistency ConsistencyLevel = iota
	WeakConsistency
	StrongConsistency
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

	raft   *raft.Raft // The consensus mechanism
	raftTn *mux.Layer // Raft transport mux

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
func (s *Store) Open(cfg *HraftConfig) (err error) {

	s.cfg = *cfg

	os.MkdirAll(s.cfg.RaftDataDir, 0700)

	// Open datastore
	if err = s.m.Open(); err != nil {
		return
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	//  Disable raft logger

	if !s.cfg.EnableRaftLogging {
		config.LogOutput = ioutil.Discard
	} else {
		log.Infoln("Enabling raft logs")
	}

	// Check for any existing peers.
	var peers []string
	if peers, err = readPeersJSON(filepath.Join(s.cfg.RaftDataDir, "peers.json")); err != nil {
		return
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if s.cfg.JoinAddr == "" && len(peers) <= 1 {
		log.Infoln("Enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// mux
	var ln net.Listener
	if ln, err = net.Listen("tcp", s.cfg.RaftBindAddr); err != nil {
		return err
	}

	// Setup Raft communication.
	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", s.cfg.RaftBindAddr); err != nil {
		return err
	}

	muxTrans := mux.NewMuxedTransport(ln, addr)
	go muxTrans.Serve()

	s.raftTn = muxTrans.Listen(muxRaftHeader)

	transport := raft.NewNetworkTransport(s.raftTn, 3, 10*time.Second, config.LogOutput)

	// Create peer storage.
	s.peerStore = raft.NewJSONPeers(s.cfg.RaftDataDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	var snapshots *raft.FileSnapshotStore
	if snapshots, err = raft.NewFileSnapshotStore(s.cfg.RaftDataDir, retainSnapshotCount, os.Stderr); err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logAndStableStore *raftboltdb.BoltStore
	if logAndStableStore, err = raftboltdb.NewBoltStore(filepath.Join(s.cfg.RaftDataDir, "raft.db")); err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	log.Infof("[raft] Starting raft on: %s", s.cfg.RaftBindAddr)
	var ra *raft.Raft
	if ra, err = raft.NewRaft(config, (*fsm)(s), logAndStableStore, logAndStableStore, snapshots, s.peerStore, transport); err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	knownPeers, _ := s.peerStore.Peers()
	log.Infof("[raft] Known peers: %v", knownPeers)

	// Instanticate inter-node communcation (rpc server)
	if s.cfg.EnableLeaderForward {
		s.crpc = NewClusterRPC(muxTrans.Listen(muxRpcHeader))
		go s.crpc.Serve(s.serveRPC)
	}

	return
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) (err error) {
	log.Infof("[raft] Received join request for remote node as %s", addr)

	if s.raft.State() == raft.Leader {
		f := s.raft.AddPeer(addr)
		err = f.Error()
	} else if s.cfg.EnableLeaderForward {
		co := raftCommand{
			Op:  OpTypeJoin,
			Key: []byte(addr),
		}
		b := co.Serialize()
		_, err = s.forwardRequestToLeader(b)
	} else {
		err = fmt.Errorf("node is not the leader")
	}

	if err == nil {
		log.Infof("[raft] Node at %s joined successfully", addr)
	}
	return
}

func (s *Store) applyRaftLog(b []byte) ([]byte, error) {
	if s.raft.State() == raft.Leader {
		// Apply to leader log
		f := s.raft.Apply(b, raftTimeout)

		if e := f.(raft.Future); e.Error() != nil {
			return nil, e.Error()
		}

		resp := f.Response().(*fsmResp)
		return resp.data, resp.err
	}

	if !s.cfg.EnableLeaderForward {
		return nil, fmt.Errorf("node is not the leader")
	}
	// Forward to leader
	return s.forwardRequestToLeader(b)
}

// Forward request to lead over rpc layer
func (s *Store) forwardRequestToLeader(b []byte) (resp []byte, err error) {
	if s.raft.Leader() != "" {
		log.Debugf("[store] Forwarding to leader: %s", s.raft.Leader())

		var conn net.Conn
		if conn, err = s.crpc.Dial(s.raft.Leader(), 10*time.Second); err == nil {
			resp, err = requestResponseRpc(conn, b)
		}

	} else {
		err = fmt.Errorf("No known leader. Not forwarding request")
	}
	return
}

// handles all the forwarded requests.
func (s *Store) serveRPC(b []byte) (resp []byte, err error) {
	var c raftCommand
	c.Deserialize(b)

	switch c.Op {
	case OpTypeGet:
		resp, err = s.Get(c.Namespace, c.Key, StrongConsistency)
	case OpTypeSet:
		err = s.Set(c.Namespace, c.Key, c.Value)
	case OpTypeNamespaceCreate:
		err = s.CreateNamespace(c.Namespace)
	case OpTypeNamespaceDelete:
		err = s.DeleteNamespace(c.Namespace)
	case OpTypeNamespaceExists:
		var exists bool
		if exists, err = s.NamespaceExists(c.Namespace, StrongConsistency); err == nil {
			if exists {
				resp = []byte("true")
			} else {
				resp = []byte("false")
			}
		}
	case OpTypeJoin:
		r := s.raft.AddPeer(string(c.Key))
		err = r.Error()
	default:
		err = fmt.Errorf("unsupported rpc command op: %v", c.Op)
	}

	return
}
