package store

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/euforia/go-chord"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

const (
	opTypeSet byte = iota
	opTypeDelete
)

type command struct {
	Op    byte   `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func defaultChordConfig(bindAddr string, vnodeCount int, stabMin, stabMax time.Duration) *chord.Config {
	cfg := DefaultConfig(*bindAddr)

	if vnodeCount > 0 {
		cfg.Chord.NumVnodes = vnodeCount
	}

	// Stabilization
	cfg.Chord.StabilizeMin = stabMin
	cfg.Chord.StabilizeMax = stabMax
}

func createOrJoinChordRing(chordConfig *chord.Config, bindAddr, joinAddr string, timeout time.Duration) (*chord.Ring, error) {
	trans, err := chord.InitTCPTransport(bindAddr, timeout)
	if err != nil {
		return nil, err
	}

	if joinAddr != "" {
		return chord.Join(chordConfig, trans, joinAddr)
	}
	return chord.Create(chordConfig, trans)
}
