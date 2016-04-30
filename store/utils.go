package store

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"time"
)

type OpType uint8

const (
	OpTypeSet OpType = iota
	OpTypeDelete
)

type commandOptimized struct {
	Op    OpType
	Key   []byte
	Value []byte
}

// b[0] = op type
// b[1] = key length (max: 256)
func (co *commandOptimized) Serialize() []byte {
	pre := append([]byte{byte(co.Op), byte(uint8(len(co.Key)))}, co.Key...)
	return append(pre, co.Value...)
}

func (co *commandOptimized) Deserialize(data []byte) {
	co.Op = OpType(data[0])
	// if unint8(data[2]) > 256 {
	// Error out
	//}
	co.Key = data[2 : uint8(data[1])+2]
	co.Value = data[uint8(data[1])+2:]
}

/*==========================================*/

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

/*
type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}
*/

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
