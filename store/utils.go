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
	Key   string
	Value []byte
}

// b[0] = op type
// b[1] = key length (max: 256)
func (co *commandOptimized) Serialize() []byte {
	pre := append([]byte{byte(co.Op), byte(uint8(len(co.Key)))}, []byte(co.Key)...)
	return append(pre, co.Value...)
}

// b[0] = op type
// b[1] = key length (max: 256)
func (co *commandOptimized) Deserialize(data []byte) {
	co.Op = OpType(data[0])
	co.Key = string(data[2 : uint8(data[1])+2])
	co.Value = data[uint8(data[1])+2:]
}

/*==========================================*/

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

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
