package store

type OpType uint8

const (
	OpTypeSet OpType = iota
	OpTypeDelete
	OpTypeJoin
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
