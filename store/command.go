package store

type OpType uint8

const (
	OpTypeGet OpType = iota
	OpTypeSet
	OpTypeDelete
	OpTypeNamespaceCreate
	OpTypeNamespaceDelete
	OpTypeNamespaceExists

	OpTypeJoin
)

type raftCommand struct {
	Op        OpType
	Namespace []byte
	Key       []byte
	Value     []byte
}

// b[0] = op type
// b[1] = key length (max: 256)
func (co *raftCommand) Serialize() []byte {
	pre := append([]byte{byte(co.Op), byte(uint8(len(co.Namespace)))}, co.Namespace...)
	pre = append(pre, byte(uint8(len(co.Key))))
	pre = append(pre, co.Key...)
	return append(pre, co.Value...)
	//return pre
}

// b[0] = op type
// b[1] = key length (max: 256)
func (co *raftCommand) Deserialize(data []byte) {
	co.Op = OpType(data[0])

	pos := uint8(data[1]) + 2

	co.Namespace = data[2:pos]

	nextPos := uint8(data[pos]) + pos + 1

	co.Key = data[pos+1 : nextPos]
	co.Value = data[nextPos:]
}
