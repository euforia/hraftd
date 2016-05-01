package store

import (
	"testing"
)

func Test_commandOptimized(t *testing.T) {
	co := commandOptimized{Op: OpTypeDelete, Key: "key", Value: []byte("value")}
	t.Logf("%#v\n", co)

	b := co.Serialize()

	if uint8(b[0]) != 1 {
		t.Fatal("Op mismatch")
	}

	if string(b[2:uint8(b[1])+2]) != "key" {
		t.Fatal("Key mismatch!")
	}

	t.Logf("OpType: %d; Key len: %d; Key: '%s'; Value: '%s'\n", b[0], b[1], b[2:uint8(b[1])+2], b[uint8(b[1])+2:])

	var c commandOptimized
	c.Deserialize(b)

	if c.Op != OpTypeDelete {
		t.Fatal("Op deserialization failed")
	}
	if string(c.Key) != "key" {
		t.Fatal("Key deserialization failed")
	}
	if string(c.Value) != "value" {
		t.Fatal("Value deserialization failed")
	}

	t.Logf("%#v\n", c)
}

func Test_getRpcBindAddr(t *testing.T) {
	n, err := getRpcBindAddr(":10000")
	if err != nil {
		t.Fatal(err)
	}
	if n != ":10001" {
		t.Fatalf("Failed: %s\n", n)
	}

	t.Log(n)
}
