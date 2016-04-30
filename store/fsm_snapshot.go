package store

import (
	"bytes"
	"encoding/gob"

	"github.com/hashicorp/raft"
)

type fsmSnapshot struct {
	store map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		var buff bytes.Buffer
		if err := gob.NewEncoder(&buff).Encode(f.store); err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(buff.Bytes()); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
