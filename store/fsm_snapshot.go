package store

import (
	"bytes"
	//"encoding/gob"

	"github.com/hashicorp/raft"
)

type fsmSnapshot struct {
	store KVStore
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		var buff bytes.Buffer

		e := f.store.Backup(&buff)
		if e != nil {
			return e
		}

		//if err := gob.NewEncoder(&buff).Encode(f.store); err != nil {
		//	return err
		//}

		// Write data to sink.
		if _, e = sink.Write(buff.Bytes()); e != nil {
			return e
		}

		// Close the sink.
		if e = sink.Close(); e != nil {
			return e
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
