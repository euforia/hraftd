package store

import (
	"encoding/gob"
	"fmt"
	"io"
	//"io/ioutil"
)

type KVStore interface {
	Open() error

	CreateNamespace(ns []byte) error
	DeleteNamespace(ns []byte) error
	NamespaceExists(ns []byte) bool

	Get(ns []byte, key []byte) ([]byte, error)
	Set(ns []byte, key []byte, value []byte) error
	Delete(ns []byte, key []byte) error

	// Return a current copy of the store.
	Snapshot() (KVStore, error)

	Backup(w io.Writer) error
	Restore(rc io.ReadCloser) error
}

type InMemKvStore map[string][]byte

func (ikv InMemKvStore) Open() error { return nil }

func (ikv InMemKvStore) Get(ns []byte, key []byte) ([]byte, error) {
	if v, ok := ikv[string(key)]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("Key not found: %s", key)
}

func (ikv InMemKvStore) Set(ns []byte, key []byte, value []byte) error {
	ikv[string(key)] = value
	return nil
}

func (ikv InMemKvStore) Delete(ns []byte, key []byte) error {
	delete(ikv, string(key))
	return nil
}

func (ikv InMemKvStore) CreateNamespace(ns []byte) error {
	return nil
}

func (ikv InMemKvStore) DeleteNamespace(ns []byte) error {
	return nil
}

func (ikv InMemKvStore) NamespaceExists(ns []byte) bool {
	return true
}

func (ikv InMemKvStore) Snapshot() (KVStore, error) {
	t := InMemKvStore{}
	for k, v := range ikv {
		t[k] = v
	}
	return t, nil
}

func (ikv InMemKvStore) Backup(w io.Writer) error {
	return gob.NewEncoder(w).Encode(ikv)
}

func (ikv InMemKvStore) Restore(rc io.ReadCloser) error {
	err := gob.NewDecoder(rc).Decode(&ikv)
	rc.Close()
	return err
}

/*
func (ikv InMemKvStore) New() KVStore {
	return InMemKvStore{}
}
*/
