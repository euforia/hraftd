package store

import (
	"fmt"
)

type KVStore interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error

	// Return a current copy of the store.
	Snapshot() KVStore

	// Return a new instantiated KVStore
	New() KVStore
}

type InMemKvStore map[string][]byte

func (ikv InMemKvStore) Get(key string) ([]byte, error) {
	if v, ok := ikv[key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("Key not found: %s", key)
}

func (ikv InMemKvStore) Set(key string, value []byte) error {
	ikv[key] = value
	return nil
}

func (ikv InMemKvStore) Delete(key string) error {
	delete(ikv, key)
	return nil
}

func (ikv InMemKvStore) Snapshot() KVStore {
	t := InMemKvStore{}
	for k, v := range ikv {
		t[k] = v
	}
	return t
}
func (ikv InMemKvStore) New() KVStore {
	return InMemKvStore{}
}
