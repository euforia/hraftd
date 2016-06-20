package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/boltdb/bolt"
)

type BoltKvStore struct {
	db     *bolt.DB
	dbfile string
}

func NewBoltKvStore(path string) *BoltKvStore {
	return &BoltKvStore{dbfile: path}
	//b.db, err = bolt.Open(b.dbfile, 0600, nil)
	//return
}

func (bkv *BoltKvStore) Open() (err error) {
	bkv.db, err = bolt.Open(bkv.dbfile, 0600, nil)
	return
}

func (bkv *BoltKvStore) CreateNamespace(ns []byte) error {
	return bkv.db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucket(ns)
		return e
	})
}

func (bkv *BoltKvStore) DeleteNamespace(ns []byte) error {
	return bkv.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(ns)
	})
}

func (bkv *BoltKvStore) NamespaceExists(ns []byte) (be bool) {
	bkv.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(ns); b == nil {
			be = false
		} else {
			be = true
		}
		return nil
	})
	return be
}

func (bkv *BoltKvStore) Get(ns []byte, key []byte) (d []byte, err error) {
	err = bkv.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket(ns); bucket != nil {
			//bucket := tx.Bucket(ns)
			if d = bucket.Get(key); d == nil {
				return fmt.Errorf("key not found: ns=%s key=%s", ns, key)
			}
			return nil
		}
		return fmt.Errorf("namespace not found: %s", ns)
	})
	return
}

func (bkv *BoltKvStore) Set(ns []byte, key []byte, value []byte) error {
	return bkv.db.Update(func(tx *bolt.Tx) error {
		if bkt := tx.Bucket(ns); bkt != nil {
			return bkt.Put(key, value)
		}
		return fmt.Errorf("namespace not found: %s", ns)
	})
}

func (bkv *BoltKvStore) Delete(ns []byte, key []byte) error {
	return bkv.db.Update(func(tx *bolt.Tx) error {
		if bkt := tx.Bucket(ns); bkt != nil {
			return bkt.Delete(key)
		}
		return fmt.Errorf("namespace not found: %s", ns)
	})
}

// Backup current db to the given path
func (bkv *BoltKvStore) Backup(w io.Writer) error {

	tdb, err := bolt.Open(bkv.dbfile, 0666, &bolt.Options{ReadOnly: true})
	//f, err := os.Create(path)
	if err == nil {
		defer tdb.Close()
		err = tdb.View(func(tx *bolt.Tx) error {
			_, e := tx.WriteTo(w)
			//tx.CopyFile(path, 0666)
			return e
		})
	}

	return err
}

// Restore complete store from reader
func (bkv *BoltKvStore) Restore(rc io.ReadCloser) (err error) {
	if err = bkv.db.Close(); err == nil {
		var f *os.File
		if f, err = os.Open(bkv.dbfile); err == nil {
			//defer f.Close()
			if _, err = io.Copy(f, rc); err == nil {
				f.Close()
				bkv.db, err = bolt.Open(bkv.dbfile, 0600, nil)
			}
			rc.Close()
		}
	}
	return
}

// Return a current copy of the store.

func (bkv *BoltKvStore) Snapshot() (KVStore, error) {

	var tstore *BoltKvStore

	f, err := ioutil.TempFile("", "hraftd-snap-")
	if err == nil {
		if err = bkv.Backup(f); err == nil {

			n := f.Name()
			//tstore = &BoltKvStore{dbfile: f.Name()}
			if err = f.Close(); err == nil {
				tstore = NewBoltKvStore(n)
				err = tstore.Open()
			}
		}
	}

	return tstore, err
}

// Return a new instantiated KVStore
//func (bkv *BoltKvStore) New() KVStore {}
