package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var (
	hraftConfig = DefaultHraftConfig()
)

func TestMain(m *testing.M) {
	hraftConfig.EnableLeaderForward = false
	hraftConfig.RaftBindAddr = "127.0.0.1:0"
	os.Exit(m.Run())
}

func Test_StoreOpen(t *testing.T) {
	s := New(InMemKvStore{})

	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	hraftConfig.RaftDataDir = tmpDir

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(&hraftConfig); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

func Test_StoreOpenSingleNode(t *testing.T) {
	s := New(InMemKvStore{})

	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	hraftConfig.RaftDataDir = tmpDir

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(&hraftConfig); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Set("foo", []byte("bar")); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, err := s.Get("foo")
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if string(value) != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("foo"); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, err = s.Get("foo")
	if err == nil {
		t.Fatal("key should be missing")
	}
	if string(value) != "" {
		t.Fatalf("key has wrong value: %s", value)
	}

}
