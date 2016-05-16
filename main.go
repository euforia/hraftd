package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/euforia/hraftd/http"
	"github.com/euforia/hraftd/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
)

// Command line parameters
var (
	httpAddr string
	hrConfig = store.DefaultHraftConfig()
)

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&hrConfig.RaftBindAddr, "raft-addr", store.DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&hrConfig.JoinAddr, "join", "", "Set join address, if any")

	flag.StringVar(&hrConfig.RaftDataDir, "data-dir", store.DefaultRaftDataDir, "Storage data directory")

	flag.BoolVar(&hrConfig.EnableRaftLogging, "vv", false, "Very verbose logging")
	flag.BoolVar(&hrConfig.EnableDebug, "v", false, "Verbose logging")

	flag.Usage = func() {
		fmt.Printf(`
Usage:

    %s [options]

Options:

`, os.Args[0])

		flag.PrintDefaults()
	}

	flag.Parse()

	if hrConfig.EnableDebug || hrConfig.EnableRaftLogging {
		log.SetLevel(log.DebugLevel)
		log.Infoln("Debug mode: ON")
	}

	if hrConfig.RaftDataDir == "" {
		log.Fatalln("No Raft storage directory specified")
	}
}

func main() {

	kvstore, err := store.NewBoltKvStore(hrConfig.RaftDataDir + "/bolt-kvstore.db")
	if err != nil {
		log.Fatalln(err)
	}

	s := store.New(kvstore)
	//s := store.New(store.InMemKvStore{})

	if err := s.Open(&hrConfig); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if hrConfig.JoinAddr != "" {
		if err := join(hrConfig.JoinAddr, hrConfig.RaftBindAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", hrConfig.JoinAddr, err.Error())
		}
	}

	log.Infof("Started hraftd successfully")

	// Block forever.
	select {}
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
