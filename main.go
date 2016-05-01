package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	//"log"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/euforia/hraftd/http"
	"github.com/euforia/hraftd/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

// Command line parameters
var (
	httpAddr string
	raftAddr string
	joinAddr string

	// Toggle raft logging
	logRaft bool
)

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raft-addr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")

	flag.BoolVar(&logRaft, "vv", false, "Very verbose logging")
	debug := flag.Bool("v", false, "Verbose logging")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *debug || logRaft {
		log.SetLevel(log.DebugLevel)
		log.Infoln("Debug mode: ON")
	}
}

func main() {

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := store.New()

	s.RaftDir = raftDir
	s.RaftBind = raftAddr

	if err := s.Open(joinAddr == "", logRaft); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
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
