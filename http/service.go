// Package httpd provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package httpd

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/euforia/hraftd/store"
)

var defaultNamespace = []byte("_default_")

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(ns []byte, key []byte, lvl store.ConsistencyLevel) ([]byte, error)
	NamespaceExists(ns []byte, lvl store.ConsistencyLevel) (bool, error)

	CreateNamespace(ns []byte) error

	// Set sets the value for the given key, via distributed consensus.
	Set(ns []byte, key []byte, value []byte) error

	// Delete removes the given key, via distributed consensus.
	Delete(ns []byte, key []byte) error

	// Join joins the node (self), reachable at addr, to the cluster.
	Join(addr string) error
}

// Service provides HTTP service.
type Service struct {
	addr string
	ln   net.Listener

	store Store
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v0") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		log.Errorln(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	ns, key := func() (string, string) {
		parts := strings.Split(r.URL.Path, "/")
		//log.Printf("%#v\n", parts)
		if len(parts) == 4 {
			return parts[2], parts[3]
		} else if len(parts) == 3 {
			return parts[2], ""
		}
		return "", ""
	}()

	log.Debugf("%s Namespace='%s' Key='%s'", r.Method, ns, key)

	switch r.Method {
	case "GET":
		var lvl store.ConsistencyLevel
		if _, ok := r.URL.Query()["consistency"]; ok {
			lvl = store.StrongConsistency
		} else {
			lvl = store.NoneConsistency
		}

		// Check if namespace exists
		if key == "" {
			//w.WriteHeader(404)

			b, err := s.store.NamespaceExists([]byte(ns), lvl)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			if b {
				w.WriteHeader(200)
				return
			}
			w.WriteHeader(404)
		} else {

			v, err := s.store.Get([]byte(ns), []byte(key), lvl)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			b, err := json.Marshal(map[string]string{key: string(v)})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			io.WriteString(w, string(b))
		}
	case "POST":
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if ns == "" {
			//w.WriteHeader(404)
			if err := s.store.CreateNamespace([]byte(m["namespace"])); err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
			}
		} else {
			// Read the value from the POST body.

			for k, v := range m {
				if err := s.store.Set([]byte(ns), []byte(k), []byte(v)); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
					return
				}
			}
		}
	case "DELETE":
		if key == "" {
			// Delete namespace
			w.WriteHeader(404)
		} else {

			if err := s.store.Delete([]byte(ns), []byte(key)); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			//s.store.Delete(defaultNamespace, []byte(k))
			s.store.Delete([]byte(ns), []byte(key))
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
