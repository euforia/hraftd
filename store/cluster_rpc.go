package store

import (
	"net"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Transport interface {
	net.Listener

	Dial(address string, timeout time.Duration) (net.Conn, error)
}

type ClusterRPC struct {
	tn Transport
}

func NewClusterRPC(tn Transport) *ClusterRPC {
	return &ClusterRPC{
		tn: tn,
	}
}

func (crpc *ClusterRPC) Serve(callback func(payload []byte) error) error {
	for {
		conn, err := crpc.tn.Accept()
		if err != nil {
			return err
		}

		go crpc.handleConn(conn, callback)
	}
}

func (crpc *ClusterRPC) handleConn(conn net.Conn, callback func(payload []byte) error) {
	log.Infoln("[fowarder] Forwarder client connected:", conn.RemoteAddr())

	var (
		b   []byte
		err error
	)

	if b, err = recvRpcMessage(conn); err == nil {
		//&& len(b) > 0
		log.Debugln("[fowarder] Received forwarded bytes:", len(b))

		if err = callback(b); err == nil {
			err = sendRpcMessage(conn, []byte("ok"))
		} else {
			err = sendRpcMessage(conn, []byte(err.Error()))
		}
	}

	if err != nil {
		log.Errorln(err)
		conn.Close()
	}

}

func (crpc *ClusterRPC) Dial(address string, timeout time.Duration) (conn net.Conn, err error) {
	return crpc.tn.Dial(address, timeout)
}
