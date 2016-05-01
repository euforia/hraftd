package store

import (
	"net"

	log "github.com/Sirupsen/logrus"
)

type RPCServer struct {
	net.Listener
}

func NewRPCServer(bindAddr string) (rs *RPCServer, err error) {
	rs = &RPCServer{}
	log.Infoln("Starting RPC server on:", bindAddr)
	rs.Listener, err = net.Listen("tcp", bindAddr)
	return
}

func (rs *RPCServer) Start(callback func(payload []byte) error) {
	go func() {
		for {
			conn, err := rs.Accept()
			if err == nil {
				log.Infoln("[fowarder] Forwarder client connected:", conn.RemoteAddr())

				var b []byte
				if b, err = recvRpcMessage(conn); err == nil {
					//&& len(b) > 0
					log.Debugln("[fowarder] Received forwarded bytes:", len(b))

					if err = callback(b); err == nil {
						err = sendRpcMessage(conn, []byte("ok"))
					} else {
						err = sendRpcMessage(conn, []byte(err.Error()))
					}
				}
			}

			if err != nil {
				log.Errorln(err)
			}

			conn.Close()
		}
	}()
}
