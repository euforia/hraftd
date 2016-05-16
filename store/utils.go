package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	//"strconv"
	"strings"
)

func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	err = json.Unmarshal(b, &peers)
	return peers, err
}

// int64 to byte array
func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// byte array to int64
func btoi(b []byte) (i int64, err error) {
	err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &i)
	return
}

func recvRpcMessage(conn net.Conn) ([]byte, error) {

	b := make([]byte, 8)
	r, err := conn.Read(b)
	if err == nil {
		if r == 8 {
			var length int64
			if length, err = btoi(b); err != nil {
				return nil, err
			}

			b = make([]byte, length)
			if r, err = conn.Read(b); err == nil {
				if int64(r) == length {
					return b, nil
				} else {
					err = fmt.Errorf("Payload length mismatch %d != %d", r, length)
				}
			}
		} else {
			err = fmt.Errorf("Invalid payload")
		}
	}
	return nil, err
}

func sendRpcMessage(conn net.Conn, data []byte) (err error) {
	_, err = conn.Write(append(itob(int64(len(data))), data...))
	return
}

func requestResponseRpc(conn net.Conn, data []byte) (resp []byte, err error) {
	if err = sendRpcMessage(conn, data); err == nil {
		if resp, err = recvRpcMessage(conn); err == nil {
			if strings.HasPrefix(string(resp), "error") {
				err = fmt.Errorf("%s", resp)
			}
		}
	}
	return
}
