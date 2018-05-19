// Package msgpackrpc implements a MessagePack-RPC ClientCodec and ServerCodec
package msgpackrpc

import (
	"io"
	"net"
	"net/rpc"
)

func Dial(network, address string) (*rpc.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), err
}

func NewClient(conn io.ReadWriteCloser) *rpc.Client {
	return rpc.NewClientWithCodec(NewClientCodec(conn))
}

func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return NewCodec(true, true, conn)
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return NewCodec(true, true, conn)
}

func ServeConn(conn io.ReadWriteCloser) {
	rpc.ServeCodec(NewServerCodec(conn))
}
