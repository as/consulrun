package msgpackrpc

import (
	"bufio"
	"io"
	"net/rpc"
	"sync"

	"github.com/as/consulrun/hashicorp/go-msgpack/codec"
)

var (
	msgpackHandle = &codec.MsgpackHandle{}
)

type MsgpackCodec struct {
	closed    bool
	conn      io.ReadWriteCloser
	bufR      *bufio.Reader
	bufW      *bufio.Writer
	enc       *codec.Encoder
	dec       *codec.Decoder
	writeLock sync.Mutex
}

func NewCodec(bufReads, bufWrites bool, conn io.ReadWriteCloser) *MsgpackCodec {
	return NewCodecFromHandle(bufReads, bufWrites, conn, msgpackHandle)
}

func NewCodecFromHandle(bufReads, bufWrites bool, conn io.ReadWriteCloser,
	h *codec.MsgpackHandle) *MsgpackCodec {
	cc := &MsgpackCodec{
		conn: conn,
	}
	if bufReads {
		cc.bufR = bufio.NewReader(conn)
		cc.dec = codec.NewDecoder(cc.bufR, h)
	} else {
		cc.dec = codec.NewDecoder(cc.conn, h)
	}
	if bufWrites {
		cc.bufW = bufio.NewWriter(conn)
		cc.enc = codec.NewEncoder(cc.bufW, h)
	} else {
		cc.enc = codec.NewEncoder(cc.conn, h)
	}
	return cc
}

func (cc *MsgpackCodec) ReadRequestHeader(r *rpc.Request) error {
	return cc.read(r)
}

func (cc *MsgpackCodec) ReadRequestBody(out interface{}) error {
	return cc.read(out)
}

func (cc *MsgpackCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	cc.writeLock.Lock()
	defer cc.writeLock.Unlock()
	return cc.write(r, body)
}

func (cc *MsgpackCodec) ReadResponseHeader(r *rpc.Response) error {
	return cc.read(r)
}

func (cc *MsgpackCodec) ReadResponseBody(out interface{}) error {
	return cc.read(out)
}

func (cc *MsgpackCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	cc.writeLock.Lock()
	defer cc.writeLock.Unlock()
	return cc.write(r, body)
}

func (cc *MsgpackCodec) Close() error {
	if cc.closed {
		return nil
	}
	cc.closed = true
	return cc.conn.Close()
}

func (cc *MsgpackCodec) write(obj1, obj2 interface{}) (err error) {
	if cc.closed {
		return io.EOF
	}
	if err = cc.enc.Encode(obj1); err != nil {
		return
	}
	if err = cc.enc.Encode(obj2); err != nil {
		return
	}
	if cc.bufW != nil {
		return cc.bufW.Flush()
	}
	return
}

func (cc *MsgpackCodec) read(obj interface{}) (err error) {
	if cc.closed {
		return io.EOF
	}

	if obj == nil {
		var obj2 interface{}
		return cc.dec.Decode(&obj2)
	}
	return cc.dec.Decode(obj)
}
