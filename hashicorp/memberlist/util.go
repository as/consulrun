package memberlist

import (
	"bytes"
	"compress/lzw"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/go-msgpack/codec"
	"github.com/sean-/seed"
)

const pushPullScaleThreshold = 32

const (
	lzwLitWidth = 8
)

func init() {
	seed.Init()
}

func decode(buf []byte, out interface{}) error {
	r := bytes.NewReader(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func encode(msgType messageType, in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(msgType))
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func randomOffset(n int) int {
	if n == 0 {
		return 0
	}
	return int(rand.Uint32() % uint32(n))
}

func suspicionTimeout(suspicionMult, n int, interval time.Duration) time.Duration {
	nodeScale := math.Max(1.0, math.Log10(math.Max(1.0, float64(n))))

	timeout := time.Duration(suspicionMult) * time.Duration(nodeScale*1000) * interval / 1000
	return timeout
}

func retransmitLimit(retransmitMult, n int) int {
	nodeScale := math.Ceil(math.Log10(float64(n + 1)))
	limit := retransmitMult * int(nodeScale)
	return limit
}

func shuffleNodes(nodes []*nodeState) {
	n := len(nodes)
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func pushPullScale(interval time.Duration, n int) time.Duration {

	if n <= pushPullScaleThreshold {
		return interval
	}

	multiplier := math.Ceil(math.Log2(float64(n))-math.Log2(pushPullScaleThreshold)) + 1.0
	return time.Duration(multiplier) * interval
}

func moveDeadNodes(nodes []*nodeState, gossipToTheDeadTime time.Duration) int {
	numDead := 0
	n := len(nodes)
	for i := 0; i < n-numDead; i++ {
		if nodes[i].State != stateDead {
			continue
		}

		if time.Since(nodes[i].StateChange) <= gossipToTheDeadTime {
			continue
		}

		nodes[i], nodes[n-numDead-1] = nodes[n-numDead-1], nodes[i]
		numDead++
		i--
	}
	return n - numDead
}

func kRandomNodes(k int, nodes []*nodeState, filterFn func(*nodeState) bool) []*nodeState {
	n := len(nodes)
	kNodes := make([]*nodeState, 0, k)
OUTER:

	for i := 0; i < 3*n && len(kNodes) < k; i++ {

		idx := randomOffset(n)
		node := nodes[idx]

		if filterFn != nil && filterFn(node) {
			continue OUTER
		}

		for j := 0; j < len(kNodes); j++ {
			if node == kNodes[j] {
				continue OUTER
			}
		}

		kNodes = append(kNodes, node)
	}
	return kNodes
}

func makeCompoundMessage(msgs [][]byte) *bytes.Buffer {

	buf := bytes.NewBuffer(nil)

	buf.WriteByte(uint8(compoundMsg))

	buf.WriteByte(uint8(len(msgs)))

	for _, m := range msgs {
		binary.Write(buf, binary.BigEndian, uint16(len(m)))
	}

	for _, m := range msgs {
		buf.Write(m)
	}

	return buf
}

func decodeCompoundMessage(buf []byte) (trunc int, parts [][]byte, err error) {
	if len(buf) < 1 {
		err = fmt.Errorf("missing compound length byte")
		return
	}
	numParts := uint8(buf[0])
	buf = buf[1:]

	if len(buf) < int(numParts*2) {
		err = fmt.Errorf("truncated len slice")
		return
	}

	lengths := make([]uint16, numParts)
	for i := 0; i < int(numParts); i++ {
		lengths[i] = binary.BigEndian.Uint16(buf[i*2 : i*2+2])
	}
	buf = buf[numParts*2:]

	for idx, msgLen := range lengths {
		if len(buf) < int(msgLen) {
			trunc = int(numParts) - idx
			return
		}

		slice := buf[:msgLen]
		buf = buf[msgLen:]
		parts = append(parts, slice)
	}
	return
}

func compressPayload(inp []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	compressor := lzw.NewWriter(&buf, lzw.LSB, lzwLitWidth)

	_, err := compressor.Write(inp)
	if err != nil {
		return nil, err
	}

	if err := compressor.Close(); err != nil {
		return nil, err
	}

	c := compress{
		Algo: lzwAlgo,
		Buf:  buf.Bytes(),
	}
	return encode(compressMsg, &c)
}

func decompressPayload(msg []byte) ([]byte, error) {

	var c compress
	if err := decode(msg, &c); err != nil {
		return nil, err
	}
	return decompressBuffer(&c)
}

func decompressBuffer(c *compress) ([]byte, error) {

	if c.Algo != lzwAlgo {
		return nil, fmt.Errorf("Cannot decompress unknown algorithm %d", c.Algo)
	}

	uncomp := lzw.NewReader(bytes.NewReader(c.Buf), lzw.LSB, lzwLitWidth)
	defer uncomp.Close()

	var b bytes.Buffer
	_, err := io.Copy(&b, uncomp)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func joinHostPort(host string, port uint16) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

func hasPort(s string) bool {

	if strings.LastIndex(s, "[") == 0 {
		return strings.LastIndex(s, ":") > strings.LastIndex(s, "]")
	}

	return strings.Count(s, ":") == 1
}

func ensurePort(s string, port int) string {
	if hasPort(s) {
		return s
	}

	s = strings.Trim(s, "[]")
	s = net.JoinHostPort(s, strconv.Itoa(port))
	return s
}
