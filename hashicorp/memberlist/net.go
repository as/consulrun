package memberlist

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/go-msgpack/codec"
)

const (
	ProtocolVersionMin uint8 = 1

	//

	ProtocolVersion2Compatible = 2

	ProtocolVersionMax = 5
)

type messageType uint8

const (
	pingMsg messageType = iota
	indirectPingMsg
	ackRespMsg
	suspectMsg
	aliveMsg
	deadMsg
	pushPullMsg
	compoundMsg
	userMsg // User mesg, not handled by us
	compressMsg
	encryptMsg
	nackRespMsg
	hasCrcMsg
	errMsg
)

type compressionType uint8

const (
	lzwAlgo compressionType = iota
)

const (
	MetaMaxSize            = 512 // Maximum size for node meta data
	compoundHeaderOverhead = 2   // Assumed header overhead
	compoundOverhead       = 2   // Assumed overhead per entry in compoundHeader
	userMsgOverhead        = 1
	blockingWarning        = 10 * time.Millisecond // Warn if a UDP packet takes this long to process
	maxPushStateBytes      = 10 * 1024 * 1024
)

type ping struct {
	SeqNo uint32

	Node string
}

type indirectPingReq struct {
	SeqNo  uint32
	Target []byte
	Port   uint16
	Node   string
	Nack   bool // true if we'd like a nack back
}

type ackResp struct {
	SeqNo   uint32
	Payload []byte
}

type nackResp struct {
	SeqNo uint32
}

type errResp struct {
	Error string
}

type suspect struct {
	Incarnation uint32
	Node        string
	From        string // Include who is suspecting
}

type alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
	Port        uint16
	Meta        []byte

	Vsn []uint8
}

type dead struct {
	Incarnation uint32
	Node        string
	From        string // Include who is suspecting
}

type pushPullHeader struct {
	Nodes        int
	UserStateLen int  // Encodes the byte lengh of user state
	Join         bool // Is this a join request or a anti-entropy run
}

type userMsgHeader struct {
	UserMsgLen int // Encodes the byte lengh of user state
}

type pushNodeState struct {
	Name        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Incarnation uint32
	State       nodeStateType
	Vsn         []uint8 // Protocol versions
}

type compress struct {
	Algo compressionType
	Buf  []byte
}

type msgHandoff struct {
	msgType messageType
	buf     []byte
	from    net.Addr
}

func (m *Memberlist) encryptionVersion() encryptionVersion {
	switch m.ProtocolVersion() {
	case 1:
		return 0
	default:
		return 1
	}
}

func (m *Memberlist) streamListen() {
	for {
		select {
		case conn := <-m.transport.StreamCh():
			go m.handleConn(conn)

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) handleConn(conn net.Conn) {
	m.logger.Printf("[DEBUG] memberlist: Stream connection %s", LogConn(conn))

	defer conn.Close()
	metrics.IncrCounter([]string{"memberlist", "tcp", "accept"}, 1)

	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))
	msgType, bufConn, dec, err := m.readStream(conn)
	if err != nil {
		if err != io.EOF {
			m.logger.Printf("[ERR] memberlist: failed to receive: %s %s", err, LogConn(conn))

			resp := errResp{err.Error()}
			out, err := encode(errMsg, &resp)
			if err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to encode error response: %s", err)
				return
			}

			err = m.rawSendMsgStream(conn, out.Bytes())
			if err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to send error: %s %s", err, LogConn(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case userMsg:
		if err := m.readUserMsg(bufConn, dec); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to receive user message: %s %s", err, LogConn(conn))
		}
	case pushPullMsg:
		join, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to read remote state: %s %s", err, LogConn(conn))
			return
		}

		if err := m.sendLocalState(conn, join); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to push local state: %s %s", err, LogConn(conn))
			return
		}

		if err := m.mergeRemoteState(join, remoteNodes, userState); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed push/pull merge: %s %s", err, LogConn(conn))
			return
		}
	case pingMsg:
		var p ping
		if err := dec.Decode(&p); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to decode ping: %s %s", err, LogConn(conn))
			return
		}

		if p.Node != "" && p.Node != m.config.Name {
			m.logger.Printf("[WARN] memberlist: Got ping for unexpected node %s %s", p.Node, LogConn(conn))
			return
		}

		ack := ackResp{p.SeqNo, nil}
		out, err := encode(ackRespMsg, &ack)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to encode ack: %s", err)
			return
		}

		err = m.rawSendMsgStream(conn, out.Bytes())
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to send ack: %s %s", err, LogConn(conn))
			return
		}
	default:
		m.logger.Printf("[ERR] memberlist: Received invalid msgType (%d) %s", msgType, LogConn(conn))
	}
}

func (m *Memberlist) packetListen() {
	for {
		select {
		case packet := <-m.transport.PacketCh():
			m.ingestPacket(packet.Buf, packet.From, packet.Timestamp)

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) ingestPacket(buf []byte, from net.Addr, timestamp time.Time) {

	if m.config.EncryptionEnabled() {

		plain, err := decryptPayload(m.config.Keyring.GetKeys(), buf, nil)
		if err != nil {
			if !m.config.GossipVerifyIncoming {

				plain = buf
			} else {
				m.logger.Printf("[ERR] memberlist: Decrypt packet failed: %v %s", err, LogAddress(from))
				return
			}
		}

		buf = plain
	}

	if len(buf) >= 5 && messageType(buf[0]) == hasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			m.logger.Printf("[WARN] memberlist: Got invalid checksum for UDP packet: %x, %x", crc, expected)
			return
		}
		m.handleCommand(buf[5:], from, timestamp)
	} else {
		m.handleCommand(buf, from, timestamp)
	}
}

func (m *Memberlist) handleCommand(buf []byte, from net.Addr, timestamp time.Time) {

	msgType := messageType(buf[0])
	buf = buf[1:]

	switch msgType {
	case compoundMsg:
		m.handleCompound(buf, from, timestamp)
	case compressMsg:
		m.handleCompressed(buf, from, timestamp)

	case pingMsg:
		m.handlePing(buf, from)
	case indirectPingMsg:
		m.handleIndirectPing(buf, from)
	case ackRespMsg:
		m.handleAck(buf, from, timestamp)
	case nackRespMsg:
		m.handleNack(buf, from)

	case suspectMsg:
		fallthrough
	case aliveMsg:
		fallthrough
	case deadMsg:
		fallthrough
	case userMsg:
		select {
		case m.handoff <- msgHandoff{msgType, buf, from}:
		default:
			m.logger.Printf("[WARN] memberlist: handler queue full, dropping message (%d) %s", msgType, LogAddress(from))
		}

	default:
		m.logger.Printf("[ERR] memberlist: msg type (%d) not supported %s", msgType, LogAddress(from))
	}
}

func (m *Memberlist) packetHandler() {
	for {
		select {
		case msg := <-m.handoff:
			msgType := msg.msgType
			buf := msg.buf
			from := msg.from

			switch msgType {
			case suspectMsg:
				m.handleSuspect(buf, from)
			case aliveMsg:
				m.handleAlive(buf, from)
			case deadMsg:
				m.handleDead(buf, from)
			case userMsg:
				m.handleUser(buf, from)
			default:
				m.logger.Printf("[ERR] memberlist: Message type (%d) not supported %s (packet handler)", msgType, LogAddress(from))
			}

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) handleCompound(buf []byte, from net.Addr, timestamp time.Time) {

	trunc, parts, err := decodeCompoundMessage(buf)
	if err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode compound request: %s %s", err, LogAddress(from))
		return
	}

	if trunc > 0 {
		m.logger.Printf("[WARN] memberlist: Compound request had %d truncated messages %s", trunc, LogAddress(from))
	}

	for _, part := range parts {
		m.handleCommand(part, from, timestamp)
	}
}

func (m *Memberlist) handlePing(buf []byte, from net.Addr) {
	var p ping
	if err := decode(buf, &p); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode ping request: %s %s", err, LogAddress(from))
		return
	}

	if p.Node != "" && p.Node != m.config.Name {
		m.logger.Printf("[WARN] memberlist: Got ping for unexpected node '%s' %s", p.Node, LogAddress(from))
		return
	}
	var ack ackResp
	ack.SeqNo = p.SeqNo
	if m.config.Ping != nil {
		ack.Payload = m.config.Ping.AckPayload()
	}
	if err := m.encodeAndSendMsg(from.String(), ackRespMsg, &ack); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to send ack: %s %s", err, LogAddress(from))
	}
}

func (m *Memberlist) handleIndirectPing(buf []byte, from net.Addr) {
	var ind indirectPingReq
	if err := decode(buf, &ind); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode indirect ping request: %s %s", err, LogAddress(from))
		return
	}

	if m.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(m.config.BindPort)
	}

	localSeqNo := m.nextSeqNo()
	ping := ping{SeqNo: localSeqNo, Node: ind.Node}

	cancelCh := make(chan struct{})
	respHandler := func(payload []byte, timestamp time.Time) {

		close(cancelCh)

		ack := ackResp{ind.SeqNo, nil}
		if err := m.encodeAndSendMsg(from.String(), ackRespMsg, &ack); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to forward ack: %s %s", err, LogAddress(from))
		}
	}
	m.setAckHandler(localSeqNo, respHandler, m.config.ProbeTimeout)

	addr := joinHostPort(net.IP(ind.Target).String(), ind.Port)
	if err := m.encodeAndSendMsg(addr, pingMsg, &ping); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to send ping: %s %s", err, LogAddress(from))
	}

	if ind.Nack {
		go func() {
			select {
			case <-cancelCh:
				return
			case <-time.After(m.config.ProbeTimeout):
				nack := nackResp{ind.SeqNo}
				if err := m.encodeAndSendMsg(from.String(), nackRespMsg, &nack); err != nil {
					m.logger.Printf("[ERR] memberlist: Failed to send nack: %s %s", err, LogAddress(from))
				}
			}
		}()
	}
}

func (m *Memberlist) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack ackResp
	if err := decode(buf, &ack); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode ack response: %s %s", err, LogAddress(from))
		return
	}
	m.invokeAckHandler(ack, timestamp)
}

func (m *Memberlist) handleNack(buf []byte, from net.Addr) {
	var nack nackResp
	if err := decode(buf, &nack); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode nack response: %s %s", err, LogAddress(from))
		return
	}
	m.invokeNackHandler(nack)
}

func (m *Memberlist) handleSuspect(buf []byte, from net.Addr) {
	var sus suspect
	if err := decode(buf, &sus); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode suspect message: %s %s", err, LogAddress(from))
		return
	}
	m.suspectNode(&sus)
}

func (m *Memberlist) handleAlive(buf []byte, from net.Addr) {
	var live alive
	if err := decode(buf, &live); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode alive message: %s %s", err, LogAddress(from))
		return
	}

	if m.ProtocolVersion() < 2 || live.Port == 0 {
		live.Port = uint16(m.config.BindPort)
	}

	m.aliveNode(&live, nil, false)
}

func (m *Memberlist) handleDead(buf []byte, from net.Addr) {
	var d dead
	if err := decode(buf, &d); err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decode dead message: %s %s", err, LogAddress(from))
		return
	}
	m.deadNode(&d)
}

func (m *Memberlist) handleUser(buf []byte, from net.Addr) {
	d := m.config.Delegate
	if d != nil {
		d.NotifyMsg(buf)
	}
}

func (m *Memberlist) handleCompressed(buf []byte, from net.Addr, timestamp time.Time) {

	payload, err := decompressPayload(buf)
	if err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to decompress payload: %v %s", err, LogAddress(from))
		return
	}

	m.handleCommand(payload, from, timestamp)
}

func (m *Memberlist) encodeAndSendMsg(addr string, msgType messageType, msg interface{}) error {
	out, err := encode(msgType, msg)
	if err != nil {
		return err
	}
	if err := m.sendMsg(addr, out.Bytes()); err != nil {
		return err
	}
	return nil
}

func (m *Memberlist) sendMsg(addr string, msg []byte) error {

	bytesAvail := m.config.UDPBufferSize - len(msg) - compoundHeaderOverhead
	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {
		bytesAvail -= encryptOverhead(m.encryptionVersion())
	}
	extra := m.getBroadcasts(compoundOverhead, bytesAvail)

	if len(extra) == 0 {
		return m.rawSendMsgPacket(addr, nil, msg)
	}

	msgs := make([][]byte, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	compound := makeCompoundMessage(msgs)

	return m.rawSendMsgPacket(addr, nil, compound.Bytes())
}

func (m *Memberlist) rawSendMsgPacket(addr string, node *Node, msg []byte) error {

	if m.config.EnableCompression {
		buf, err := compressPayload(msg)
		if err != nil {
			m.logger.Printf("[WARN] memberlist: Failed to compress payload: %v", err)
		} else {

			if buf.Len() < len(msg) {
				msg = buf.Bytes()
			}
		}
	}

	if node == nil {
		toAddr, _, err := net.SplitHostPort(addr)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to parse address %q: %v", addr, err)
			return err
		}
		m.nodeLock.RLock()
		nodeState, ok := m.nodeMap[toAddr]
		m.nodeLock.RUnlock()
		if ok {
			node = &nodeState.Node
		}
	}

	if node != nil && node.PMax >= 5 {
		crc := crc32.ChecksumIEEE(msg)
		header := make([]byte, 5, 5+len(msg))
		header[0] = byte(hasCrcMsg)
		binary.BigEndian.PutUint32(header[1:], crc)
		msg = append(header, msg...)
	}

	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {

		var buf bytes.Buffer
		primaryKey := m.config.Keyring.GetPrimaryKey()
		err := encryptPayload(m.encryptionVersion(), primaryKey, msg, nil, &buf)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Encryption of message failed: %v", err)
			return err
		}
		msg = buf.Bytes()
	}

	metrics.IncrCounter([]string{"memberlist", "udp", "sent"}, float32(len(msg)))
	_, err := m.transport.WriteTo(msg, addr)
	return err
}

func (m *Memberlist) rawSendMsgStream(conn net.Conn, sendBuf []byte) error {

	if m.config.EnableCompression {
		compBuf, err := compressPayload(sendBuf)
		if err != nil {
			m.logger.Printf("[ERROR] memberlist: Failed to compress payload: %v", err)
		} else {
			sendBuf = compBuf.Bytes()
		}
	}

	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {
		crypt, err := m.encryptLocalState(sendBuf)
		if err != nil {
			m.logger.Printf("[ERROR] memberlist: Failed to encrypt local state: %v", err)
			return err
		}
		sendBuf = crypt
	}

	metrics.IncrCounter([]string{"memberlist", "tcp", "sent"}, float32(len(sendBuf)))

	if n, err := conn.Write(sendBuf); err != nil {
		return err
	} else if n != len(sendBuf) {
		return fmt.Errorf("only %d of %d bytes written", n, len(sendBuf))
	}

	return nil
}

func (m *Memberlist) sendUserMsg(addr string, sendBuf []byte) error {
	conn, err := m.transport.DialTimeout(addr, m.config.TCPTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	bufConn := bytes.NewBuffer(nil)
	if err := bufConn.WriteByte(byte(userMsg)); err != nil {
		return err
	}

	header := userMsgHeader{UserMsgLen: len(sendBuf)}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)
	if err := enc.Encode(&header); err != nil {
		return err
	}
	if _, err := bufConn.Write(sendBuf); err != nil {
		return err
	}
	return m.rawSendMsgStream(conn, bufConn.Bytes())
}

func (m *Memberlist) sendAndReceiveState(addr string, join bool) ([]pushNodeState, []byte, error) {

	conn, err := m.transport.DialTimeout(addr, m.config.TCPTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	m.logger.Printf("[DEBUG] memberlist: Initiating push/pull sync with: %s", conn.RemoteAddr())
	metrics.IncrCounter([]string{"memberlist", "tcp", "connect"}, 1)

	if err := m.sendLocalState(conn, join); err != nil {
		return nil, nil, err
	}

	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))
	msgType, bufConn, dec, err := m.readStream(conn)
	if err != nil {
		return nil, nil, err
	}

	if msgType == errMsg {
		var resp errResp
		if err := dec.Decode(&resp); err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("remote error: %v", resp.Error)
	}

	if msgType != pushPullMsg {
		err := fmt.Errorf("received invalid msgType (%d), expected pushPullMsg (%d) %s", msgType, pushPullMsg, LogConn(conn))
		return nil, nil, err
	}

	_, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
	return remoteNodes, userState, err
}

func (m *Memberlist) sendLocalState(conn net.Conn, join bool) error {

	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))

	m.nodeLock.RLock()
	localNodes := make([]pushNodeState, len(m.nodes))
	for idx, n := range m.nodes {
		localNodes[idx].Name = n.Name
		localNodes[idx].Addr = n.Addr
		localNodes[idx].Port = n.Port
		localNodes[idx].Incarnation = n.Incarnation
		localNodes[idx].State = n.State
		localNodes[idx].Meta = n.Meta
		localNodes[idx].Vsn = []uint8{
			n.PMin, n.PMax, n.PCur,
			n.DMin, n.DMax, n.DCur,
		}
	}
	m.nodeLock.RUnlock()

	var userData []byte
	if m.config.Delegate != nil {
		userData = m.config.Delegate.LocalState(join)
	}

	bufConn := bytes.NewBuffer(nil)

	header := pushPullHeader{Nodes: len(localNodes), UserStateLen: len(userData), Join: join}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)

	if _, err := bufConn.Write([]byte{byte(pushPullMsg)}); err != nil {
		return err
	}

	if err := enc.Encode(&header); err != nil {
		return err
	}
	for i := 0; i < header.Nodes; i++ {
		if err := enc.Encode(&localNodes[i]); err != nil {
			return err
		}
	}

	if userData != nil {
		if _, err := bufConn.Write(userData); err != nil {
			return err
		}
	}

	return m.rawSendMsgStream(conn, bufConn.Bytes())
}

func (m *Memberlist) encryptLocalState(sendBuf []byte) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(byte(encryptMsg))

	sizeBuf := make([]byte, 4)
	encVsn := m.encryptionVersion()
	encLen := encryptedLength(encVsn, len(sendBuf))
	binary.BigEndian.PutUint32(sizeBuf, uint32(encLen))
	buf.Write(sizeBuf)

	key := m.config.Keyring.GetPrimaryKey()
	err := encryptPayload(encVsn, key, sendBuf, buf.Bytes()[:5], &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Memberlist) decryptRemoteState(bufConn io.Reader) ([]byte, error) {

	cipherText := bytes.NewBuffer(nil)
	cipherText.WriteByte(byte(encryptMsg))
	_, err := io.CopyN(cipherText, bufConn, 4)
	if err != nil {
		return nil, err
	}

	moreBytes := binary.BigEndian.Uint32(cipherText.Bytes()[1:5])
	if moreBytes > maxPushStateBytes {
		return nil, fmt.Errorf("Remote node state is larger than limit (%d)", moreBytes)
	}

	_, err = io.CopyN(cipherText, bufConn, int64(moreBytes))
	if err != nil {
		return nil, err
	}

	dataBytes := cipherText.Bytes()[:5]
	cipherBytes := cipherText.Bytes()[5:]

	keys := m.config.Keyring.GetKeys()
	return decryptPayload(keys, cipherBytes, dataBytes)
}

func (m *Memberlist) readStream(conn net.Conn) (messageType, io.Reader, *codec.Decoder, error) {

	var bufConn io.Reader = bufio.NewReader(conn)

	buf := [1]byte{0}
	if _, err := bufConn.Read(buf[:]); err != nil {
		return 0, nil, nil, err
	}
	msgType := messageType(buf[0])

	if msgType == encryptMsg {
		if !m.config.EncryptionEnabled() {
			return 0, nil, nil,
				fmt.Errorf("Remote state is encrypted and encryption is not configured")
		}

		plain, err := m.decryptRemoteState(bufConn)
		if err != nil {
			return 0, nil, nil, err
		}

		msgType = messageType(plain[0])
		bufConn = bytes.NewReader(plain[1:])
	} else if m.config.EncryptionEnabled() && m.config.GossipVerifyIncoming {
		return 0, nil, nil,
			fmt.Errorf("Encryption is configured but remote state is not encrypted")
	}

	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(bufConn, &hd)

	if msgType == compressMsg {
		var c compress
		if err := dec.Decode(&c); err != nil {
			return 0, nil, nil, err
		}
		decomp, err := decompressBuffer(&c)
		if err != nil {
			return 0, nil, nil, err
		}

		msgType = messageType(decomp[0])

		bufConn = bytes.NewReader(decomp[1:])

		dec = codec.NewDecoder(bufConn, &hd)
	}

	return msgType, bufConn, dec, nil
}

func (m *Memberlist) readRemoteState(bufConn io.Reader, dec *codec.Decoder) (bool, []pushNodeState, []byte, error) {

	var header pushPullHeader
	if err := dec.Decode(&header); err != nil {
		return false, nil, nil, err
	}

	remoteNodes := make([]pushNodeState, header.Nodes)

	for i := 0; i < header.Nodes; i++ {
		if err := dec.Decode(&remoteNodes[i]); err != nil {
			return false, nil, nil, err
		}
	}

	var userBuf []byte
	if header.UserStateLen > 0 {
		userBuf = make([]byte, header.UserStateLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserStateLen)
		if err == nil && bytes != header.UserStateLen {
			err = fmt.Errorf(
				"Failed to read full user state (%d / %d)",
				bytes, header.UserStateLen)
		}
		if err != nil {
			return false, nil, nil, err
		}
	}

	for idx := range remoteNodes {
		if m.ProtocolVersion() < 2 || remoteNodes[idx].Port == 0 {
			remoteNodes[idx].Port = uint16(m.config.BindPort)
		}
	}

	return header.Join, remoteNodes, userBuf, nil
}

func (m *Memberlist) mergeRemoteState(join bool, remoteNodes []pushNodeState, userBuf []byte) error {
	if err := m.verifyProtocol(remoteNodes); err != nil {
		return err
	}

	if join && m.config.Merge != nil {
		nodes := make([]*Node, len(remoteNodes))
		for idx, n := range remoteNodes {
			nodes[idx] = &Node{
				Name: n.Name,
				Addr: n.Addr,
				Port: n.Port,
				Meta: n.Meta,
				PMin: n.Vsn[0],
				PMax: n.Vsn[1],
				PCur: n.Vsn[2],
				DMin: n.Vsn[3],
				DMax: n.Vsn[4],
				DCur: n.Vsn[5],
			}
		}
		if err := m.config.Merge.NotifyMerge(nodes); err != nil {
			return err
		}
	}

	m.mergeState(remoteNodes)

	if userBuf != nil && m.config.Delegate != nil {
		m.config.Delegate.MergeRemoteState(userBuf, join)
	}
	return nil
}

func (m *Memberlist) readUserMsg(bufConn io.Reader, dec *codec.Decoder) error {

	var header userMsgHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	var userBuf []byte
	if header.UserMsgLen > 0 {
		userBuf = make([]byte, header.UserMsgLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserMsgLen)
		if err == nil && bytes != header.UserMsgLen {
			err = fmt.Errorf(
				"Failed to read full user message (%d / %d)",
				bytes, header.UserMsgLen)
		}
		if err != nil {
			return err
		}

		d := m.config.Delegate
		if d != nil {
			d.NotifyMsg(userBuf)
		}
	}

	return nil
}

func (m *Memberlist) sendPingAndWaitForAck(addr string, ping ping, deadline time.Time) (bool, error) {
	conn, err := m.transport.DialTimeout(addr, m.config.TCPTimeout)
	if err != nil {

		return false, nil
	}
	defer conn.Close()
	conn.SetDeadline(deadline)

	out, err := encode(pingMsg, &ping)
	if err != nil {
		return false, err
	}

	if err = m.rawSendMsgStream(conn, out.Bytes()); err != nil {
		return false, err
	}

	msgType, _, dec, err := m.readStream(conn)
	if err != nil {
		return false, err
	}

	if msgType != ackRespMsg {
		return false, fmt.Errorf("Unexpected msgType (%d) from ping %s", msgType, LogConn(conn))
	}

	var ack ackResp
	if err = dec.Decode(&ack); err != nil {
		return false, err
	}

	if ack.SeqNo != ping.SeqNo {
		return false, fmt.Errorf("Sequence number from ack (%d) doesn't match ping (%d) %v", ack.SeqNo, ping.SeqNo, LogConn(conn))
	}

	return true, nil
}
