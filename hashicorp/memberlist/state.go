package memberlist

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
)

type nodeStateType int

const (
	stateAlive nodeStateType = iota
	stateSuspect
	stateDead
)

type Node struct {
	Name string
	Addr net.IP
	Port uint16
	Meta []byte // Metadata from the delegate for this node.
	PMin uint8  // Minimum protocol version this understands
	PMax uint8  // Maximum protocol version this understands
	PCur uint8  // Current version node is speaking
	DMin uint8  // Min protocol version for the delegate to understand
	DMax uint8  // Max protocol version for the delegate to understand
	DCur uint8  // Current version delegate is speaking
}

func (n *Node) Address() string {
	return joinHostPort(n.Addr.String(), n.Port)
}

func (n *Node) String() string {
	return n.Name
}

type nodeState struct {
	Node
	Incarnation uint32        // Last known incarnation number
	State       nodeStateType // Current state
	StateChange time.Time     // Time last state change happened
}

func (n *nodeState) Address() string {
	return n.Node.Address()
}

type ackHandler struct {
	ackFn  func([]byte, time.Time)
	nackFn func()
	timer  *time.Timer
}

type NoPingResponseError struct {
	node string
}

func (f NoPingResponseError) Error() string {
	return fmt.Sprintf("No response from node %s", f.node)
}

func (m *Memberlist) schedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()

	if len(m.tickers) > 0 {
		return
	}

	stopCh := make(chan struct{})

	if m.config.ProbeInterval > 0 {
		t := time.NewTicker(m.config.ProbeInterval)
		go m.triggerFunc(m.config.ProbeInterval, t.C, stopCh, m.probe)
		m.tickers = append(m.tickers, t)
	}

	if m.config.PushPullInterval > 0 {
		go m.pushPullTrigger(stopCh)
	}

	if m.config.GossipInterval > 0 && m.config.GossipNodes > 0 {
		t := time.NewTicker(m.config.GossipInterval)
		go m.triggerFunc(m.config.GossipInterval, t.C, stopCh, m.gossip)
		m.tickers = append(m.tickers, t)
	}

	if len(m.tickers) > 0 {
		m.stopTick = stopCh
	}
}

func (m *Memberlist) triggerFunc(stagger time.Duration, C <-chan time.Time, stop <-chan struct{}, f func()) {

	randStagger := time.Duration(uint64(rand.Int63()) % uint64(stagger))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}
	for {
		select {
		case <-C:
			f()
		case <-stop:
			return
		}
	}
}

func (m *Memberlist) pushPullTrigger(stop <-chan struct{}) {
	interval := m.config.PushPullInterval

	randStagger := time.Duration(uint64(rand.Int63()) % uint64(interval))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}

	for {
		tickTime := pushPullScale(interval, m.estNumNodes())
		select {
		case <-time.After(tickTime):
			m.pushPull()
		case <-stop:
			return
		}
	}
}

func (m *Memberlist) deschedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()

	if len(m.tickers) == 0 {
		return
	}

	close(m.stopTick)

	for _, t := range m.tickers {
		t.Stop()
	}
	m.tickers = nil
}

func (m *Memberlist) probe() {

	numCheck := 0
START:
	m.nodeLock.RLock()

	if numCheck >= len(m.nodes) {
		m.nodeLock.RUnlock()
		return
	}

	if m.probeIndex >= len(m.nodes) {
		m.nodeLock.RUnlock()
		m.resetNodes()
		m.probeIndex = 0
		numCheck++
		goto START
	}

	skip := false
	var node nodeState

	node = *m.nodes[m.probeIndex]
	if node.Name == m.config.Name {
		skip = true
	} else if node.State == stateDead {
		skip = true
	}

	m.nodeLock.RUnlock()
	m.probeIndex++
	if skip {
		numCheck++
		goto START
	}

	m.probeNode(&node)
}

func (m *Memberlist) probeNode(node *nodeState) {
	defer metrics.MeasureSince([]string{"memberlist", "probeNode"}, time.Now())

	probeInterval := m.awareness.ScaleTimeout(m.config.ProbeInterval)
	if probeInterval > m.config.ProbeInterval {
		metrics.IncrCounter([]string{"memberlist", "degraded", "probe"}, 1)
	}

	ping := ping{SeqNo: m.nextSeqNo(), Node: node.Name}
	ackCh := make(chan ackMessage, m.config.IndirectChecks+1)
	nackCh := make(chan struct{}, m.config.IndirectChecks+1)
	m.setProbeChannels(ping.SeqNo, ackCh, nackCh, probeInterval)

	sent := time.Now()

	deadline := sent.Add(probeInterval)
	addr := node.Address()
	if node.State == stateAlive {
		if err := m.encodeAndSendMsg(addr, pingMsg, &ping); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to send ping: %s", err)
			return
		}
	} else {
		var msgs [][]byte
		if buf, err := encode(pingMsg, &ping); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to encode ping message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}
		s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.config.Name}
		if buf, err := encode(suspectMsg, &s); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to encode suspect message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}

		compound := makeCompoundMessage(msgs)
		if err := m.rawSendMsgPacket(addr, &node.Node, compound.Bytes()); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to send compound ping and suspect message to %s: %s", addr, err)
			return
		}
	}

	awarenessDelta := -1
	defer func() {
		m.awareness.ApplyDelta(awarenessDelta)
	}()

	select {
	case v := <-ackCh:
		if v.Complete == true {
			if m.config.Ping != nil {
				rtt := v.Timestamp.Sub(sent)
				m.config.Ping.NotifyPingComplete(&node.Node, rtt, v.Payload)
			}
			return
		}

		if v.Complete == false {
			ackCh <- v
		}
	case <-time.After(m.config.ProbeTimeout):

		m.logger.Printf("[DEBUG] memberlist: Failed ping: %v (timeout reached)", node.Name)
	}

	m.nodeLock.RLock()
	kNodes := kRandomNodes(m.config.IndirectChecks, m.nodes, func(n *nodeState) bool {
		return n.Name == m.config.Name ||
			n.Name == node.Name ||
			n.State != stateAlive
	})
	m.nodeLock.RUnlock()

	expectedNacks := 0
	ind := indirectPingReq{SeqNo: ping.SeqNo, Target: node.Addr, Port: node.Port, Node: node.Name}
	for _, peer := range kNodes {

		if ind.Nack = peer.PMax >= 4; ind.Nack {
			expectedNacks++
		}

		if err := m.encodeAndSendMsg(peer.Address(), indirectPingMsg, &ind); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to send indirect ping: %s", err)
		}
	}

	//

	fallbackCh := make(chan bool, 1)
	if (!m.config.DisableTcpPings) && (node.PMax >= 3) {
		go func() {
			defer close(fallbackCh)
			didContact, err := m.sendPingAndWaitForAck(node.Address(), ping, deadline)
			if err != nil {
				m.logger.Printf("[ERR] memberlist: Failed fallback ping: %s", err)
			} else {
				fallbackCh <- didContact
			}
		}()
	} else {
		close(fallbackCh)
	}

	select {
	case v := <-ackCh:
		if v.Complete == true {
			return
		}
	}

	for didContact := range fallbackCh {
		if didContact {
			m.logger.Printf("[WARN] memberlist: Was able to connect to %s but other probes failed, network may be misconfigured", node.Name)
			return
		}
	}

	awarenessDelta = 0
	if expectedNacks > 0 {
		if nackCount := len(nackCh); nackCount < expectedNacks {
			awarenessDelta += (expectedNacks - nackCount)
		}
	} else {
		awarenessDelta += 1
	}

	m.logger.Printf("[INFO] memberlist: Suspect %s has failed, no acks received", node.Name)
	s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: m.config.Name}
	m.suspectNode(&s)
}

func (m *Memberlist) Ping(node string, addr net.Addr) (time.Duration, error) {

	ping := ping{SeqNo: m.nextSeqNo(), Node: node}
	ackCh := make(chan ackMessage, m.config.IndirectChecks+1)
	m.setProbeChannels(ping.SeqNo, ackCh, nil, m.config.ProbeInterval)

	if err := m.encodeAndSendMsg(addr.String(), pingMsg, &ping); err != nil {
		return 0, err
	}

	sent := time.Now()

	select {
	case v := <-ackCh:
		if v.Complete == true {
			return v.Timestamp.Sub(sent), nil
		}
	case <-time.After(m.config.ProbeTimeout):

	}

	m.logger.Printf("[DEBUG] memberlist: Failed UDP ping: %v (timeout reached)", node)
	return 0, NoPingResponseError{ping.Node}
}

func (m *Memberlist) resetNodes() {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	deadIdx := moveDeadNodes(m.nodes, m.config.GossipToTheDeadTime)

	for i := deadIdx; i < len(m.nodes); i++ {
		delete(m.nodeMap, m.nodes[i].Name)
		m.nodes[i] = nil
	}

	m.nodes = m.nodes[0:deadIdx]

	atomic.StoreUint32(&m.numNodes, uint32(deadIdx))

	shuffleNodes(m.nodes)
}

func (m *Memberlist) gossip() {
	defer metrics.MeasureSince([]string{"memberlist", "gossip"}, time.Now())

	m.nodeLock.RLock()
	kNodes := kRandomNodes(m.config.GossipNodes, m.nodes, func(n *nodeState) bool {
		if n.Name == m.config.Name {
			return true
		}

		switch n.State {
		case stateAlive, stateSuspect:
			return false

		case stateDead:
			return time.Since(n.StateChange) > m.config.GossipToTheDeadTime

		default:
			return true
		}
	})
	m.nodeLock.RUnlock()

	bytesAvail := m.config.UDPBufferSize - compoundHeaderOverhead
	if m.config.EncryptionEnabled() {
		bytesAvail -= encryptOverhead(m.encryptionVersion())
	}

	for _, node := range kNodes {

		msgs := m.getBroadcasts(compoundOverhead, bytesAvail)
		if len(msgs) == 0 {
			return
		}

		addr := node.Address()
		if len(msgs) == 1 {

			if err := m.rawSendMsgPacket(addr, &node.Node, msgs[0]); err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to send gossip to %s: %s", addr, err)
			}
		} else {

			compound := makeCompoundMessage(msgs)
			if err := m.rawSendMsgPacket(addr, &node.Node, compound.Bytes()); err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to send gossip to %s: %s", addr, err)
			}
		}
	}
}

func (m *Memberlist) pushPull() {

	m.nodeLock.RLock()
	nodes := kRandomNodes(1, m.nodes, func(n *nodeState) bool {
		return n.Name == m.config.Name ||
			n.State != stateAlive
	})
	m.nodeLock.RUnlock()

	if len(nodes) == 0 {
		return
	}
	node := nodes[0]

	if err := m.pushPullNode(node.Address(), false); err != nil {
		m.logger.Printf("[ERR] memberlist: Push/Pull with %s failed: %s", node.Name, err)
	}
}

func (m *Memberlist) pushPullNode(addr string, join bool) error {
	defer metrics.MeasureSince([]string{"memberlist", "pushPullNode"}, time.Now())

	remote, userState, err := m.sendAndReceiveState(addr, join)
	if err != nil {
		return err
	}

	if err := m.mergeRemoteState(join, remote, userState); err != nil {
		return err
	}
	return nil
}

//
//
func (m *Memberlist) verifyProtocol(remote []pushNodeState) error {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	var maxpmin, minpmax uint8
	var maxdmin, mindmax uint8
	minpmax = math.MaxUint8
	mindmax = math.MaxUint8

	for _, rn := range remote {

		if rn.State != stateAlive {
			continue
		}

		if len(rn.Vsn) == 0 {
			continue
		}

		if rn.Vsn[0] > maxpmin {
			maxpmin = rn.Vsn[0]
		}

		if rn.Vsn[1] < minpmax {
			minpmax = rn.Vsn[1]
		}

		if rn.Vsn[3] > maxdmin {
			maxdmin = rn.Vsn[3]
		}

		if rn.Vsn[4] < mindmax {
			mindmax = rn.Vsn[4]
		}
	}

	for _, n := range m.nodes {

		if n.State != stateAlive {
			continue
		}

		if n.PMin > maxpmin {
			maxpmin = n.PMin
		}

		if n.PMax < minpmax {
			minpmax = n.PMax
		}

		if n.DMin > maxdmin {
			maxdmin = n.DMin
		}

		if n.DMax < mindmax {
			mindmax = n.DMax
		}
	}

	for _, n := range remote {
		var nPCur, nDCur uint8
		if len(n.Vsn) > 0 {
			nPCur = n.Vsn[2]
			nDCur = n.Vsn[5]
		}

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf(
				"Node '%s' protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf(
				"Node '%s' delegate protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nDCur, maxdmin, mindmax)
		}
	}

	for _, n := range m.nodes {
		nPCur := n.PCur
		nDCur := n.DCur

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf(
				"Node '%s' protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf(
				"Node '%s' delegate protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nDCur, maxdmin, mindmax)
		}
	}

	return nil
}

func (m *Memberlist) nextSeqNo() uint32 {
	return atomic.AddUint32(&m.sequenceNum, 1)
}

func (m *Memberlist) nextIncarnation() uint32 {
	return atomic.AddUint32(&m.incarnation, 1)
}

func (m *Memberlist) skipIncarnation(offset uint32) uint32 {
	return atomic.AddUint32(&m.incarnation, offset)
}

func (m *Memberlist) estNumNodes() int {
	return int(atomic.LoadUint32(&m.numNodes))
}

type ackMessage struct {
	Complete  bool
	Payload   []byte
	Timestamp time.Time
}

func (m *Memberlist) setProbeChannels(seqNo uint32, ackCh chan ackMessage, nackCh chan struct{}, timeout time.Duration) {

	ackFn := func(payload []byte, timestamp time.Time) {
		select {
		case ackCh <- ackMessage{true, payload, timestamp}:
		default:
		}
	}
	nackFn := func() {
		select {
		case nackCh <- struct{}{}:
		default:
		}
	}

	ah := &ackHandler{ackFn, nackFn, nil}
	m.ackLock.Lock()
	m.ackHandlers[seqNo] = ah
	m.ackLock.Unlock()

	ah.timer = time.AfterFunc(timeout, func() {
		m.ackLock.Lock()
		delete(m.ackHandlers, seqNo)
		m.ackLock.Unlock()
		select {
		case ackCh <- ackMessage{false, nil, time.Now()}:
		default:
		}
	})
}

func (m *Memberlist) setAckHandler(seqNo uint32, ackFn func([]byte, time.Time), timeout time.Duration) {

	ah := &ackHandler{ackFn, nil, nil}
	m.ackLock.Lock()
	m.ackHandlers[seqNo] = ah
	m.ackLock.Unlock()

	ah.timer = time.AfterFunc(timeout, func() {
		m.ackLock.Lock()
		delete(m.ackHandlers, seqNo)
		m.ackLock.Unlock()
	})
}

func (m *Memberlist) invokeAckHandler(ack ackResp, timestamp time.Time) {
	m.ackLock.Lock()
	ah, ok := m.ackHandlers[ack.SeqNo]
	delete(m.ackHandlers, ack.SeqNo)
	m.ackLock.Unlock()
	if !ok {
		return
	}
	ah.timer.Stop()
	ah.ackFn(ack.Payload, timestamp)
}

func (m *Memberlist) invokeNackHandler(nack nackResp) {
	m.ackLock.Lock()
	ah, ok := m.ackHandlers[nack.SeqNo]
	m.ackLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	ah.nackFn()
}

func (m *Memberlist) refute(me *nodeState, accusedInc uint32) {

	inc := m.nextIncarnation()
	if accusedInc >= inc {
		inc = m.skipIncarnation(accusedInc - inc + 1)
	}
	me.Incarnation = inc

	m.awareness.ApplyDelta(1)

	a := alive{
		Incarnation: inc,
		Node:        me.Name,
		Addr:        me.Addr,
		Port:        me.Port,
		Meta:        me.Meta,
		Vsn: []uint8{
			me.PMin, me.PMax, me.PCur,
			me.DMin, me.DMax, me.DCur,
		},
	}
	m.encodeAndBroadcast(me.Addr.String(), aliveMsg, a)
}

func (m *Memberlist) aliveNode(a *alive, notify chan struct{}, bootstrap bool) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[a.Node]

	if m.hasLeft() && a.Node == m.config.Name {
		return
	}

	if m.config.Alive != nil {
		node := &Node{
			Name: a.Node,
			Addr: a.Addr,
			Port: a.Port,
			Meta: a.Meta,
			PMin: a.Vsn[0],
			PMax: a.Vsn[1],
			PCur: a.Vsn[2],
			DMin: a.Vsn[3],
			DMax: a.Vsn[4],
			DCur: a.Vsn[5],
		}
		if err := m.config.Alive.NotifyAlive(node); err != nil {
			m.logger.Printf("[WARN] memberlist: ignoring alive message for '%s': %s",
				a.Node, err)
			return
		}
	}

	if !ok {
		state = &nodeState{
			Node: Node{
				Name: a.Node,
				Addr: a.Addr,
				Port: a.Port,
				Meta: a.Meta,
			},
			State: stateDead,
		}

		m.nodeMap[a.Node] = state

		n := len(m.nodes)
		offset := randomOffset(n)

		m.nodes = append(m.nodes, state)
		m.nodes[offset], m.nodes[n] = m.nodes[n], m.nodes[offset]

		atomic.AddUint32(&m.numNodes, 1)
	}

	if !bytes.Equal([]byte(state.Addr), a.Addr) || state.Port != a.Port {
		m.logger.Printf("[ERR] memberlist: Conflicting address for %s. Mine: %v:%d Theirs: %v:%d",
			state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port)

		if m.config.Conflict != nil {
			other := Node{
				Name: a.Node,
				Addr: a.Addr,
				Port: a.Port,
				Meta: a.Meta,
			}
			m.config.Conflict.NotifyConflict(&state.Node, &other)
		}
		return
	}

	isLocalNode := state.Name == m.config.Name
	if a.Incarnation <= state.Incarnation && !isLocalNode {
		return
	}

	if a.Incarnation < state.Incarnation && isLocalNode {
		return
	}

	delete(m.nodeTimers, a.Node)

	oldState := state.State
	oldMeta := state.Meta

	if !bootstrap && isLocalNode {

		versions := []uint8{
			state.PMin, state.PMax, state.PCur,
			state.DMin, state.DMax, state.DCur,
		}

		//

		//
		if a.Incarnation == state.Incarnation &&
			bytes.Equal(a.Meta, state.Meta) &&
			bytes.Equal(a.Vsn, versions) {
			return
		}

		m.refute(state, a.Incarnation)
		m.logger.Printf("[WARN] memberlist: Refuting an alive message")
	} else {
		m.encodeBroadcastNotify(a.Node, aliveMsg, a, notify)

		if len(a.Vsn) > 0 {
			state.PMin = a.Vsn[0]
			state.PMax = a.Vsn[1]
			state.PCur = a.Vsn[2]
			state.DMin = a.Vsn[3]
			state.DMax = a.Vsn[4]
			state.DCur = a.Vsn[5]
		}

		state.Incarnation = a.Incarnation
		state.Meta = a.Meta
		if state.State != stateAlive {
			state.State = stateAlive
			state.StateChange = time.Now()
		}
	}

	metrics.IncrCounter([]string{"memberlist", "msg", "alive"}, 1)

	if m.config.Events != nil {
		if oldState == stateDead {

			m.config.Events.NotifyJoin(&state.Node)

		} else if !bytes.Equal(oldMeta, state.Meta) {

			m.config.Events.NotifyUpdate(&state.Node)
		}
	}
}

func (m *Memberlist) suspectNode(s *suspect) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[s.Node]

	if !ok {
		return
	}

	if s.Incarnation < state.Incarnation {
		return
	}

	if timer, ok := m.nodeTimers[s.Node]; ok {
		if timer.Confirm(s.From) {
			m.encodeAndBroadcast(s.Node, suspectMsg, s)
		}
		return
	}

	if state.State != stateAlive {
		return
	}

	if state.Name == m.config.Name {
		m.refute(state, s.Incarnation)
		m.logger.Printf("[WARN] memberlist: Refuting a suspect message (from: %s)", s.From)
		return // Do not mark ourself suspect
	} else {
		m.encodeAndBroadcast(s.Node, suspectMsg, s)
	}

	metrics.IncrCounter([]string{"memberlist", "msg", "suspect"}, 1)

	state.Incarnation = s.Incarnation
	state.State = stateSuspect
	changeTime := time.Now()
	state.StateChange = changeTime

	k := m.config.SuspicionMult - 2

	n := m.estNumNodes()
	if n-2 < k {
		k = 0
	}

	min := suspicionTimeout(m.config.SuspicionMult, n, m.config.ProbeInterval)
	max := time.Duration(m.config.SuspicionMaxTimeoutMult) * min
	fn := func(numConfirmations int) {
		m.nodeLock.Lock()
		state, ok := m.nodeMap[s.Node]
		timeout := ok && state.State == stateSuspect && state.StateChange == changeTime
		m.nodeLock.Unlock()

		if timeout {
			if k > 0 && numConfirmations < k {
				metrics.IncrCounter([]string{"memberlist", "degraded", "timeout"}, 1)
			}

			m.logger.Printf("[INFO] memberlist: Marking %s as failed, suspect timeout reached (%d peer confirmations)",
				state.Name, numConfirmations)
			d := dead{Incarnation: state.Incarnation, Node: state.Name, From: m.config.Name}
			m.deadNode(&d)
		}
	}
	m.nodeTimers[s.Node] = newSuspicion(s.From, k, min, max, fn)
}

func (m *Memberlist) deadNode(d *dead) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	state, ok := m.nodeMap[d.Node]

	if !ok {
		return
	}

	if d.Incarnation < state.Incarnation {
		return
	}

	delete(m.nodeTimers, d.Node)

	if state.State == stateDead {
		return
	}

	if state.Name == m.config.Name {

		if !m.hasLeft() {
			m.refute(state, d.Incarnation)
			m.logger.Printf("[WARN] memberlist: Refuting a dead message (from: %s)", d.From)
			return // Do not mark ourself dead
		}

		m.encodeBroadcastNotify(d.Node, deadMsg, d, m.leaveBroadcast)
	} else {
		m.encodeAndBroadcast(d.Node, deadMsg, d)
	}

	metrics.IncrCounter([]string{"memberlist", "msg", "dead"}, 1)

	state.Incarnation = d.Incarnation
	state.State = stateDead
	state.StateChange = time.Now()

	if m.config.Events != nil {
		m.config.Events.NotifyLeave(&state.Node)
	}
}

func (m *Memberlist) mergeState(remote []pushNodeState) {
	for _, r := range remote {
		switch r.State {
		case stateAlive:
			a := alive{
				Incarnation: r.Incarnation,
				Node:        r.Name,
				Addr:        r.Addr,
				Port:        r.Port,
				Meta:        r.Meta,
				Vsn:         r.Vsn,
			}
			m.aliveNode(&a, nil, false)

		case stateDead:

			fallthrough
		case stateSuspect:
			s := suspect{Incarnation: r.Incarnation, Node: r.Name, From: m.config.Name}
			m.suspectNode(&s)
		}
	}
}
