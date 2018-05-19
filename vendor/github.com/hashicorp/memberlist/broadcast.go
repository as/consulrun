package memberlist

/*
The broadcast mechanism works by maintaining a sorted list of messages to be
sent out. When a message is to be broadcast, the retransmit count
is set to zero and appended to the queue. The retransmit count serves
as the "priority", ensuring that newer messages get sent first. Once
a message hits the retransmit limit, it is removed from the queue.

Additionally, older entries can be invalidated by new messages that
are contradictory. For example, if we send "{suspect M1 inc: 1},
then a following {alive M1 inc: 2} will invalidate that message
*/

type memberlistBroadcast struct {
	node   string
	msg    []byte
	notify chan struct{}
}

func (b *memberlistBroadcast) Invalidates(other Broadcast) bool {

	mb, ok := other.(*memberlistBroadcast)
	if !ok {
		return false
	}

	return b.node == mb.node
}

func (b *memberlistBroadcast) Message() []byte {
	return b.msg
}

func (b *memberlistBroadcast) Finished() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (m *Memberlist) encodeAndBroadcast(node string, msgType messageType, msg interface{}) {
	m.encodeBroadcastNotify(node, msgType, msg, nil)
}

func (m *Memberlist) encodeBroadcastNotify(node string, msgType messageType, msg interface{}, notify chan struct{}) {
	buf, err := encode(msgType, msg)
	if err != nil {
		m.logger.Printf("[ERR] memberlist: Failed to encode message for broadcast: %s", err)
	} else {
		m.queueBroadcast(node, buf.Bytes(), notify)
	}
}

func (m *Memberlist) queueBroadcast(node string, msg []byte, notify chan struct{}) {
	b := &memberlistBroadcast{node, msg, notify}
	m.broadcasts.QueueBroadcast(b)
}

func (m *Memberlist) getBroadcasts(overhead, limit int) [][]byte {

	toSend := m.broadcasts.GetBroadcasts(overhead, limit)

	d := m.config.Delegate
	if d != nil {

		bytesUsed := 0
		for _, msg := range toSend {
			bytesUsed += len(msg) + overhead
		}

		avail := limit - bytesUsed
		if avail > overhead+userMsgOverhead {
			userMsgs := d.GetBroadcasts(overhead+userMsgOverhead, avail)

			for _, msg := range userMsgs {
				buf := make([]byte, 1, len(msg)+1)
				buf[0] = byte(userMsg)
				buf = append(buf, msg...)
				toSend = append(toSend, buf)
			}
		}
	}
	return toSend
}
