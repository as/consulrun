package serf

import (
	"bytes"
	"fmt"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-msgpack/codec"
)

type delegate struct {
	serf *Serf
}

func (d *delegate) NodeMeta(limit int) []byte {
	roleBytes := d.serf.encodeTags(d.serf.config.Tags)
	if len(roleBytes) > limit {
		panic(fmt.Errorf("Node tags '%v' exceeds length limit of %d bytes", d.serf.config.Tags, limit))
	}

	return roleBytes
}

func (d *delegate) NotifyMsg(buf []byte) {

	if len(buf) == 0 {
		return
	}
	metrics.AddSample([]string{"serf", "msgs", "received"}, float32(len(buf)))

	rebroadcast := false
	rebroadcastQueue := d.serf.broadcasts
	t := messageType(buf[0])
	switch t {
	case messageLeaveType:
		var leave messageLeave
		if err := decodeMessage(buf[1:], &leave); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding leave message: %s", err)
			break
		}

		d.serf.logger.Printf("[DEBUG] serf: messageLeaveType: %s", leave.Node)
		rebroadcast = d.serf.handleNodeLeaveIntent(&leave)

	case messageJoinType:
		var join messageJoin
		if err := decodeMessage(buf[1:], &join); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding join message: %s", err)
			break
		}

		d.serf.logger.Printf("[DEBUG] serf: messageJoinType: %s", join.Node)
		rebroadcast = d.serf.handleNodeJoinIntent(&join)

	case messageUserEventType:
		var event messageUserEvent
		if err := decodeMessage(buf[1:], &event); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding user event message: %s", err)
			break
		}

		d.serf.logger.Printf("[DEBUG] serf: messageUserEventType: %s", event.Name)
		rebroadcast = d.serf.handleUserEvent(&event)
		rebroadcastQueue = d.serf.eventBroadcasts

	case messageQueryType:
		var query messageQuery
		if err := decodeMessage(buf[1:], &query); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding query message: %s", err)
			break
		}

		d.serf.logger.Printf("[DEBUG] serf: messageQueryType: %s", query.Name)
		rebroadcast = d.serf.handleQuery(&query)
		rebroadcastQueue = d.serf.queryBroadcasts

	case messageQueryResponseType:
		var resp messageQueryResponse
		if err := decodeMessage(buf[1:], &resp); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding query response message: %s", err)
			break
		}

		d.serf.logger.Printf("[DEBUG] serf: messageQueryResponseType: %v", resp.From)
		d.serf.handleQueryResponse(&resp)

	case messageRelayType:
		var header relayHeader
		var handle codec.MsgpackHandle
		reader := bytes.NewReader(buf[1:])
		decoder := codec.NewDecoder(reader, &handle)
		if err := decoder.Decode(&header); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error decoding relay header: %s", err)
			break
		}

		raw := make([]byte, reader.Len())
		reader.Read(raw)
		d.serf.logger.Printf("[DEBUG] serf: Relaying response to addr: %s", header.DestAddr.String())
		if err := d.serf.memberlist.SendTo(&header.DestAddr, raw); err != nil {
			d.serf.logger.Printf("[ERR] serf: Error forwarding message to %s: %s", header.DestAddr.String(), err)
			break
		}

	default:
		d.serf.logger.Printf("[WARN] serf: Received message of unknown type: %d", t)
	}

	if rebroadcast {

		newBuf := make([]byte, len(buf))
		copy(newBuf, buf)

		rebroadcastQueue.QueueBroadcast(&broadcast{
			msg:    newBuf,
			notify: nil,
		})
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	msgs := d.serf.broadcasts.GetBroadcasts(overhead, limit)

	bytesUsed := 0
	for _, msg := range msgs {
		lm := len(msg)
		bytesUsed += lm + overhead
		metrics.AddSample([]string{"serf", "msgs", "sent"}, float32(lm))
	}

	queryMsgs := d.serf.queryBroadcasts.GetBroadcasts(overhead, limit-bytesUsed)
	if queryMsgs != nil {
		for _, m := range queryMsgs {
			lm := len(m)
			bytesUsed += lm + overhead
			metrics.AddSample([]string{"serf", "msgs", "sent"}, float32(lm))
		}
		msgs = append(msgs, queryMsgs...)
	}

	eventMsgs := d.serf.eventBroadcasts.GetBroadcasts(overhead, limit-bytesUsed)
	if eventMsgs != nil {
		for _, m := range eventMsgs {
			lm := len(m)
			bytesUsed += lm + overhead
			metrics.AddSample([]string{"serf", "msgs", "sent"}, float32(lm))
		}
		msgs = append(msgs, eventMsgs...)
	}

	return msgs
}

func (d *delegate) LocalState(join bool) []byte {
	d.serf.memberLock.RLock()
	defer d.serf.memberLock.RUnlock()
	d.serf.eventLock.RLock()
	defer d.serf.eventLock.RUnlock()

	pp := messagePushPull{
		LTime:        d.serf.clock.Time(),
		StatusLTimes: make(map[string]LamportTime, len(d.serf.members)),
		LeftMembers:  make([]string, 0, len(d.serf.leftMembers)),
		EventLTime:   d.serf.eventClock.Time(),
		Events:       d.serf.eventBuffer,
		QueryLTime:   d.serf.queryClock.Time(),
	}

	for name, member := range d.serf.members {
		pp.StatusLTimes[name] = member.statusLTime
	}

	for _, member := range d.serf.leftMembers {
		pp.LeftMembers = append(pp.LeftMembers, member.Name)
	}

	buf, err := encodeMessage(messagePushPullType, &pp)
	if err != nil {
		d.serf.logger.Printf("[ERR] serf: Failed to encode local state: %v", err)
		return nil
	}
	return buf
}

func (d *delegate) MergeRemoteState(buf []byte, isJoin bool) {

	if len(buf) == 0 {
		d.serf.logger.Printf("[ERR] serf: Remote state is zero bytes")
		return
	}

	if messageType(buf[0]) != messagePushPullType {
		d.serf.logger.Printf("[ERR] serf: Remote state has bad type prefix: %v", buf[0])
		return
	}

	pp := messagePushPull{}
	if err := decodeMessage(buf[1:], &pp); err != nil {
		d.serf.logger.Printf("[ERR] serf: Failed to decode remote state: %v", err)
		return
	}

	if pp.LTime > 0 {
		d.serf.clock.Witness(pp.LTime - 1)
	}
	if pp.EventLTime > 0 {
		d.serf.eventClock.Witness(pp.EventLTime - 1)
	}
	if pp.QueryLTime > 0 {
		d.serf.queryClock.Witness(pp.QueryLTime - 1)
	}

	leftMap := make(map[string]struct{}, len(pp.LeftMembers))
	leave := messageLeave{}
	for _, name := range pp.LeftMembers {
		leftMap[name] = struct{}{}
		leave.LTime = pp.StatusLTimes[name]
		leave.Node = name
		d.serf.handleNodeLeaveIntent(&leave)
	}

	join := messageJoin{}
	for name, statusLTime := range pp.StatusLTimes {

		if _, ok := leftMap[name]; ok {
			continue
		}

		join.LTime = statusLTime
		join.Node = name
		d.serf.handleNodeJoinIntent(&join)
	}

	eventJoinIgnore := d.serf.eventJoinIgnore.Load().(bool)
	if isJoin && eventJoinIgnore {
		d.serf.eventLock.Lock()
		if pp.EventLTime > d.serf.eventMinTime {
			d.serf.eventMinTime = pp.EventLTime
		}
		d.serf.eventLock.Unlock()
	}

	userEvent := messageUserEvent{}
	for _, events := range pp.Events {
		if events == nil {
			continue
		}
		userEvent.LTime = events.LTime
		for _, e := range events.Events {
			userEvent.Name = e.Name
			userEvent.Payload = e.Payload
			d.serf.handleUserEvent(&userEvent)
		}
	}
}
