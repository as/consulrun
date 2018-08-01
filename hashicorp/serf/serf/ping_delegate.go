package serf

import (
	"bytes"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/go-msgpack/codec"
	"github.com/as/consulrun/hashicorp/memberlist"
	"github.com/as/consulrun/hashicorp/serf/coordinate"
)

type pingDelegate struct {
	serf *Serf
}

const (
	PingVersion = 1
)

func (p *pingDelegate) AckPayload() []byte {
	var buf bytes.Buffer

	version := []byte{PingVersion}
	buf.Write(version)

	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(p.serf.coordClient.GetCoordinate()); err != nil {
		p.serf.logger.Printf("[ERR] serf: Failed to encode coordinate: %v\n", err)
	}
	return buf.Bytes()
}

func (p *pingDelegate) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	if payload == nil || len(payload) == 0 {
		return
	}

	version := payload[0]
	if version != PingVersion {
		p.serf.logger.Printf("[ERR] serf: Unsupported ping version: %v", version)
		return
	}

	r := bytes.NewReader(payload[1:])
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	var coord coordinate.Coordinate
	if err := dec.Decode(&coord); err != nil {
		p.serf.logger.Printf("[ERR] serf: Failed to decode coordinate from ping: %v", err)
		return
	}

	before := p.serf.coordClient.GetCoordinate()
	after, err := p.serf.coordClient.Update(other.Name, &coord, rtt)
	if err != nil {
		metrics.IncrCounter([]string{"serf", "coordinate", "rejected"}, 1)
		p.serf.logger.Printf("[TRACE] serf: Rejected coordinate from %s: %v\n",
			other.Name, err)
		return
	}

	d := float32(before.DistanceTo(after).Seconds() * 1.0e3)
	metrics.AddSample([]string{"serf", "coordinate", "adjustment-ms"}, d)

	p.serf.coordCacheLock.Lock()
	p.serf.coordCache[other.Name] = &coord
	p.serf.coordCache[p.serf.config.NodeName] = p.serf.coordClient.GetCoordinate()
	p.serf.coordCacheLock.Unlock()
}
