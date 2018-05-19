// The /v1/operator/area endpoints are available only in Consul Enterprise and
package api

import (
	"net"
	"time"
)

type Area struct {
	ID string

	PeerDatacenter string

	RetryJoin []string

	UseTLS bool
}

type AreaJoinResponse struct {
	Address string

	Joined bool

	Error string
}

type SerfMember struct {
	ID string

	Name string

	Addr net.IP

	Port uint16

	Datacenter string

	Role string

	Build string

	Protocol int

	Status string

	RTT time.Duration
}

func (op *Operator) AreaCreate(area *Area, q *WriteOptions) (string, *WriteMeta, error) {
	r := op.c.newRequest("POST", "/v1/operator/area")
	r.setWriteOptions(q)
	r.obj = area
	rtt, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt

	var out struct{ ID string }
	if err := decodeBody(resp, &out); err != nil {
		return "", nil, err
	}
	return out.ID, wm, nil
}

func (op *Operator) AreaUpdate(areaID string, area *Area, q *WriteOptions) (string, *WriteMeta, error) {
	r := op.c.newRequest("PUT", "/v1/operator/area/"+areaID)
	r.setWriteOptions(q)
	r.obj = area
	rtt, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt

	var out struct{ ID string }
	if err := decodeBody(resp, &out); err != nil {
		return "", nil, err
	}
	return out.ID, wm, nil
}

func (op *Operator) AreaGet(areaID string, q *QueryOptions) ([]*Area, *QueryMeta, error) {
	var out []*Area
	qm, err := op.c.query("/v1/operator/area/"+areaID, &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (op *Operator) AreaList(q *QueryOptions) ([]*Area, *QueryMeta, error) {
	var out []*Area
	qm, err := op.c.query("/v1/operator/area", &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (op *Operator) AreaDelete(areaID string, q *WriteOptions) (*WriteMeta, error) {
	r := op.c.newRequest("DELETE", "/v1/operator/area/"+areaID)
	r.setWriteOptions(q)
	rtt, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt
	return wm, nil
}

func (op *Operator) AreaJoin(areaID string, addresses []string, q *WriteOptions) ([]*AreaJoinResponse, *WriteMeta, error) {
	r := op.c.newRequest("PUT", "/v1/operator/area/"+areaID+"/join")
	r.setWriteOptions(q)
	r.obj = addresses
	rtt, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt

	var out []*AreaJoinResponse
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, wm, nil
}

func (op *Operator) AreaMembers(areaID string, q *QueryOptions) ([]*SerfMember, *QueryMeta, error) {
	var out []*SerfMember
	qm, err := op.c.query("/v1/operator/area/"+areaID+"/members", &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}
