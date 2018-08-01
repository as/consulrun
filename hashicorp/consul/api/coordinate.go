package api

import (
	"github.com/as/consulrun/hashicorp/serf/coordinate"
)

type CoordinateEntry struct {
	Node    string
	Segment string
	Coord   *coordinate.Coordinate
}

type CoordinateDatacenterMap struct {
	Datacenter  string
	AreaID      string
	Coordinates []CoordinateEntry
}

type Coordinate struct {
	c *Client
}

func (c *Client) Coordinate() *Coordinate {
	return &Coordinate{c}
}

func (c *Coordinate) Datacenters() ([]*CoordinateDatacenterMap, error) {
	r := c.c.newRequest("GET", "/v1/coordinate/datacenters")
	_, resp, err := requireOK(c.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []*CoordinateDatacenterMap
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Coordinate) Nodes(q *QueryOptions) ([]*CoordinateEntry, *QueryMeta, error) {
	r := c.c.newRequest("GET", "/v1/coordinate/nodes")
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(c.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out []*CoordinateEntry
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (c *Coordinate) Update(coord *CoordinateEntry, q *WriteOptions) (*WriteMeta, error) {
	r := c.c.newRequest("PUT", "/v1/coordinate/update")
	r.setWriteOptions(q)
	r.obj = coord
	rtt, resp, err := requireOK(c.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt

	return wm, nil
}

func (c *Coordinate) Node(node string, q *QueryOptions) ([]*CoordinateEntry, *QueryMeta, error) {
	r := c.c.newRequest("GET", "/v1/coordinate/node/"+node)
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(c.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out []*CoordinateEntry
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}
