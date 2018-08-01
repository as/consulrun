package api

import (
	"bytes"
	"strconv"
)

type Event struct {
	c *Client
}

type UserEvent struct {
	ID            string
	Name          string
	Payload       []byte
	NodeFilter    string
	ServiceFilter string
	TagFilter     string
	Version       int
	LTime         uint64
}

func (c *Client) Event() *Event {
	return &Event{c}
}

func (e *Event) Fire(params *UserEvent, q *WriteOptions) (string, *WriteMeta, error) {
	r := e.c.newRequest("PUT", "/v1/event/fire/"+params.Name)
	r.setWriteOptions(q)
	if params.NodeFilter != "" {
		r.params.Set("node", params.NodeFilter)
	}
	if params.ServiceFilter != "" {
		r.params.Set("service", params.ServiceFilter)
	}
	if params.TagFilter != "" {
		r.params.Set("tag", params.TagFilter)
	}
	if params.Payload != nil {
		r.body = bytes.NewReader(params.Payload)
	}

	rtt, resp, err := requireOK(e.c.doRequest(r))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{RequestTime: rtt}
	var out UserEvent
	if err := decodeBody(resp, &out); err != nil {
		return "", nil, err
	}
	return out.ID, wm, nil
}

func (e *Event) List(name string, q *QueryOptions) ([]*UserEvent, *QueryMeta, error) {
	r := e.c.newRequest("GET", "/v1/event/list")
	r.setQueryOptions(q)
	if name != "" {
		r.params.Set("name", name)
	}
	rtt, resp, err := requireOK(e.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var entries []*UserEvent
	if err := decodeBody(resp, &entries); err != nil {
		return nil, nil, err
	}
	return entries, qm, nil
}

func (e *Event) IDToIndex(uuid string) uint64 {
	lower := uuid[0:8] + uuid[9:13] + uuid[14:18]
	upper := uuid[19:23] + uuid[24:36]
	lowVal, err := strconv.ParseUint(lower, 16, 64)
	if err != nil {
		panic("Failed to convert " + lower)
	}
	highVal, err := strconv.ParseUint(upper, 16, 64)
	if err != nil {
		panic("Failed to convert " + upper)
	}
	return lowVal ^ highVal
}
