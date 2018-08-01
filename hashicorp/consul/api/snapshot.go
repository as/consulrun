package api

import (
	"io"
)

type Snapshot struct {
	c *Client
}

func (c *Client) Snapshot() *Snapshot {
	return &Snapshot{c}
}

func (s *Snapshot) Save(q *QueryOptions) (io.ReadCloser, *QueryMeta, error) {
	r := s.c.newRequest("GET", "/v1/snapshot")
	r.setQueryOptions(q)

	rtt, resp, err := requireOK(s.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt
	return resp.Body, qm, nil
}

func (s *Snapshot) Restore(q *WriteOptions, in io.Reader) error {
	r := s.c.newRequest("PUT", "/v1/snapshot")
	r.body = in
	r.setWriteOptions(q)
	_, _, err := requireOK(s.c.doRequest(r))
	if err != nil {
		return err
	}
	return nil
}
