package api

type RaftServer struct {
	ID string

	Node string

	Address string

	Leader bool

	ProtocolVersion string

	Voter bool
}

type RaftConfiguration struct {
	Servers []*RaftServer

	Index uint64
}

func (op *Operator) RaftGetConfiguration(q *QueryOptions) (*RaftConfiguration, error) {
	r := op.c.newRequest("GET", "/v1/operator/raft/configuration")
	r.setQueryOptions(q)
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out RaftConfiguration
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (op *Operator) RaftRemovePeerByAddress(address string, q *WriteOptions) error {
	r := op.c.newRequest("DELETE", "/v1/operator/raft/peer")
	r.setWriteOptions(q)

	r.params.Set("address", string(address))

	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}

	resp.Body.Close()
	return nil
}

func (op *Operator) RaftRemovePeerByID(id string, q *WriteOptions) error {
	r := op.c.newRequest("DELETE", "/v1/operator/raft/peer")
	r.setWriteOptions(q)

	r.params.Set("id", string(id))

	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}

	resp.Body.Close()
	return nil
}
