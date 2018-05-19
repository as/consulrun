package api

type keyringRequest struct {
	Key string
}

type KeyringResponse struct {
	WAN bool

	Datacenter string

	Segment string

	Keys map[string]int

	NumNodes int
}

func (op *Operator) KeyringInstall(key string, q *WriteOptions) error {
	r := op.c.newRequest("POST", "/v1/operator/keyring")
	r.setWriteOptions(q)
	r.obj = keyringRequest{
		Key: key,
	}
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (op *Operator) KeyringList(q *QueryOptions) ([]*KeyringResponse, error) {
	r := op.c.newRequest("GET", "/v1/operator/keyring")
	r.setQueryOptions(q)
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []*KeyringResponse
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (op *Operator) KeyringRemove(key string, q *WriteOptions) error {
	r := op.c.newRequest("DELETE", "/v1/operator/keyring")
	r.setWriteOptions(q)
	r.obj = keyringRequest{
		Key: key,
	}
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (op *Operator) KeyringUse(key string, q *WriteOptions) error {
	r := op.c.newRequest("PUT", "/v1/operator/keyring")
	r.setWriteOptions(q)
	r.obj = keyringRequest{
		Key: key,
	}
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
