package api

type Raw struct {
	c *Client
}

func (c *Client) Raw() *Raw {
	return &Raw{c}
}

func (raw *Raw) Query(endpoint string, out interface{}, q *QueryOptions) (*QueryMeta, error) {
	return raw.c.query(endpoint, out, q)
}

func (raw *Raw) Write(endpoint string, in, out interface{}, q *WriteOptions) (*WriteMeta, error) {
	return raw.c.write(endpoint, in, out, q)
}
