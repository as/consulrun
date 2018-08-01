package api

type QueryDatacenterOptions struct {
	NearestN int

	Datacenters []string
}

type QueryDNSOptions struct {
	TTL string
}

type ServiceQuery struct {
	Service string

	Near string

	Failover QueryDatacenterOptions

	IgnoreCheckIDs []string

	OnlyPassing bool

	Tags []string

	NodeMeta map[string]string
}

type QueryTemplate struct {
	Type string

	Regexp string
}

type PreparedQueryDefinition struct {
	ID string

	Name string

	Session string

	Token string

	Service ServiceQuery

	DNS QueryDNSOptions

	Template QueryTemplate
}

type PreparedQueryExecuteResponse struct {
	Service string

	Nodes []ServiceEntry

	DNS QueryDNSOptions

	Datacenter string

	Failovers int
}

type PreparedQuery struct {
	c *Client
}

func (c *Client) PreparedQuery() *PreparedQuery {
	return &PreparedQuery{c}
}

func (c *PreparedQuery) Create(query *PreparedQueryDefinition, q *WriteOptions) (string, *WriteMeta, error) {
	r := c.c.newRequest("POST", "/v1/query")
	r.setWriteOptions(q)
	r.obj = query
	rtt, resp, err := requireOK(c.c.doRequest(r))
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

func (c *PreparedQuery) Update(query *PreparedQueryDefinition, q *WriteOptions) (*WriteMeta, error) {
	return c.c.write("/v1/query/"+query.ID, query, nil, q)
}

func (c *PreparedQuery) List(q *QueryOptions) ([]*PreparedQueryDefinition, *QueryMeta, error) {
	var out []*PreparedQueryDefinition
	qm, err := c.c.query("/v1/query", &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (c *PreparedQuery) Get(queryID string, q *QueryOptions) ([]*PreparedQueryDefinition, *QueryMeta, error) {
	var out []*PreparedQueryDefinition
	qm, err := c.c.query("/v1/query/"+queryID, &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (c *PreparedQuery) Delete(queryID string, q *WriteOptions) (*WriteMeta, error) {
	r := c.c.newRequest("DELETE", "/v1/query/"+queryID)
	r.setWriteOptions(q)
	rtt, resp, err := requireOK(c.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{}
	wm.RequestTime = rtt
	return wm, nil
}

func (c *PreparedQuery) Execute(queryIDOrName string, q *QueryOptions) (*PreparedQueryExecuteResponse, *QueryMeta, error) {
	var out *PreparedQueryExecuteResponse
	qm, err := c.c.query("/v1/query/"+queryIDOrName+"/execute", &out, q)
	if err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}
