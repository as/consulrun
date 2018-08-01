package structs

import "github.com/as/consulrun/hashicorp/consul/types"

type QueryDatacenterOptions struct {
	NearestN int

	Datacenters []string
}

type QueryDNSOptions struct {
	TTL string
}

type ServiceQuery struct {
	Service string

	Failover QueryDatacenterOptions

	OnlyPassing bool

	IgnoreCheckIDs []types.CheckID

	Near string

	Tags []string

	NodeMeta map[string]string
}

const (
	QueryTemplateTypeNamePrefixMatch = "name_prefix_match"
)

type QueryTemplateOptions struct {
	Type string

	Regexp string

	RemoveEmptyTags bool
}

type PreparedQuery struct {
	ID string

	Name string

	Session string

	Token string

	Template QueryTemplateOptions

	Service ServiceQuery

	DNS QueryDNSOptions

	RaftIndex
}

func (pq *PreparedQuery) GetACLPrefix() (string, bool) {
	if pq.Name != "" || pq.Template.Type != "" {
		return pq.Name, true
	}

	return "", false
}

type PreparedQueries []*PreparedQuery

type IndexedPreparedQueries struct {
	Queries PreparedQueries
	QueryMeta
}

type PreparedQueryOp string

const (
	PreparedQueryCreate PreparedQueryOp = "create"
	PreparedQueryUpdate PreparedQueryOp = "update"
	PreparedQueryDelete PreparedQueryOp = "delete"
)

type PreparedQueryRequest struct {
	Datacenter string

	Op PreparedQueryOp

	Query *PreparedQuery

	WriteRequest
}

func (q *PreparedQueryRequest) RequestDatacenter() string {
	return q.Datacenter
}

type PreparedQuerySpecificRequest struct {
	Datacenter string

	QueryID string

	QueryOptions
}

func (q *PreparedQuerySpecificRequest) RequestDatacenter() string {
	return q.Datacenter
}

type PreparedQueryExecuteRequest struct {
	Datacenter string

	QueryIDOrName string

	Limit int

	Source QuerySource

	Agent QuerySource

	QueryOptions
}

func (q *PreparedQueryExecuteRequest) RequestDatacenter() string {
	return q.Datacenter
}

type PreparedQueryExecuteRemoteRequest struct {
	Datacenter string

	Query PreparedQuery

	Limit int

	QueryOptions
}

func (q *PreparedQueryExecuteRemoteRequest) RequestDatacenter() string {
	return q.Datacenter
}

type PreparedQueryExecuteResponse struct {
	Service string

	Nodes CheckServiceNodes

	DNS QueryDNSOptions

	Datacenter string

	Failovers int

	QueryMeta
}

type PreparedQueryExplainResponse struct {
	Query PreparedQuery

	QueryMeta
}
