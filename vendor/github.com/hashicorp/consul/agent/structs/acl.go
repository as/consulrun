package structs

import (
	"errors"
	"time"

	"github.com/hashicorp/consul/acl"
)

type ACLOp string

const (
	ACLBootstrapInit = "bootstrap-init"

	ACLBootstrapNow = "bootstrap-now"

	ACLSet ACLOp = "set"

	ACLForceSet = "force-set"

	ACLDelete = "delete"
)

var ACLBootstrapNotInitializedErr = errors.New("ACL bootstrap not initialized, need to force a leader election and ensure all Consul servers support this feature")

var ACLBootstrapNotAllowedErr = errors.New("ACL bootstrap no longer allowed")

const (
	ACLTypeClient = "client"

	ACLTypeManagement = "management"
)

type ACL struct {
	ID    string
	Name  string
	Type  string
	Rules string

	RaftIndex
}

type ACLs []*ACL

func (a *ACL) IsSame(other *ACL) bool {
	if a.ID != other.ID ||
		a.Name != other.Name ||
		a.Type != other.Type ||
		a.Rules != other.Rules {
		return false
	}

	return true
}

type ACLBootstrap struct {
	AllowBootstrap bool

	RaftIndex
}

type ACLRequest struct {
	Datacenter string
	Op         ACLOp
	ACL        ACL
	WriteRequest
}

func (r *ACLRequest) RequestDatacenter() string {
	return r.Datacenter
}

type ACLRequests []*ACLRequest

type ACLSpecificRequest struct {
	Datacenter string
	ACL        string
	QueryOptions
}

func (r *ACLSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

type ACLPolicyRequest struct {
	Datacenter string
	ACL        string
	ETag       string
	QueryOptions
}

func (r *ACLPolicyRequest) RequestDatacenter() string {
	return r.Datacenter
}

type IndexedACLs struct {
	ACLs ACLs
	QueryMeta
}

type ACLPolicy struct {
	ETag   string
	Parent string
	Policy *acl.Policy
	TTL    time.Duration
	QueryMeta
}

type ACLReplicationStatus struct {
	Enabled          bool
	Running          bool
	SourceDatacenter string
	ReplicatedIndex  uint64
	LastSuccess      time.Time
	LastError        time.Time
}
