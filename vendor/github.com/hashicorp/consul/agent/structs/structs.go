package structs

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/serf/coordinate"
)

type MessageType uint8

type RaftIndex struct {
	CreateIndex uint64
	ModifyIndex uint64
}

const (
	RegisterRequestType       MessageType = 0
	DeregisterRequestType                 = 1
	KVSRequestType                        = 2
	SessionRequestType                    = 3
	ACLRequestType                        = 4
	TombstoneRequestType                  = 5
	CoordinateBatchUpdateType             = 6
	PreparedQueryRequestType              = 7
	TxnRequestType                        = 8
	AutopilotRequestType                  = 9
	AreaRequestType                       = 10
	ACLBootstrapRequestType               = 11 // FSM snapshots only.
)

const (
	IgnoreUnknownTypeFlag MessageType = 128

	NodeMaint = "_node_maintenance"

	ServiceMaintPrefix = "_service_maintenance:"

	metaKeyReservedPrefix = "consul-"

	metaMaxKeyPairs = 64

	metaKeyMaxLength = 128

	metaValueMaxLength = 512

	MetaSegmentKey = "consul-network-segment"

	MaxLockDelay = 60 * time.Second
)

var metaKeyFormat = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString

func ValidStatus(s string) bool {
	return s == api.HealthPassing || s == api.HealthWarning || s == api.HealthCritical
}

type RPCInfo interface {
	RequestDatacenter() string
	IsRead() bool
	AllowStaleRead() bool
	ACLToken() string
}

type QueryOptions struct {
	Token string

	MinQueryIndex uint64

	MaxQueryTime time.Duration

	AllowStale bool

	RequireConsistent bool

	MaxStaleDuration time.Duration
}

func (q QueryOptions) IsRead() bool {
	return true
}

func (q QueryOptions) ConsistencyLevel() string {
	if q.RequireConsistent {
		return "consistent"
	} else if q.AllowStale {
		return "stale"
	} else {
		return "leader"
	}
}

func (q QueryOptions) AllowStaleRead() bool {
	return q.AllowStale
}

func (q QueryOptions) ACLToken() string {
	return q.Token
}

type WriteRequest struct {
	Token string
}

func (w WriteRequest) IsRead() bool {
	return false
}

func (w WriteRequest) AllowStaleRead() bool {
	return false
}

func (w WriteRequest) ACLToken() string {
	return w.Token
}

type QueryMeta struct {
	Index uint64

	LastContact time.Duration

	KnownLeader bool

	ConsistencyLevel string
}

type RegisterRequest struct {
	Datacenter      string
	ID              types.NodeID
	Node            string
	Address         string
	TaggedAddresses map[string]string
	NodeMeta        map[string]string
	Service         *NodeService
	Check           *HealthCheck
	Checks          HealthChecks

	SkipNodeUpdate bool

	WriteRequest
}

func (r *RegisterRequest) RequestDatacenter() string {
	return r.Datacenter
}

func (r *RegisterRequest) ChangesNode(node *Node) bool {

	if node == nil {
		return true
	}

	if r.SkipNodeUpdate {
		return false
	}

	if r.ID != node.ID ||
		r.Node != node.Node ||
		r.Address != node.Address ||
		r.Datacenter != node.Datacenter ||
		!reflect.DeepEqual(r.TaggedAddresses, node.TaggedAddresses) ||
		!reflect.DeepEqual(r.NodeMeta, node.Meta) {
		return true
	}

	return false
}

type DeregisterRequest struct {
	Datacenter string
	Node       string
	ServiceID  string
	CheckID    types.CheckID
	WriteRequest
}

func (r *DeregisterRequest) RequestDatacenter() string {
	return r.Datacenter
}

type QuerySource struct {
	Datacenter string
	Segment    string
	Node       string
	Ip         string
}

type DCSpecificRequest struct {
	Datacenter      string
	NodeMetaFilters map[string]string
	Source          QuerySource
	QueryOptions
}

func (r *DCSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

type ServiceSpecificRequest struct {
	Datacenter      string
	NodeMetaFilters map[string]string
	ServiceName     string
	ServiceTag      string
	TagFilter       bool // Controls tag filtering
	Source          QuerySource
	QueryOptions
}

func (r *ServiceSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

type NodeSpecificRequest struct {
	Datacenter string
	Node       string
	QueryOptions
}

func (r *NodeSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

type ChecksInStateRequest struct {
	Datacenter      string
	NodeMetaFilters map[string]string
	State           string
	Source          QuerySource
	QueryOptions
}

func (r *ChecksInStateRequest) RequestDatacenter() string {
	return r.Datacenter
}

type Node struct {
	ID              types.NodeID
	Node            string
	Address         string
	Datacenter      string
	TaggedAddresses map[string]string
	Meta            map[string]string

	RaftIndex
}
type Nodes []*Node

func ValidateMetadata(meta map[string]string, allowConsulPrefix bool) error {
	if len(meta) > metaMaxKeyPairs {
		return fmt.Errorf("Node metadata cannot contain more than %d key/value pairs", metaMaxKeyPairs)
	}

	for key, value := range meta {
		if err := validateMetaPair(key, value, allowConsulPrefix); err != nil {
			return fmt.Errorf("Couldn't load metadata pair ('%s', '%s'): %s", key, value, err)
		}
	}

	return nil
}

func validateMetaPair(key, value string, allowConsulPrefix bool) error {
	if key == "" {
		return fmt.Errorf("Key cannot be blank")
	}
	if !metaKeyFormat(key) {
		return fmt.Errorf("Key contains invalid characters")
	}
	if len(key) > metaKeyMaxLength {
		return fmt.Errorf("Key is too long (limit: %d characters)", metaKeyMaxLength)
	}
	if strings.HasPrefix(key, metaKeyReservedPrefix) && !allowConsulPrefix {
		return fmt.Errorf("Key prefix '%s' is reserved for internal use", metaKeyReservedPrefix)
	}
	if len(value) > metaValueMaxLength {
		return fmt.Errorf("Value is too long (limit: %d characters)", metaValueMaxLength)
	}
	return nil
}

func SatisfiesMetaFilters(meta map[string]string, filters map[string]string) bool {
	for key, value := range filters {
		if v, ok := meta[key]; !ok || v != value {
			return false
		}
	}
	return true
}

type Services map[string][]string

type ServiceNode struct {
	ID                       types.NodeID
	Node                     string
	Address                  string
	Datacenter               string
	TaggedAddresses          map[string]string
	NodeMeta                 map[string]string
	ServiceID                string
	ServiceName              string
	ServiceTags              []string
	ServiceAddress           string
	ServiceMeta              map[string]string
	ServicePort              int
	ServiceEnableTagOverride bool

	RaftIndex
}

func (s *ServiceNode) PartialClone() *ServiceNode {
	tags := make([]string, len(s.ServiceTags))
	copy(tags, s.ServiceTags)
	nsmeta := make(map[string]string)
	for k, v := range s.ServiceMeta {
		nsmeta[k] = v
	}

	return &ServiceNode{

		Node: s.Node,

		ServiceID:                s.ServiceID,
		ServiceName:              s.ServiceName,
		ServiceTags:              tags,
		ServiceAddress:           s.ServiceAddress,
		ServicePort:              s.ServicePort,
		ServiceMeta:              nsmeta,
		ServiceEnableTagOverride: s.ServiceEnableTagOverride,
		RaftIndex: RaftIndex{
			CreateIndex: s.CreateIndex,
			ModifyIndex: s.ModifyIndex,
		},
	}
}

func (s *ServiceNode) ToNodeService() *NodeService {
	return &NodeService{
		ID:                s.ServiceID,
		Service:           s.ServiceName,
		Tags:              s.ServiceTags,
		Address:           s.ServiceAddress,
		Port:              s.ServicePort,
		Meta:              s.ServiceMeta,
		EnableTagOverride: s.ServiceEnableTagOverride,
		RaftIndex: RaftIndex{
			CreateIndex: s.CreateIndex,
			ModifyIndex: s.ModifyIndex,
		},
	}
}

type ServiceNodes []*ServiceNode

type NodeService struct {
	ID                string
	Service           string
	Tags              []string
	Address           string
	Meta              map[string]string
	Port              int
	EnableTagOverride bool

	RaftIndex
}

func (s *NodeService) IsSame(other *NodeService) bool {
	if s.ID != other.ID ||
		s.Service != other.Service ||
		!reflect.DeepEqual(s.Tags, other.Tags) ||
		s.Address != other.Address ||
		s.Port != other.Port ||
		!reflect.DeepEqual(s.Meta, other.Meta) ||
		s.EnableTagOverride != other.EnableTagOverride {
		return false
	}

	return true
}

func (s *NodeService) ToServiceNode(node string) *ServiceNode {
	return &ServiceNode{

		Node: node,

		ServiceID:                s.ID,
		ServiceName:              s.Service,
		ServiceTags:              s.Tags,
		ServiceAddress:           s.Address,
		ServicePort:              s.Port,
		ServiceMeta:              s.Meta,
		ServiceEnableTagOverride: s.EnableTagOverride,
		RaftIndex: RaftIndex{
			CreateIndex: s.CreateIndex,
			ModifyIndex: s.ModifyIndex,
		},
	}
}

type NodeServices struct {
	Node     *Node
	Services map[string]*NodeService
}

type HealthCheck struct {
	Node        string
	CheckID     types.CheckID // Unique per-node ID
	Name        string        // Check name
	Status      string        // The current check status
	Notes       string        // Additional notes with the status
	Output      string        // Holds output of script runs
	ServiceID   string        // optional associated service
	ServiceName string        // optional service name
	ServiceTags []string      // optional service tags

	Definition HealthCheckDefinition

	RaftIndex
}

type HealthCheckDefinition struct {
	HTTP                           string               `json:",omitempty"`
	TLSSkipVerify                  bool                 `json:",omitempty"`
	Header                         map[string][]string  `json:",omitempty"`
	Method                         string               `json:",omitempty"`
	TCP                            string               `json:",omitempty"`
	Interval                       api.ReadableDuration `json:",omitempty"`
	Timeout                        api.ReadableDuration `json:",omitempty"`
	DeregisterCriticalServiceAfter api.ReadableDuration `json:",omitempty"`
}

func (c *HealthCheck) IsSame(other *HealthCheck) bool {
	if c.Node != other.Node ||
		c.CheckID != other.CheckID ||
		c.Name != other.Name ||
		c.Status != other.Status ||
		c.Notes != other.Notes ||
		c.Output != other.Output ||
		c.ServiceID != other.ServiceID ||
		c.ServiceName != other.ServiceName ||
		!reflect.DeepEqual(c.ServiceTags, other.ServiceTags) {
		return false
	}

	return true
}

func (c *HealthCheck) Clone() *HealthCheck {
	clone := new(HealthCheck)
	*clone = *c
	return clone
}

type HealthChecks []*HealthCheck

type CheckServiceNode struct {
	Node    *Node
	Service *NodeService
	Checks  HealthChecks
}
type CheckServiceNodes []CheckServiceNode

func (nodes CheckServiceNodes) Shuffle() {
	for i := len(nodes) - 1; i > 0; i-- {
		j := rand.Int31n(int32(i + 1))
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func (nodes CheckServiceNodes) Filter(onlyPassing bool) CheckServiceNodes {
	return nodes.FilterIgnore(onlyPassing, nil)
}

func (nodes CheckServiceNodes) FilterIgnore(onlyPassing bool,
	ignoreCheckIDs []types.CheckID) CheckServiceNodes {
	n := len(nodes)
OUTER:
	for i := 0; i < n; i++ {
		node := nodes[i]
	INNER:
		for _, check := range node.Checks {
			for _, ignore := range ignoreCheckIDs {
				if check.CheckID == ignore {

					continue INNER
				}
			}
			if check.Status == api.HealthCritical ||
				(onlyPassing && check.Status != api.HealthPassing) {
				nodes[i], nodes[n-1] = nodes[n-1], CheckServiceNode{}
				n--
				i--

				continue OUTER
			}
		}
	}
	return nodes[:n]
}

type NodeInfo struct {
	ID              types.NodeID
	Node            string
	Address         string
	TaggedAddresses map[string]string
	Meta            map[string]string
	Services        []*NodeService
	Checks          HealthChecks
}

type NodeDump []*NodeInfo

type IndexedNodes struct {
	Nodes Nodes
	QueryMeta
}

type IndexedServices struct {
	Services Services
	QueryMeta
}

type IndexedServiceNodes struct {
	ServiceNodes ServiceNodes
	QueryMeta
}

type IndexedNodeServices struct {
	NodeServices *NodeServices
	QueryMeta
}

type IndexedHealthChecks struct {
	HealthChecks HealthChecks
	QueryMeta
}

type IndexedCheckServiceNodes struct {
	Nodes CheckServiceNodes
	QueryMeta
}

type IndexedNodeDump struct {
	Dump NodeDump
	QueryMeta
}

type DirEntry struct {
	LockIndex uint64
	Key       string
	Flags     uint64
	Value     []byte
	Session   string `json:",omitempty"`

	RaftIndex
}

func (d *DirEntry) Clone() *DirEntry {
	return &DirEntry{
		LockIndex: d.LockIndex,
		Key:       d.Key,
		Flags:     d.Flags,
		Value:     d.Value,
		Session:   d.Session,
		RaftIndex: RaftIndex{
			CreateIndex: d.CreateIndex,
			ModifyIndex: d.ModifyIndex,
		},
	}
}

type DirEntries []*DirEntry

type KVSRequest struct {
	Datacenter string
	Op         api.KVOp // Which operation are we performing
	DirEnt     DirEntry // Which directory entry
	WriteRequest
}

func (r *KVSRequest) RequestDatacenter() string {
	return r.Datacenter
}

type KeyRequest struct {
	Datacenter string
	Key        string
	QueryOptions
}

func (r *KeyRequest) RequestDatacenter() string {
	return r.Datacenter
}

type KeyListRequest struct {
	Datacenter string
	Prefix     string
	Seperator  string
	QueryOptions
}

func (r *KeyListRequest) RequestDatacenter() string {
	return r.Datacenter
}

type IndexedDirEntries struct {
	Entries DirEntries
	QueryMeta
}

type IndexedKeyList struct {
	Keys []string
	QueryMeta
}

type SessionBehavior string

const (
	SessionKeysRelease SessionBehavior = "release"
	SessionKeysDelete                  = "delete"
)

const (
	SessionTTLMax        = 24 * time.Hour
	SessionTTLMultiplier = 2
)

type Session struct {
	ID        string
	Name      string
	Node      string
	Checks    []types.CheckID
	LockDelay time.Duration
	Behavior  SessionBehavior // What to do when session is invalidated
	TTL       string

	RaftIndex
}
type Sessions []*Session

type SessionOp string

const (
	SessionCreate  SessionOp = "create"
	SessionDestroy           = "destroy"
)

type SessionRequest struct {
	Datacenter string
	Op         SessionOp // Which operation are we performing
	Session    Session   // Which session
	WriteRequest
}

func (r *SessionRequest) RequestDatacenter() string {
	return r.Datacenter
}

type SessionSpecificRequest struct {
	Datacenter string
	Session    string
	QueryOptions
}

func (r *SessionSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

type IndexedSessions struct {
	Sessions Sessions
	QueryMeta
}

type Coordinate struct {
	Node    string
	Segment string
	Coord   *coordinate.Coordinate
}

type Coordinates []*Coordinate

type IndexedCoordinate struct {
	Coord *coordinate.Coordinate
	QueryMeta
}

type IndexedCoordinates struct {
	Coordinates Coordinates
	QueryMeta
}

type DatacenterMap struct {
	Datacenter  string
	AreaID      types.AreaID
	Coordinates Coordinates
}

type CoordinateUpdateRequest struct {
	Datacenter string
	Node       string
	Segment    string
	Coord      *coordinate.Coordinate
	WriteRequest
}

func (c *CoordinateUpdateRequest) RequestDatacenter() string {
	return c.Datacenter
}

type EventFireRequest struct {
	Datacenter string
	Name       string
	Payload    []byte

	QueryOptions
}

func (r *EventFireRequest) RequestDatacenter() string {
	return r.Datacenter
}

type EventFireResponse struct {
	QueryMeta
}

type TombstoneOp string

const (
	TombstoneReap TombstoneOp = "reap"
)

type TombstoneRequest struct {
	Datacenter string
	Op         TombstoneOp
	ReapIndex  uint64
	WriteRequest
}

func (r *TombstoneRequest) RequestDatacenter() string {
	return r.Datacenter
}

var msgpackHandle = &codec.MsgpackHandle{}

func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), msgpackHandle).Decode(out)
}

func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(msg)
	return buf.Bytes(), err
}

type CompoundResponse interface {
	Add(interface{})

	New() interface{}
}

type KeyringOp string

const (
	KeyringList    KeyringOp = "list"
	KeyringInstall           = "install"
	KeyringUse               = "use"
	KeyringRemove            = "remove"
)

type KeyringRequest struct {
	Operation   KeyringOp
	Key         string
	Datacenter  string
	Forwarded   bool
	RelayFactor uint8
	QueryOptions
}

func (r *KeyringRequest) RequestDatacenter() string {
	return r.Datacenter
}

type KeyringResponse struct {
	WAN        bool
	Datacenter string
	Segment    string
	Messages   map[string]string `json:",omitempty"`
	Keys       map[string]int
	NumNodes   int
	Error      string `json:",omitempty"`
}

type KeyringResponses struct {
	Responses []*KeyringResponse
	QueryMeta
}

func (r *KeyringResponses) Add(v interface{}) {
	val := v.(*KeyringResponses)
	r.Responses = append(r.Responses, val.Responses...)
}

func (r *KeyringResponses) New() interface{} {
	return new(KeyringResponses)
}
