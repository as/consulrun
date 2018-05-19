package api

import (
	"bufio"
	"fmt"
)

type AgentCheck struct {
	Node        string
	CheckID     string
	Name        string
	Status      string
	Notes       string
	Output      string
	ServiceID   string
	ServiceName string
	Definition  HealthCheckDefinition
}

type AgentService struct {
	ID                string
	Service           string
	Tags              []string
	Meta              map[string]string
	Port              int
	Address           string
	EnableTagOverride bool
	CreateIndex       uint64
	ModifyIndex       uint64
}

type AgentMember struct {
	Name        string
	Addr        string
	Port        uint16
	Tags        map[string]string
	Status      int
	ProtocolMin uint8
	ProtocolMax uint8
	ProtocolCur uint8
	DelegateMin uint8
	DelegateMax uint8
	DelegateCur uint8
}

const AllSegments = "_all"

type MembersOpts struct {
	WAN bool

	Segment string
}

type AgentServiceRegistration struct {
	ID                string            `json:",omitempty"`
	Name              string            `json:",omitempty"`
	Tags              []string          `json:",omitempty"`
	Port              int               `json:",omitempty"`
	Address           string            `json:",omitempty"`
	EnableTagOverride bool              `json:",omitempty"`
	Meta              map[string]string `json:",omitempty"`
	Check             *AgentServiceCheck
	Checks            AgentServiceChecks
}

type AgentCheckRegistration struct {
	ID        string `json:",omitempty"`
	Name      string `json:",omitempty"`
	Notes     string `json:",omitempty"`
	ServiceID string `json:",omitempty"`
	AgentServiceCheck
}

type AgentServiceCheck struct {
	CheckID           string              `json:",omitempty"`
	Name              string              `json:",omitempty"`
	Args              []string            `json:"ScriptArgs,omitempty"`
	Script            string              `json:",omitempty"` // Deprecated, use Args.
	DockerContainerID string              `json:",omitempty"`
	Shell             string              `json:",omitempty"` // Only supported for Docker.
	Interval          string              `json:",omitempty"`
	Timeout           string              `json:",omitempty"`
	TTL               string              `json:",omitempty"`
	HTTP              string              `json:",omitempty"`
	Header            map[string][]string `json:",omitempty"`
	Method            string              `json:",omitempty"`
	TCP               string              `json:",omitempty"`
	Status            string              `json:",omitempty"`
	Notes             string              `json:",omitempty"`
	TLSSkipVerify     bool                `json:",omitempty"`
	GRPC              string              `json:",omitempty"`
	GRPCUseTLS        bool                `json:",omitempty"`

	DeregisterCriticalServiceAfter string `json:",omitempty"`
}
type AgentServiceChecks []*AgentServiceCheck

type AgentToken struct {
	Token string
}

type MetricsInfo struct {
	Timestamp string
	Gauges    []GaugeValue
	Points    []PointValue
	Counters  []SampledValue
	Samples   []SampledValue
}

type GaugeValue struct {
	Name   string
	Value  float32
	Labels map[string]string
}

type PointValue struct {
	Name   string
	Points []float32
}

type SampledValue struct {
	Name   string
	Count  int
	Sum    float64
	Min    float64
	Max    float64
	Mean   float64
	Stddev float64
	Labels map[string]string
}

type Agent struct {
	c *Client

	nodeName string
}

func (c *Client) Agent() *Agent {
	return &Agent{c: c}
}

func (a *Agent) Self() (map[string]map[string]interface{}, error) {
	r := a.c.newRequest("GET", "/v1/agent/self")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out map[string]map[string]interface{}
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) Metrics() (*MetricsInfo, error) {
	r := a.c.newRequest("GET", "/v1/agent/metrics")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out *MetricsInfo
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) Reload() error {
	r := a.c.newRequest("PUT", "/v1/agent/reload")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) NodeName() (string, error) {
	if a.nodeName != "" {
		return a.nodeName, nil
	}
	info, err := a.Self()
	if err != nil {
		return "", err
	}
	name := info["Config"]["NodeName"].(string)
	a.nodeName = name
	return name, nil
}

func (a *Agent) Checks() (map[string]*AgentCheck, error) {
	r := a.c.newRequest("GET", "/v1/agent/checks")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out map[string]*AgentCheck
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) Services() (map[string]*AgentService, error) {
	r := a.c.newRequest("GET", "/v1/agent/services")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out map[string]*AgentService
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) Members(wan bool) ([]*AgentMember, error) {
	r := a.c.newRequest("GET", "/v1/agent/members")
	if wan {
		r.params.Set("wan", "1")
	}
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []*AgentMember
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) MembersOpts(opts MembersOpts) ([]*AgentMember, error) {
	r := a.c.newRequest("GET", "/v1/agent/members")
	r.params.Set("segment", opts.Segment)
	if opts.WAN {
		r.params.Set("wan", "1")
	}

	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []*AgentMember
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *Agent) ServiceRegister(service *AgentServiceRegistration) error {
	r := a.c.newRequest("PUT", "/v1/agent/service/register")
	r.obj = service
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) ServiceDeregister(serviceID string) error {
	r := a.c.newRequest("PUT", "/v1/agent/service/deregister/"+serviceID)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

//
func (a *Agent) PassTTL(checkID, note string) error {
	return a.updateTTL(checkID, note, "pass")
}

//
func (a *Agent) WarnTTL(checkID, note string) error {
	return a.updateTTL(checkID, note, "warn")
}

//
func (a *Agent) FailTTL(checkID, note string) error {
	return a.updateTTL(checkID, note, "fail")
}

//
func (a *Agent) updateTTL(checkID, note, status string) error {
	switch status {
	case "pass":
	case "warn":
	case "fail":
	default:
		return fmt.Errorf("Invalid status: %s", status)
	}
	endpoint := fmt.Sprintf("/v1/agent/check/%s/%s", status, checkID)
	r := a.c.newRequest("PUT", endpoint)
	r.params.Set("note", note)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

type checkUpdate struct {
	Status string

	Output string
}

func (a *Agent) UpdateTTL(checkID, output, status string) error {
	switch status {
	case "pass", HealthPassing:
		status = HealthPassing
	case "warn", HealthWarning:
		status = HealthWarning
	case "fail", HealthCritical:
		status = HealthCritical
	default:
		return fmt.Errorf("Invalid status: %s", status)
	}

	endpoint := fmt.Sprintf("/v1/agent/check/update/%s", checkID)
	r := a.c.newRequest("PUT", endpoint)
	r.obj = &checkUpdate{
		Status: status,
		Output: output,
	}

	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) CheckRegister(check *AgentCheckRegistration) error {
	r := a.c.newRequest("PUT", "/v1/agent/check/register")
	r.obj = check
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) CheckDeregister(checkID string) error {
	r := a.c.newRequest("PUT", "/v1/agent/check/deregister/"+checkID)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) Join(addr string, wan bool) error {
	r := a.c.newRequest("PUT", "/v1/agent/join/"+addr)
	if wan {
		r.params.Set("wan", "1")
	}
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) Leave() error {
	r := a.c.newRequest("PUT", "/v1/agent/leave")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) ForceLeave(node string) error {
	r := a.c.newRequest("PUT", "/v1/agent/force-leave/"+node)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) EnableServiceMaintenance(serviceID, reason string) error {
	r := a.c.newRequest("PUT", "/v1/agent/service/maintenance/"+serviceID)
	r.params.Set("enable", "true")
	r.params.Set("reason", reason)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) DisableServiceMaintenance(serviceID string) error {
	r := a.c.newRequest("PUT", "/v1/agent/service/maintenance/"+serviceID)
	r.params.Set("enable", "false")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) EnableNodeMaintenance(reason string) error {
	r := a.c.newRequest("PUT", "/v1/agent/maintenance")
	r.params.Set("enable", "true")
	r.params.Set("reason", reason)
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) DisableNodeMaintenance() error {
	r := a.c.newRequest("PUT", "/v1/agent/maintenance")
	r.params.Set("enable", "false")
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (a *Agent) Monitor(loglevel string, stopCh <-chan struct{}, q *QueryOptions) (chan string, error) {
	r := a.c.newRequest("GET", "/v1/agent/monitor")
	r.setQueryOptions(q)
	if loglevel != "" {
		r.params.Add("loglevel", loglevel)
	}
	_, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}

	logCh := make(chan string, 64)
	go func() {
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for {
			select {
			case <-stopCh:
				close(logCh)
				return
			default:
			}
			if scanner.Scan() {

				if text := scanner.Text(); text != "" {
					logCh <- text
				} else {
					logCh <- " "
				}
			} else {
				logCh <- ""
			}
		}
	}()

	return logCh, nil
}

func (a *Agent) UpdateACLToken(token string, q *WriteOptions) (*WriteMeta, error) {
	return a.updateToken("acl_token", token, q)
}

func (a *Agent) UpdateACLAgentToken(token string, q *WriteOptions) (*WriteMeta, error) {
	return a.updateToken("acl_agent_token", token, q)
}

func (a *Agent) UpdateACLAgentMasterToken(token string, q *WriteOptions) (*WriteMeta, error) {
	return a.updateToken("acl_agent_master_token", token, q)
}

func (a *Agent) UpdateACLReplicationToken(token string, q *WriteOptions) (*WriteMeta, error) {
	return a.updateToken("acl_replication_token", token, q)
}

func (a *Agent) updateToken(target, token string, q *WriteOptions) (*WriteMeta, error) {
	r := a.c.newRequest("PUT", fmt.Sprintf("/v1/agent/token/%s", target))
	r.setWriteOptions(q)
	r.obj = &AgentToken{Token: token}
	rtt, resp, err := requireOK(a.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	wm := &WriteMeta{RequestTime: rtt}
	return wm, nil
}
