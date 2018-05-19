package local

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/token"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/types"
)

type Config struct {
	AdvertiseAddr       string
	CheckUpdateInterval time.Duration
	Datacenter          string
	DiscardCheckOutput  bool
	NodeID              types.NodeID
	NodeName            string
	TaggedAddresses     map[string]string
}

type ServiceState struct {
	Service *structs.NodeService

	Token string

	InSync bool

	Deleted bool
}

func (s *ServiceState) Clone() *ServiceState {
	s2 := new(ServiceState)
	*s2 = *s
	return s2
}

type CheckState struct {
	Check *structs.HealthCheck

	Token string

	CriticalTime time.Time

	DeferCheck *time.Timer

	InSync bool

	Deleted bool
}

func (c *CheckState) Clone() *CheckState {
	c2 := new(CheckState)
	*c2 = *c
	return c2
}

func (c *CheckState) Critical() bool {
	return !c.CriticalTime.IsZero()
}

func (c *CheckState) CriticalFor() time.Duration {
	return time.Since(c.CriticalTime)
}

type rpc interface {
	RPC(method string, args interface{}, reply interface{}) error
}

type State struct {
	sync.RWMutex

	//

	Delegate rpc

	//

	TriggerSyncChanges func()

	logger *log.Logger

	config Config

	nodeInfoInSync bool

	services map[string]*ServiceState

	checks map[types.CheckID]*CheckState

	metadata map[string]string

	discardCheckOutput atomic.Value // bool

	tokens *token.Store
}

func NewState(c Config, lg *log.Logger, tokens *token.Store) *State {
	l := &State{
		config:   c,
		logger:   lg,
		services: make(map[string]*ServiceState),
		checks:   make(map[types.CheckID]*CheckState),
		metadata: make(map[string]string),
		tokens:   tokens,
	}
	l.SetDiscardCheckOutput(c.DiscardCheckOutput)
	return l
}

func (l *State) SetDiscardCheckOutput(b bool) {
	l.discardCheckOutput.Store(b)
}

func (l *State) ServiceToken(id string) string {
	l.RLock()
	defer l.RUnlock()
	return l.serviceToken(id)
}

func (l *State) serviceToken(id string) string {
	var token string
	if s := l.services[id]; s != nil {
		token = s.Token
	}
	if token == "" {
		token = l.tokens.UserToken()
	}
	return token
}

func (l *State) AddService(service *structs.NodeService, token string) error {
	if service == nil {
		return fmt.Errorf("no service")
	}

	if service.ID == "" {
		service.ID = service.Service
	}

	l.SetServiceState(&ServiceState{
		Service: service,
		Token:   token,
	})
	return nil
}

func (l *State) RemoveService(id string) error {
	l.Lock()
	defer l.Unlock()

	s := l.services[id]
	if s == nil || s.Deleted {
		return fmt.Errorf("Service %q does not exist", id)
	}

	s.InSync = false
	s.Deleted = true
	l.TriggerSyncChanges()

	return nil
}

func (l *State) Service(id string) *structs.NodeService {
	l.RLock()
	defer l.RUnlock()

	s := l.services[id]
	if s == nil || s.Deleted {
		return nil
	}
	return s.Service
}

func (l *State) Services() map[string]*structs.NodeService {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]*structs.NodeService)
	for id, s := range l.services {
		if s.Deleted {
			continue
		}
		m[id] = s.Service
	}
	return m
}

func (l *State) ServiceState(id string) *ServiceState {
	l.RLock()
	defer l.RUnlock()

	s := l.services[id]
	if s == nil || s.Deleted {
		return nil
	}
	return s.Clone()
}

func (l *State) SetServiceState(s *ServiceState) {
	l.Lock()
	defer l.Unlock()

	l.services[s.Service.ID] = s
	l.TriggerSyncChanges()
}

func (l *State) ServiceStates() map[string]*ServiceState {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]*ServiceState)
	for id, s := range l.services {
		if s.Deleted {
			continue
		}
		m[id] = s.Clone()
	}
	return m
}

func (l *State) CheckToken(checkID types.CheckID) string {
	l.RLock()
	defer l.RUnlock()
	return l.checkToken(checkID)
}

func (l *State) checkToken(id types.CheckID) string {
	var token string
	c := l.checks[id]
	if c != nil {
		token = c.Token
	}
	if token == "" {
		token = l.tokens.UserToken()
	}
	return token
}

func (l *State) AddCheck(check *structs.HealthCheck, token string) error {
	if check == nil {
		return fmt.Errorf("no check")
	}

	check = check.Clone()

	if l.discardCheckOutput.Load().(bool) {
		check.Output = ""
	}

	if check.ServiceID != "" && l.Service(check.ServiceID) == nil {
		return fmt.Errorf("Check %q refers to non-existent service %q", check.CheckID, check.ServiceID)
	}

	check.Node = l.config.NodeName

	l.SetCheckState(&CheckState{
		Check: check,
		Token: token,
	})
	return nil
}

func (l *State) RemoveCheck(id types.CheckID) error {
	l.Lock()
	defer l.Unlock()

	c := l.checks[id]
	if c == nil || c.Deleted {
		return fmt.Errorf("Check %q does not exist", id)
	}

	c.InSync = false
	c.Deleted = true
	l.TriggerSyncChanges()

	return nil
}

func (l *State) UpdateCheck(id types.CheckID, status, output string) {
	l.Lock()
	defer l.Unlock()

	c := l.checks[id]
	if c == nil || c.Deleted {
		return
	}

	if l.discardCheckOutput.Load().(bool) {
		output = ""
	}

	if status == api.HealthCritical {
		if !c.Critical() {
			c.CriticalTime = time.Now()
		}
	} else {
		c.CriticalTime = time.Time{}
	}

	if c.Check.Status == status && c.Check.Output == output {
		return
	}

	if l.config.CheckUpdateInterval > 0 && c.Check.Status == status {
		c.Check.Output = output
		if c.DeferCheck == nil {
			d := l.config.CheckUpdateInterval
			intv := time.Duration(uint64(d)/2) + lib.RandomStagger(d)
			c.DeferCheck = time.AfterFunc(intv, func() {
				l.Lock()
				defer l.Unlock()

				c := l.checks[id]
				if c == nil {
					return
				}
				c.DeferCheck = nil
				if c.Deleted {
					return
				}
				c.InSync = false
				l.TriggerSyncChanges()
			})
		}
		return
	}

	c.Check.Status = status
	c.Check.Output = output
	c.InSync = false
	l.TriggerSyncChanges()
}

func (l *State) Check(id types.CheckID) *structs.HealthCheck {
	l.RLock()
	defer l.RUnlock()

	c := l.checks[id]
	if c == nil || c.Deleted {
		return nil
	}
	return c.Check
}

func (l *State) Checks() map[types.CheckID]*structs.HealthCheck {
	m := make(map[types.CheckID]*structs.HealthCheck)
	for id, c := range l.CheckStates() {
		m[id] = c.Check
	}
	return m
}

func (l *State) CheckState(id types.CheckID) *CheckState {
	l.RLock()
	defer l.RUnlock()

	c := l.checks[id]
	if c == nil || c.Deleted {
		return nil
	}
	return c.Clone()
}

func (l *State) SetCheckState(c *CheckState) {
	l.Lock()
	defer l.Unlock()

	l.checks[c.Check.CheckID] = c
	l.TriggerSyncChanges()
}

func (l *State) CheckStates() map[types.CheckID]*CheckState {
	l.RLock()
	defer l.RUnlock()

	m := make(map[types.CheckID]*CheckState)
	for id, c := range l.checks {
		if c.Deleted {
			continue
		}
		m[id] = c.Clone()
	}
	return m
}

func (l *State) CriticalCheckStates() map[types.CheckID]*CheckState {
	l.RLock()
	defer l.RUnlock()

	m := make(map[types.CheckID]*CheckState)
	for id, c := range l.checks {
		if c.Deleted || !c.Critical() {
			continue
		}
		m[id] = c.Clone()
	}
	return m
}

func (l *State) Metadata() map[string]string {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]string)
	for k, v := range l.metadata {
		m[k] = v
	}
	return m
}

func (l *State) LoadMetadata(data map[string]string) error {
	l.Lock()
	defer l.Unlock()

	for k, v := range data {
		l.metadata[k] = v
	}
	l.TriggerSyncChanges()
	return nil
}

func (l *State) UnloadMetadata() {
	l.Lock()
	defer l.Unlock()
	l.metadata = make(map[string]string)
}

func (l *State) Stats() map[string]string {
	l.RLock()
	defer l.RUnlock()

	services := 0
	for _, s := range l.services {
		if s.Deleted {
			continue
		}
		services++
	}

	checks := 0
	for _, c := range l.checks {
		if c.Deleted {
			continue
		}
		checks++
	}

	return map[string]string{
		"services": strconv.Itoa(services),
		"checks":   strconv.Itoa(checks),
	}
}

func (l *State) updateSyncState() error {

	req := structs.NodeSpecificRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		QueryOptions: structs.QueryOptions{Token: l.tokens.AgentToken()},
	}

	var out1 structs.IndexedNodeServices
	if err := l.Delegate.RPC("Catalog.NodeServices", &req, &out1); err != nil {
		return err
	}

	var out2 structs.IndexedHealthChecks
	if err := l.Delegate.RPC("Health.NodeChecks", &req, &out2); err != nil {
		return err
	}

	remoteServices := make(map[string]*structs.NodeService)
	if out1.NodeServices != nil {
		remoteServices = out1.NodeServices.Services
	}

	remoteChecks := make(map[types.CheckID]*structs.HealthCheck, len(out2.HealthChecks))
	for _, rc := range out2.HealthChecks {
		remoteChecks[rc.CheckID] = rc
	}

	l.Lock()
	defer l.Unlock()

	if out1.NodeServices == nil || out1.NodeServices.Node == nil ||
		out1.NodeServices.Node.ID != l.config.NodeID ||
		!reflect.DeepEqual(out1.NodeServices.Node.TaggedAddresses, l.config.TaggedAddresses) ||
		!reflect.DeepEqual(out1.NodeServices.Node.Meta, l.metadata) {
		l.nodeInfoInSync = false
	}

	for id, s := range l.services {
		if remoteServices[id] == nil {
			s.InSync = false
		}
	}

	for id, rs := range remoteServices {
		ls := l.services[id]
		if ls == nil {

			if id == structs.ConsulServiceID {
				continue
			}

			l.services[id] = &ServiceState{Deleted: true}
			continue
		}

		if ls.Deleted {
			continue
		}

		if ls.Service.EnableTagOverride {
			ls.Service.Tags = make([]string, len(rs.Tags))
			copy(ls.Service.Tags, rs.Tags)
		}
		ls.InSync = ls.Service.IsSame(rs)
	}

	for id, c := range l.checks {
		if remoteChecks[id] == nil {
			c.InSync = false
		}
	}

	for id, rc := range remoteChecks {
		lc := l.checks[id]

		if lc == nil {

			if id == structs.SerfCheckID {
				l.logger.Printf("[DEBUG] agent: Skipping remote check %q since it is managed automatically", id)
				continue
			}

			l.checks[id] = &CheckState{Deleted: true}
			continue
		}

		if lc.Deleted {
			continue
		}

		if l.config.CheckUpdateInterval == 0 {
			lc.InSync = lc.Check.IsSame(rc)
			continue
		}

		lcCopy := lc.Check.Clone()

		rcCopy := rc.Clone()

		if lc.DeferCheck != nil {
			lcCopy.Output = ""
			rcCopy.Output = ""
		}
		lc.InSync = lcCopy.IsSame(rcCopy)
	}
	return nil
}

func (l *State) SyncFull() error {

	//

	if err := l.updateSyncState(); err != nil {
		return err
	}
	return l.SyncChanges()
}

func (l *State) SyncChanges() error {
	l.Lock()
	defer l.Unlock()

	for id, s := range l.services {
		var err error
		switch {
		case s.Deleted:
			err = l.deleteService(id)
		case !s.InSync:
			err = l.syncService(id)
		default:
			l.logger.Printf("[DEBUG] agent: Service %q in sync", id)
		}
		if err != nil {
			return err
		}
	}

	for id, c := range l.checks {
		var err error
		switch {
		case c.Deleted:
			err = l.deleteCheck(id)
		case !c.InSync:
			if c.DeferCheck != nil {
				c.DeferCheck.Stop()
				c.DeferCheck = nil
			}
			err = l.syncCheck(id)
		default:
			l.logger.Printf("[DEBUG] agent: Check %q in sync", id)
		}
		if err != nil {
			return err
		}
	}

	if l.nodeInfoInSync {
		l.logger.Printf("[DEBUG] agent: Node info in sync")
		return nil
	}
	return l.syncNodeInfo()
}

func (l *State) deleteService(id string) error {
	if id == "" {
		return fmt.Errorf("ServiceID missing")
	}

	req := structs.DeregisterRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		ServiceID:    id,
		WriteRequest: structs.WriteRequest{Token: l.serviceToken(id)},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Deregister", &req, &out)
	switch {
	case err == nil || strings.Contains(err.Error(), "Unknown service"):
		delete(l.services, id)
		l.logger.Printf("[INFO] agent: Deregistered service %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):

		l.services[id].InSync = true
		l.logger.Printf("[WARN] agent: Service %q deregistration blocked by ACLs", id)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Deregistering service %q failed. %s", id, err)
		return err
	}
}

func (l *State) deleteCheck(id types.CheckID) error {
	if id == "" {
		return fmt.Errorf("CheckID missing")
	}

	req := structs.DeregisterRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		CheckID:      id,
		WriteRequest: structs.WriteRequest{Token: l.checkToken(id)},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Deregister", &req, &out)
	switch {
	case err == nil || strings.Contains(err.Error(), "Unknown check"):
		c := l.checks[id]
		if c != nil && c.DeferCheck != nil {
			c.DeferCheck.Stop()
		}
		delete(l.checks, id)
		l.logger.Printf("[INFO] agent: Deregistered check %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):

		l.checks[id].InSync = true
		l.logger.Printf("[WARN] agent: Check %q deregistration blocked by ACLs", id)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Deregistering check %q failed. %s", id, err)
		return err
	}
}

func (l *State) syncService(id string) error {

	var checks structs.HealthChecks
	for checkID, c := range l.checks {
		if c.Deleted || c.InSync {
			continue
		}
		if c.Check.ServiceID != id {
			continue
		}
		if l.serviceToken(id) != l.checkToken(checkID) {
			continue
		}
		checks = append(checks, c.Check)
	}

	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.metadata,
		Service:         l.services[id].Service,
		WriteRequest:    structs.WriteRequest{Token: l.serviceToken(id)},
	}

	if len(checks) == 1 {
		req.Check = checks[0]
	} else {
		req.Checks = checks
	}

	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		l.services[id].InSync = true

		l.nodeInfoInSync = true
		for _, check := range checks {
			l.checks[check.CheckID].InSync = true
		}
		l.logger.Printf("[INFO] agent: Synced service %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):

		l.services[id].InSync = true
		for _, check := range checks {
			l.checks[check.CheckID].InSync = true
		}
		l.logger.Printf("[WARN] agent: Service %q registration blocked by ACLs", id)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing service %q failed. %s", id, err)
		return err
	}
}

func (l *State) syncCheck(id types.CheckID) error {
	c := l.checks[id]

	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.metadata,
		Check:           c.Check,
		WriteRequest:    structs.WriteRequest{Token: l.checkToken(id)},
	}

	s := l.services[c.Check.ServiceID]
	if s != nil && !s.Deleted {
		req.Service = s.Service
	}

	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		l.checks[id].InSync = true

		l.nodeInfoInSync = true
		l.logger.Printf("[INFO] agent: Synced check %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):

		l.checks[id].InSync = true
		l.logger.Printf("[WARN] agent: Check %q registration blocked by ACLs", id)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing check %q failed. %s", id, err)
		return err
	}
}

func (l *State) syncNodeInfo() error {
	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.metadata,
		WriteRequest:    structs.WriteRequest{Token: l.tokens.AgentToken()},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		l.nodeInfoInSync = true
		l.logger.Printf("[INFO] agent: Synced node info")
		return nil

	case acl.IsErrPermissionDenied(err):

		l.nodeInfoInSync = true
		l.logger.Printf("[WARN] agent: Node info update blocked by ACLs")
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing node info failed. %s", err)
		return err
	}
}
