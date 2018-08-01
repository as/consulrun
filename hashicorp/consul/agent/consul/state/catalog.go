package state

import (
	"fmt"
	"strings"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

func nodesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "nodes",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"uuid": {
				Name:         "uuid",
				AllowMissing: true,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},
			"meta": {
				Name:         "meta",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringMapFieldIndex{
					Field:     "Meta",
					Lowercase: false,
				},
			},
		},
	}
}

func servicesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "services",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "ServiceID",
							Lowercase: true,
						},
					},
				},
			},
			"node": {
				Name:         "node",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"service": {
				Name:         "service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "ServiceName",
					Lowercase: true,
				},
			},
		},
	}
}

func checksTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "checks",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "CheckID",
							Lowercase: true,
						},
					},
				},
			},
			"status": {
				Name:         "status",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Status",
					Lowercase: false,
				},
			},
			"service": {
				Name:         "service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "ServiceName",
					Lowercase: true,
				},
			},
			"node": {
				Name:         "node",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"node_service_check": {
				Name:         "node_service_check",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.FieldSetIndex{
							Field: "ServiceID",
						},
					},
				},
			},
			"node_service": {
				Name:         "node_service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "ServiceID",
							Lowercase: true,
						},
					},
				},
			},
		},
	}
}

func init() {
	registerSchema(nodesTableSchema)
	registerSchema(servicesTableSchema)
	registerSchema(checksTableSchema)
}

const (
	minUUIDLookupLen = 2
)

func resizeNodeLookupKey(s string) string {
	l := len(s)

	if l%2 != 0 {
		return s[0 : l-1]
	}

	return s
}

func (s *Snapshot) Nodes() (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("nodes", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Snapshot) Services(node string) (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("services", "node", node)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Snapshot) Checks(node string) (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("checks", "node", node)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Restore) Registration(idx uint64, req *structs.RegisterRequest) error {
	if err := s.store.ensureRegistrationTxn(s.tx, idx, req); err != nil {
		return err
	}
	return nil
}

func (s *Store) EnsureRegistration(idx uint64, req *structs.RegisterRequest) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureRegistrationTxn(tx, idx, req); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureRegistrationTxn(tx *memdb.Txn, idx uint64, req *structs.RegisterRequest) error {

	node := &structs.Node{
		ID:              req.ID,
		Node:            req.Node,
		Address:         req.Address,
		Datacenter:      req.Datacenter,
		TaggedAddresses: req.TaggedAddresses,
		Meta:            req.NodeMeta,
	}

	{
		existing, err := tx.First("nodes", "id", node.Node)
		if err != nil {
			return fmt.Errorf("node lookup failed: %s", err)
		}
		if existing == nil || req.ChangesNode(existing.(*structs.Node)) {
			if err := s.ensureNodeTxn(tx, idx, node); err != nil {
				return fmt.Errorf("failed inserting node: %s", err)
			}
		}
	}

	if req.Service != nil {
		existing, err := tx.First("services", "id", req.Node, req.Service.ID)
		if err != nil {
			return fmt.Errorf("failed service lookup: %s", err)
		}
		if existing == nil || !(existing.(*structs.ServiceNode).ToNodeService()).IsSame(req.Service) {
			if err := s.ensureServiceTxn(tx, idx, req.Node, req.Service); err != nil {
				return fmt.Errorf("failed inserting service: %s", err)

			}
		}
	}

	if req.Check != nil {
		if req.Check.Node != req.Node {
			return fmt.Errorf("check node %q does not match node %q",
				req.Check.Node, req.Node)
		}
		if err := s.ensureCheckTxn(tx, idx, req.Check); err != nil {
			return fmt.Errorf("failed inserting check: %s", err)
		}
	}
	for _, check := range req.Checks {
		if check.Node != req.Node {
			return fmt.Errorf("check node %q does not match node %q",
				check.Node, req.Node)
		}
		if err := s.ensureCheckTxn(tx, idx, check); err != nil {
			return fmt.Errorf("failed inserting check: %s", err)
		}
	}

	return nil
}

func (s *Store) EnsureNode(idx uint64, node *structs.Node) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureNodeTxn(tx, idx, node); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureNodeTxn(tx *memdb.Txn, idx uint64, node *structs.Node) error {

	var n *structs.Node
	if node.ID != "" {
		existing, err := tx.First("nodes", "uuid", string(node.ID))
		if err != nil {
			return fmt.Errorf("node lookup failed: %s", err)
		}
		if existing != nil {
			n = existing.(*structs.Node)
			if n.Node != node.Node {
				return fmt.Errorf("node ID %q for node %q aliases existing node %q",
					node.ID, node.Node, n.Node)
			}
		}
	}

	if n == nil {
		existing, err := tx.First("nodes", "id", node.Node)
		if err != nil {
			return fmt.Errorf("node name lookup failed: %s", err)
		}
		if existing != nil {
			n = existing.(*structs.Node)
		}
	}

	if n != nil {
		node.CreateIndex = n.CreateIndex
		node.ModifyIndex = idx
	} else {
		node.CreateIndex = idx
		node.ModifyIndex = idx
	}

	if err := tx.Insert("nodes", node); err != nil {
		return fmt.Errorf("failed inserting node: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"nodes", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) GetNode(id string) (uint64, *structs.Node, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")

	node, err := tx.First("nodes", "id", id)
	if err != nil {
		return 0, nil, fmt.Errorf("node lookup failed: %s", err)
	}
	if node != nil {
		return idx, node.(*structs.Node), nil
	}
	return idx, nil, nil
}

func (s *Store) GetNodeID(id types.NodeID) (uint64, *structs.Node, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")

	node, err := tx.First("nodes", "uuid", string(id))
	if err != nil {
		return 0, nil, fmt.Errorf("node lookup failed: %s", err)
	}
	if node != nil {
		return idx, node.(*structs.Node), nil
	}
	return idx, nil, nil
}

func (s *Store) Nodes(ws memdb.WatchSet) (uint64, structs.Nodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")

	nodes, err := tx.Get("nodes", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	ws.Add(nodes.WatchCh())

	var results structs.Nodes
	for node := nodes.Next(); node != nil; node = nodes.Next() {
		results = append(results, node.(*structs.Node))
	}
	return idx, results, nil
}

func (s *Store) NodesByMeta(ws memdb.WatchSet, filters map[string]string) (uint64, structs.Nodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")

	var args []interface{}
	for key, value := range filters {
		args = append(args, key, value)
		break
	}
	nodes, err := tx.Get("nodes", "meta", args...)
	if err != nil {
		return 0, nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	ws.Add(nodes.WatchCh())

	var results structs.Nodes
	for node := nodes.Next(); node != nil; node = nodes.Next() {
		n := node.(*structs.Node)
		if len(filters) <= 1 || structs.SatisfiesMetaFilters(n.Meta, filters) {
			results = append(results, n)
		}
	}
	return idx, results, nil
}

func (s *Store) DeleteNode(idx uint64, nodeName string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteNodeTxn(tx, idx, nodeName); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteNodeTxn(tx *memdb.Txn, idx uint64, nodeName string) error {

	node, err := tx.First("nodes", "id", nodeName)
	if err != nil {
		return fmt.Errorf("node lookup failed: %s", err)
	}
	if node == nil {
		return nil
	}

	services, err := tx.Get("services", "node", nodeName)
	if err != nil {
		return fmt.Errorf("failed service lookup: %s", err)
	}
	var sids []string
	for service := services.Next(); service != nil; service = services.Next() {
		svc := service.(*structs.ServiceNode)
		sids = append(sids, svc.ServiceID)
		if err := tx.Insert("index", &IndexEntry{serviceIndexName(svc.ServiceName), idx}); err != nil {
			return fmt.Errorf("failed updating index: %s", err)
		}
	}

	for _, sid := range sids {
		if err := s.deleteServiceTxn(tx, idx, nodeName, sid); err != nil {
			return err
		}
	}

	checks, err := tx.Get("checks", "node", nodeName)
	if err != nil {
		return fmt.Errorf("failed check lookup: %s", err)
	}
	var cids []types.CheckID
	for check := checks.Next(); check != nil; check = checks.Next() {
		cids = append(cids, check.(*structs.HealthCheck).CheckID)
	}

	for _, cid := range cids {
		if err := s.deleteCheckTxn(tx, idx, nodeName, cid); err != nil {
			return err
		}
	}

	coords, err := tx.Get("coordinates", "node", nodeName)
	if err != nil {
		return fmt.Errorf("failed coordinate lookup: %s", err)
	}
	for coord := coords.Next(); coord != nil; coord = coords.Next() {
		if err := tx.Delete("coordinates", coord); err != nil {
			return fmt.Errorf("failed deleting coordinate: %s", err)
		}
		if err := tx.Insert("index", &IndexEntry{"coordinates", idx}); err != nil {
			return fmt.Errorf("failed updating index: %s", err)
		}
	}

	if err := tx.Delete("nodes", node); err != nil {
		return fmt.Errorf("failed deleting node: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"nodes", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	sessions, err := tx.Get("sessions", "node", nodeName)
	if err != nil {
		return fmt.Errorf("failed session lookup: %s", err)
	}
	var ids []string
	for sess := sessions.Next(); sess != nil; sess = sessions.Next() {
		ids = append(ids, sess.(*structs.Session).ID)
	}

	for _, id := range ids {
		if err := s.deleteSessionTxn(tx, idx, id); err != nil {
			return fmt.Errorf("failed session delete: %s", err)
		}
	}

	return nil
}

func (s *Store) EnsureService(idx uint64, node string, svc *structs.NodeService) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureServiceTxn(tx, idx, node, svc); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureServiceTxn(tx *memdb.Txn, idx uint64, node string, svc *structs.NodeService) error {

	existing, err := tx.First("services", "id", node, svc.ID)
	if err != nil {
		return fmt.Errorf("failed service lookup: %s", err)
	}

	if err = structs.ValidateMetadata(svc.Meta, false); err != nil {
		return fmt.Errorf("Invalid Service Meta for node %s and serviceID %s: %v", node, svc.ID, err)
	}

	entry := svc.ToServiceNode(node)
	if existing != nil {
		entry.CreateIndex = existing.(*structs.ServiceNode).CreateIndex
		entry.ModifyIndex = idx
	} else {
		entry.CreateIndex = idx
		entry.ModifyIndex = idx
	}

	n, err := tx.First("nodes", "id", node)
	if err != nil {
		return fmt.Errorf("failed node lookup: %s", err)
	}
	if n == nil {
		return ErrMissingNode
	}

	if err := tx.Insert("services", entry); err != nil {
		return fmt.Errorf("failed inserting service: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"services", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{serviceIndexName(svc.Service), idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) Services(ws memdb.WatchSet) (uint64, structs.Services, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "services")

	services, err := tx.Get("services", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed querying services: %s", err)
	}
	ws.Add(services.WatchCh())

	unique := make(map[string]map[string]struct{})
	for service := services.Next(); service != nil; service = services.Next() {
		svc := service.(*structs.ServiceNode)
		tags, ok := unique[svc.ServiceName]
		if !ok {
			unique[svc.ServiceName] = make(map[string]struct{})
			tags = unique[svc.ServiceName]
		}
		for _, tag := range svc.ServiceTags {
			tags[tag] = struct{}{}
		}
	}

	var results = make(structs.Services)
	for service, tags := range unique {
		results[service] = make([]string, 0)
		for tag := range tags {
			results[service] = append(results[service], tag)
		}
	}
	return idx, results, nil
}

func (s *Store) ServicesByNodeMeta(ws memdb.WatchSet, filters map[string]string) (uint64, structs.Services, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "services", "nodes")

	var args []interface{}
	for key, value := range filters {
		args = append(args, key, value)
		break
	}
	nodes, err := tx.Get("nodes", "meta", args...)
	if err != nil {
		return 0, nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	ws.Add(nodes.WatchCh())

	allServices, err := tx.Get("services", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed services lookup: %s", err)
	}
	allServicesCh := allServices.WatchCh()

	unique := make(map[string]map[string]struct{})
	for node := nodes.Next(); node != nil; node = nodes.Next() {
		n := node.(*structs.Node)
		if len(filters) > 1 && !structs.SatisfiesMetaFilters(n.Meta, filters) {
			continue
		}

		services, err := tx.Get("services", "node", n.Node)
		if err != nil {
			return 0, nil, fmt.Errorf("failed querying services: %s", err)
		}
		ws.AddWithLimit(watchLimit, services.WatchCh(), allServicesCh)

		for service := services.Next(); service != nil; service = services.Next() {
			svc := service.(*structs.ServiceNode)
			tags, ok := unique[svc.ServiceName]
			if !ok {
				unique[svc.ServiceName] = make(map[string]struct{})
				tags = unique[svc.ServiceName]
			}
			for _, tag := range svc.ServiceTags {
				tags[tag] = struct{}{}
			}
		}
	}

	var results = make(structs.Services)
	for service, tags := range unique {
		results[service] = make([]string, 0)
		for tag := range tags {
			results[service] = append(results[service], tag)
		}
	}
	return idx, results, nil
}

func maxIndexForService(tx *memdb.Txn, serviceName string, checks bool) uint64 {
	transaction, err := tx.First("index", "id", serviceIndexName(serviceName))
	if err == nil {
		if idx, ok := transaction.(*IndexEntry); ok {
			return idx.Value
		}
	}
	if checks {
		return maxIndexTxn(tx, "nodes", "services", "checks")
	}
	return maxIndexTxn(tx, "nodes", "services")
}

func (s *Store) ServiceNodes(ws memdb.WatchSet, serviceName string) (uint64, structs.ServiceNodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexForService(tx, serviceName, false)

	services, err := tx.Get("services", "service", serviceName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed service lookup: %s", err)
	}
	ws.Add(services.WatchCh())

	var results structs.ServiceNodes
	for service := services.Next(); service != nil; service = services.Next() {
		results = append(results, service.(*structs.ServiceNode))
	}

	results, err = s.parseServiceNodes(tx, ws, results)
	if err != nil {
		return 0, nil, fmt.Errorf("failed parsing service nodes: %s", err)
	}
	return idx, results, nil
}

func (s *Store) ServiceTagNodes(ws memdb.WatchSet, service string, tag string) (uint64, structs.ServiceNodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexForService(tx, service, false)

	services, err := tx.Get("services", "service", service)
	if err != nil {
		return 0, nil, fmt.Errorf("failed service lookup: %s", err)
	}
	ws.Add(services.WatchCh())

	var results structs.ServiceNodes
	for service := services.Next(); service != nil; service = services.Next() {
		svc := service.(*structs.ServiceNode)
		if !serviceTagFilter(svc, tag) {
			results = append(results, svc)
		}
	}

	results, err = s.parseServiceNodes(tx, ws, results)
	if err != nil {
		return 0, nil, fmt.Errorf("failed parsing service nodes: %s", err)
	}
	return idx, results, nil
}

func serviceTagFilter(sn *structs.ServiceNode, tag string) bool {
	tag = strings.ToLower(tag)

	for _, t := range sn.ServiceTags {
		if strings.ToLower(t) == tag {
			return false
		}
	}

	return true
}

func (s *Store) parseServiceNodes(tx *memdb.Txn, ws memdb.WatchSet, services structs.ServiceNodes) (structs.ServiceNodes, error) {

	allNodes, err := tx.Get("nodes", "id")
	if err != nil {
		return nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	allNodesCh := allNodes.WatchCh()

	var results structs.ServiceNodes
	for _, sn := range services {

		s := sn.PartialClone()

		watchCh, n, err := tx.FirstWatch("nodes", "id", sn.Node)
		if err != nil {
			return nil, fmt.Errorf("failed node lookup: %s", err)
		}
		ws.AddWithLimit(watchLimit, watchCh, allNodesCh)

		node := n.(*structs.Node)
		s.ID = node.ID
		s.Address = node.Address
		s.Datacenter = node.Datacenter
		s.TaggedAddresses = node.TaggedAddresses
		s.NodeMeta = node.Meta

		results = append(results, s)
	}
	return results, nil
}

func (s *Store) NodeService(nodeName string, serviceID string) (uint64, *structs.NodeService, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "services")

	service, err := tx.First("services", "id", nodeName, serviceID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed querying service for node %q: %s", nodeName, err)
	}

	if service != nil {
		return idx, service.(*structs.ServiceNode).ToNodeService(), nil
	}
	return idx, nil, nil
}

func (s *Store) NodeServices(ws memdb.WatchSet, nodeNameOrID string) (uint64, *structs.NodeServices, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes", "services")

	watchCh, n, err := tx.FirstWatch("nodes", "id", nodeNameOrID)
	if err != nil {
		return 0, nil, fmt.Errorf("node lookup failed: %s", err)
	}

	if n != nil {
		ws.Add(watchCh)
	} else {
		if len(nodeNameOrID) < minUUIDLookupLen {
			ws.Add(watchCh)
			return 0, nil, nil
		}

		iter, err := tx.Get("nodes", "uuid_prefix", resizeNodeLookupKey(nodeNameOrID))
		if err != nil {
			ws.Add(watchCh)

			return 0, nil, nil
		}

		n = iter.Next()
		if n == nil {

			ws.Add(watchCh)
			return 0, nil, nil
		}

		idWatchCh := iter.WatchCh()
		if iter.Next() != nil {

			ws.Add(watchCh)
			return 0, nil, nil
		}

		ws.Add(idWatchCh)
	}

	node := n.(*structs.Node)
	nodeName := node.Node

	services, err := tx.Get("services", "node", nodeName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed querying services for node %q: %s", nodeName, err)
	}
	ws.Add(services.WatchCh())

	ns := &structs.NodeServices{
		Node:     node,
		Services: make(map[string]*structs.NodeService),
	}

	for service := services.Next(); service != nil; service = services.Next() {
		svc := service.(*structs.ServiceNode).ToNodeService()
		ns.Services[svc.ID] = svc
	}

	return idx, ns, nil
}

func (s *Store) DeleteService(idx uint64, nodeName, serviceID string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteServiceTxn(tx, idx, nodeName, serviceID); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func serviceIndexName(name string) string {
	return fmt.Sprintf("service.%s", name)
}

func (s *Store) deleteServiceTxn(tx *memdb.Txn, idx uint64, nodeName, serviceID string) error {

	service, err := tx.First("services", "id", nodeName, serviceID)
	if err != nil {
		return fmt.Errorf("failed service lookup: %s", err)
	}
	if service == nil {
		return nil
	}

	checks, err := tx.Get("checks", "node_service", nodeName, serviceID)
	if err != nil {
		return fmt.Errorf("failed service check lookup: %s", err)
	}
	var cids []types.CheckID
	for check := checks.Next(); check != nil; check = checks.Next() {
		cids = append(cids, check.(*structs.HealthCheck).CheckID)
	}

	for _, cid := range cids {
		if err := s.deleteCheckTxn(tx, idx, nodeName, cid); err != nil {
			return err
		}
	}

	if err := tx.Insert("index", &IndexEntry{"checks", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	if err := tx.Delete("services", service); err != nil {
		return fmt.Errorf("failed deleting service: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"services", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	svc := service.(*structs.ServiceNode)
	if remainingService, err := tx.First("services", "service", svc.ServiceName); err == nil {
		if remainingService != nil {

			if err := tx.Insert("index", &IndexEntry{serviceIndexName(svc.ServiceName), idx}); err != nil {
				return fmt.Errorf("failed updating index: %s", err)
			}
		} else {

			serviceIndex, err := tx.First("index", "id", serviceIndexName(svc.ServiceName))
			if err == nil && serviceIndex != nil {

				if errW := tx.Delete("index", serviceIndex); errW != nil {
					return fmt.Errorf("[FAILED] deleting serviceIndex %s: %s", svc.ServiceName, err)
				}
			}
		}
	} else {
		return fmt.Errorf("Could not find any service %s: %s", svc.ServiceName, err)
	}
	return nil
}

func (s *Store) EnsureCheck(idx uint64, hc *structs.HealthCheck) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureCheckTxn(tx, idx, hc); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureCheckTxn(tx *memdb.Txn, idx uint64, hc *structs.HealthCheck) error {

	existing, err := tx.First("checks", "id", hc.Node, string(hc.CheckID))
	if err != nil {
		return fmt.Errorf("failed health check lookup: %s", err)
	}

	if existing != nil {
		hc.CreateIndex = existing.(*structs.HealthCheck).CreateIndex
		hc.ModifyIndex = idx
	} else {
		hc.CreateIndex = idx
		hc.ModifyIndex = idx
	}

	if hc.Status == "" {
		hc.Status = api.HealthCritical
	}

	node, err := tx.First("nodes", "id", hc.Node)
	if err != nil {
		return fmt.Errorf("failed node lookup: %s", err)
	}
	if node == nil {
		return ErrMissingNode
	}

	if hc.ServiceID != "" {
		service, err := tx.First("services", "id", hc.Node, hc.ServiceID)
		if err != nil {
			return fmt.Errorf("failed service lookup: %s", err)
		}
		if service == nil {
			return ErrMissingService
		}

		svc := service.(*structs.ServiceNode)
		hc.ServiceName = svc.ServiceName
		hc.ServiceTags = svc.ServiceTags
		if err = tx.Insert("index", &IndexEntry{serviceIndexName(svc.ServiceName), idx}); err != nil {
			return fmt.Errorf("failed updating index: %s", err)
		}
	} else {

		services, err := tx.Get("services", "node", hc.Node)
		if err != nil {
			return fmt.Errorf("failed updating services for node %s: %s", hc.Node, err)
		}
		for service := services.Next(); service != nil; service = services.Next() {
			svc := service.(*structs.ServiceNode).ToNodeService()
			if err := tx.Insert("index", &IndexEntry{serviceIndexName(svc.Service), idx}); err != nil {
				return fmt.Errorf("failed updating index: %s", err)
			}
		}
	}

	if hc.Status == api.HealthCritical {
		mappings, err := tx.Get("session_checks", "node_check", hc.Node, string(hc.CheckID))
		if err != nil {
			return fmt.Errorf("failed session checks lookup: %s", err)
		}

		var ids []string
		for mapping := mappings.Next(); mapping != nil; mapping = mappings.Next() {
			ids = append(ids, mapping.(*sessionCheck).Session)
		}

		for _, id := range ids {
			if err := s.deleteSessionTxn(tx, idx, id); err != nil {
				return fmt.Errorf("failed deleting session: %s", err)
			}
		}
	}

	if err := tx.Insert("checks", hc); err != nil {
		return fmt.Errorf("failed inserting check: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"checks", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) NodeCheck(nodeName string, checkID types.CheckID) (uint64, *structs.HealthCheck, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "checks")

	check, err := tx.First("checks", "id", nodeName, string(checkID))
	if err != nil {
		return 0, nil, fmt.Errorf("failed check lookup: %s", err)
	}

	if check != nil {
		return idx, check.(*structs.HealthCheck), nil
	}
	return idx, nil, nil
}

func (s *Store) NodeChecks(ws memdb.WatchSet, nodeName string) (uint64, structs.HealthChecks, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "checks")

	iter, err := tx.Get("checks", "node", nodeName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed check lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.HealthChecks
	for check := iter.Next(); check != nil; check = iter.Next() {
		results = append(results, check.(*structs.HealthCheck))
	}
	return idx, results, nil
}

func (s *Store) ServiceChecks(ws memdb.WatchSet, serviceName string) (uint64, structs.HealthChecks, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "checks")

	iter, err := tx.Get("checks", "service", serviceName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed check lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.HealthChecks
	for check := iter.Next(); check != nil; check = iter.Next() {
		results = append(results, check.(*structs.HealthCheck))
	}
	return idx, results, nil
}

func (s *Store) ServiceChecksByNodeMeta(ws memdb.WatchSet, serviceName string,
	filters map[string]string) (uint64, structs.HealthChecks, error) {

	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexForService(tx, serviceName, true)

	iter, err := tx.Get("checks", "service", serviceName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed check lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	return s.parseChecksByNodeMeta(tx, ws, idx, iter, filters)
}

func (s *Store) ChecksInState(ws memdb.WatchSet, state string) (uint64, structs.HealthChecks, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "checks")

	var iter memdb.ResultIterator
	var err error
	if state == api.HealthAny {
		iter, err = tx.Get("checks", "status")
	} else {
		iter, err = tx.Get("checks", "status", state)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("failed check lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.HealthChecks
	for check := iter.Next(); check != nil; check = iter.Next() {
		results = append(results, check.(*structs.HealthCheck))
	}
	return idx, results, nil
}

func (s *Store) ChecksInStateByNodeMeta(ws memdb.WatchSet, state string, filters map[string]string) (uint64, structs.HealthChecks, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes", "checks")

	var iter memdb.ResultIterator
	var err error
	if state == api.HealthAny {
		iter, err = tx.Get("checks", "status")
		if err != nil {
			return 0, nil, fmt.Errorf("failed check lookup: %s", err)
		}
	} else {
		iter, err = tx.Get("checks", "status", state)
		if err != nil {
			return 0, nil, fmt.Errorf("failed check lookup: %s", err)
		}
	}
	ws.Add(iter.WatchCh())

	return s.parseChecksByNodeMeta(tx, ws, idx, iter, filters)
}

func (s *Store) parseChecksByNodeMeta(tx *memdb.Txn, ws memdb.WatchSet,
	idx uint64, iter memdb.ResultIterator, filters map[string]string) (uint64, structs.HealthChecks, error) {

	allNodes, err := tx.Get("nodes", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	allNodesCh := allNodes.WatchCh()

	var results structs.HealthChecks
	for check := iter.Next(); check != nil; check = iter.Next() {
		healthCheck := check.(*structs.HealthCheck)
		watchCh, node, err := tx.FirstWatch("nodes", "id", healthCheck.Node)
		if err != nil {
			return 0, nil, fmt.Errorf("failed node lookup: %s", err)
		}
		if node == nil {
			return 0, nil, ErrMissingNode
		}

		ws.AddWithLimit(watchLimit, watchCh, allNodesCh)
		if structs.SatisfiesMetaFilters(node.(*structs.Node).Meta, filters) {
			results = append(results, healthCheck)
		}
	}
	return idx, results, nil
}

func (s *Store) DeleteCheck(idx uint64, node string, checkID types.CheckID) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteCheckTxn(tx, idx, node, checkID); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteCheckTxn(tx *memdb.Txn, idx uint64, node string, checkID types.CheckID) error {

	hc, err := tx.First("checks", "id", node, string(checkID))
	if err != nil {
		return fmt.Errorf("check lookup failed: %s", err)
	}
	if hc == nil {
		return nil
	}
	existing := hc.(*structs.HealthCheck)
	if existing != nil && existing.ServiceID != "" {
		service, err := tx.First("services", "id", node, existing.ServiceID)
		if err != nil {
			return fmt.Errorf("failed service lookup: %s", err)
		}
		if service == nil {
			return ErrMissingService
		}

		svc := service.(*structs.ServiceNode)
		if err = tx.Insert("index", &IndexEntry{serviceIndexName(svc.ServiceName), idx}); err != nil {
			return fmt.Errorf("failed updating index: %s", err)
		}
	}

	if err := tx.Delete("checks", hc); err != nil {
		return fmt.Errorf("failed removing check: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"checks", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	mappings, err := tx.Get("session_checks", "node_check", node, string(checkID))
	if err != nil {
		return fmt.Errorf("failed session checks lookup: %s", err)
	}
	var ids []string
	for mapping := mappings.Next(); mapping != nil; mapping = mappings.Next() {
		ids = append(ids, mapping.(*sessionCheck).Session)
	}

	for _, id := range ids {
		if err := s.deleteSessionTxn(tx, idx, id); err != nil {
			return fmt.Errorf("failed deleting session: %s", err)
		}
	}

	return nil
}

func (s *Store) CheckServiceNodes(ws memdb.WatchSet, serviceName string) (uint64, structs.CheckServiceNodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexForService(tx, serviceName, true)

	iter, err := tx.Get("services", "service", serviceName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed service lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.ServiceNodes
	for service := iter.Next(); service != nil; service = iter.Next() {
		results = append(results, service.(*structs.ServiceNode))
	}
	return s.parseCheckServiceNodes(tx, ws, idx, serviceName, results, err)
}

func (s *Store) CheckServiceTagNodes(ws memdb.WatchSet, serviceName, tag string) (uint64, structs.CheckServiceNodes, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexForService(tx, serviceName, true)

	iter, err := tx.Get("services", "service", serviceName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed service lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.ServiceNodes
	for service := iter.Next(); service != nil; service = iter.Next() {
		svc := service.(*structs.ServiceNode)
		if !serviceTagFilter(svc, tag) {
			results = append(results, svc)
		}
	}
	return s.parseCheckServiceNodes(tx, ws, idx, serviceName, results, err)
}

func (s *Store) parseCheckServiceNodes(
	tx *memdb.Txn, ws memdb.WatchSet, idx uint64,
	serviceName string, services structs.ServiceNodes,
	err error) (uint64, structs.CheckServiceNodes, error) {
	if err != nil {
		return 0, nil, err
	}

	if len(services) == 0 {
		return idx, nil, nil
	}

	allNodes, err := tx.Get("nodes", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed nodes lookup: %s", err)
	}
	allNodesCh := allNodes.WatchCh()

	allChecks, err := tx.Get("checks", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed checks lookup: %s", err)
	}
	allChecksCh := allChecks.WatchCh()

	results := make(structs.CheckServiceNodes, 0, len(services))
	for _, sn := range services {

		watchCh, n, err := tx.FirstWatch("nodes", "id", sn.Node)
		if err != nil {
			return 0, nil, fmt.Errorf("failed node lookup: %s", err)
		}
		ws.AddWithLimit(watchLimit, watchCh, allNodesCh)

		if n == nil {
			return 0, nil, ErrMissingNode
		}
		node := n.(*structs.Node)

		var checks structs.HealthChecks
		iter, err := tx.Get("checks", "node_service_check", sn.Node, false)
		if err != nil {
			return 0, nil, err
		}
		ws.AddWithLimit(watchLimit, iter.WatchCh(), allChecksCh)
		for check := iter.Next(); check != nil; check = iter.Next() {
			checks = append(checks, check.(*structs.HealthCheck))
		}

		iter, err = tx.Get("checks", "node_service", sn.Node, sn.ServiceID)
		if err != nil {
			return 0, nil, err
		}
		ws.AddWithLimit(watchLimit, iter.WatchCh(), allChecksCh)
		for check := iter.Next(); check != nil; check = iter.Next() {
			checks = append(checks, check.(*structs.HealthCheck))
		}

		results = append(results, structs.CheckServiceNode{
			Node:    node,
			Service: sn.ToNodeService(),
			Checks:  checks,
		})
	}

	return idx, results, nil
}

func (s *Store) NodeInfo(ws memdb.WatchSet, node string) (uint64, structs.NodeDump, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes", "services", "checks")

	nodes, err := tx.Get("nodes", "id", node)
	if err != nil {
		return 0, nil, fmt.Errorf("failed node lookup: %s", err)
	}
	ws.Add(nodes.WatchCh())
	return s.parseNodes(tx, ws, idx, nodes)
}

func (s *Store) NodeDump(ws memdb.WatchSet) (uint64, structs.NodeDump, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes", "services", "checks")

	nodes, err := tx.Get("nodes", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed node lookup: %s", err)
	}
	ws.Add(nodes.WatchCh())
	return s.parseNodes(tx, ws, idx, nodes)
}

func (s *Store) parseNodes(tx *memdb.Txn, ws memdb.WatchSet, idx uint64,
	iter memdb.ResultIterator) (uint64, structs.NodeDump, error) {

	allServices, err := tx.Get("services", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed services lookup: %s", err)
	}
	allServicesCh := allServices.WatchCh()

	allChecks, err := tx.Get("checks", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed checks lookup: %s", err)
	}
	allChecksCh := allChecks.WatchCh()

	var results structs.NodeDump
	for n := iter.Next(); n != nil; n = iter.Next() {
		node := n.(*structs.Node)

		dump := &structs.NodeInfo{
			ID:              node.ID,
			Node:            node.Node,
			Address:         node.Address,
			TaggedAddresses: node.TaggedAddresses,
			Meta:            node.Meta,
		}

		services, err := tx.Get("services", "node", node.Node)
		if err != nil {
			return 0, nil, fmt.Errorf("failed services lookup: %s", err)
		}
		ws.AddWithLimit(watchLimit, services.WatchCh(), allServicesCh)
		for service := services.Next(); service != nil; service = services.Next() {
			ns := service.(*structs.ServiceNode).ToNodeService()
			dump.Services = append(dump.Services, ns)
		}

		checks, err := tx.Get("checks", "node", node.Node)
		if err != nil {
			return 0, nil, fmt.Errorf("failed node lookup: %s", err)
		}
		ws.AddWithLimit(watchLimit, checks.WatchCh(), allChecksCh)
		for check := checks.Next(); check != nil; check = checks.Next() {
			hc := check.(*structs.HealthCheck)
			dump.Checks = append(dump.Checks, hc)
		}

		results = append(results, dump)
	}
	return idx, results, nil
}
