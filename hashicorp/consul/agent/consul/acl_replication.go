package consul

import (
	"fmt"
	"sort"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
)

type aclIterator struct {
	acls structs.ACLs

	index int
}

func newACLIterator(acls structs.ACLs) *aclIterator {
	return &aclIterator{acls: acls}
}

func (a *aclIterator) Len() int {
	return len(a.acls)
}

func (a *aclIterator) Swap(i, j int) {
	a.acls[i], a.acls[j] = a.acls[j], a.acls[i]
}

func (a *aclIterator) Less(i, j int) bool {
	return a.acls[i].ID < a.acls[j].ID
}

func (a *aclIterator) Front() *structs.ACL {
	if a.index < len(a.acls) {
		return a.acls[a.index]
	}
	return nil
}

func (a *aclIterator) Next() {
	a.index++
}

func reconcileACLs(local, remote structs.ACLs, lastRemoteIndex uint64) structs.ACLRequests {

	localIter, remoteIter := newACLIterator(local), newACLIterator(remote)
	sort.Sort(localIter)
	sort.Sort(remoteIter)

	var changes structs.ACLRequests
	for localIter.Front() != nil || remoteIter.Front() != nil {

		if localIter.Front() == nil {
			changes = append(changes, &structs.ACLRequest{
				Op:  structs.ACLSet,
				ACL: *(remoteIter.Front()),
			})
			remoteIter.Next()
			continue
		}

		if remoteIter.Front() == nil {
			changes = append(changes, &structs.ACLRequest{
				Op:  structs.ACLDelete,
				ACL: *(localIter.Front()),
			})
			localIter.Next()
			continue
		}

		if localIter.Front().ID > remoteIter.Front().ID {
			changes = append(changes, &structs.ACLRequest{
				Op:  structs.ACLSet,
				ACL: *(remoteIter.Front()),
			})
			remoteIter.Next()
			continue
		}

		if localIter.Front().ID < remoteIter.Front().ID {
			changes = append(changes, &structs.ACLRequest{
				Op:  structs.ACLDelete,
				ACL: *(localIter.Front()),
			})
			localIter.Next()
			continue
		}

		l, r := localIter.Front(), remoteIter.Front()
		if r.RaftIndex.ModifyIndex > lastRemoteIndex && !r.IsSame(l) {
			changes = append(changes, &structs.ACLRequest{
				Op:  structs.ACLSet,
				ACL: *r,
			})
		}
		localIter.Next()
		remoteIter.Next()
	}
	return changes
}

func (s *Server) fetchLocalACLs() (structs.ACLs, error) {
	_, local, err := s.fsm.State().ACLList(nil)
	if err != nil {
		return nil, err
	}
	return local, nil
}

func (s *Server) fetchRemoteACLs(lastRemoteIndex uint64) (*structs.IndexedACLs, error) {
	defer metrics.MeasureSince([]string{"consul", "leader", "fetchRemoteACLs"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "fetchRemoteACLs"}, time.Now())

	args := structs.DCSpecificRequest{
		Datacenter: s.config.ACLDatacenter,
		QueryOptions: structs.QueryOptions{
			Token:         s.tokens.ACLReplicationToken(),
			MinQueryIndex: lastRemoteIndex,
			AllowStale:    true,
		},
	}
	var remote structs.IndexedACLs
	if err := s.RPC("ACL.List", &args, &remote); err != nil {
		return nil, err
	}
	return &remote, nil
}

func (s *Server) updateLocalACLs(changes structs.ACLRequests) error {
	defer metrics.MeasureSince([]string{"consul", "leader", "updateLocalACLs"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "updateLocalACLs"}, time.Now())

	minTimePerOp := time.Second / time.Duration(s.config.ACLReplicationApplyLimit)
	for _, change := range changes {

		var reply string
		start := time.Now()
		if err := aclApplyInternal(s, change, &reply); err != nil {
			return err
		}

		elapsed := time.Since(start)
		time.Sleep(minTimePerOp - elapsed)
	}
	return nil
}

func (s *Server) replicateACLs(lastRemoteIndex uint64) (uint64, error) {
	remote, err := s.fetchRemoteACLs(lastRemoteIndex)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve remote ACLs: %v", err)
	}

	if !s.IsLeader() {
		return 0, fmt.Errorf("no longer cluster leader")
	}

	defer metrics.MeasureSince([]string{"consul", "leader", "replicateACLs"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "replicateACLs"}, time.Now())

	local, err := s.fetchLocalACLs()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve local ACLs: %v", err)
	}

	if remote.QueryMeta.Index < lastRemoteIndex {
		s.logger.Printf("[WARN] consul: ACL replication remote index moved backwards (%d to %d), forcing a full ACL sync", lastRemoteIndex, remote.QueryMeta.Index)
		lastRemoteIndex = 0
	}

	changes := reconcileACLs(local, remote.ACLs, lastRemoteIndex)
	if err := s.updateLocalACLs(changes); err != nil {
		return 0, fmt.Errorf("failed to sync ACL changes: %v", err)
	}

	return remote.QueryMeta.Index, nil
}

func (s *Server) IsACLReplicationEnabled() bool {
	authDC := s.config.ACLDatacenter
	return len(authDC) > 0 && (authDC != s.config.Datacenter) &&
		s.config.EnableACLReplication
}

func (s *Server) updateACLReplicationStatus(status structs.ACLReplicationStatus) {

	status.LastError = status.LastError.Round(time.Second).UTC()
	status.LastSuccess = status.LastSuccess.Round(time.Second).UTC()

	s.aclReplicationStatusLock.Lock()
	s.aclReplicationStatus = status
	s.aclReplicationStatusLock.Unlock()
}

func (s *Server) runACLReplication() {
	var status structs.ACLReplicationStatus
	status.Enabled = true
	status.SourceDatacenter = s.config.ACLDatacenter
	s.updateACLReplicationStatus(status)

	defer func() {
		status.Running = false
		s.updateACLReplicationStatus(status)
	}()

	select {
	case <-s.shutdownCh:
		return

	case <-time.After(lib.RandomStagger(s.config.ACLReplicationInterval)):
	}

	var lastRemoteIndex uint64
	replicate := func() {
		if !status.Running {
			lastRemoteIndex = 0 // Re-sync everything.
			status.Running = true
			s.updateACLReplicationStatus(status)
			s.logger.Printf("[INFO] consul: ACL replication started")
		}

		index, err := s.replicateACLs(lastRemoteIndex)
		if err != nil {
			lastRemoteIndex = 0 // Re-sync everything.
			status.LastError = time.Now()
			s.updateACLReplicationStatus(status)
			s.logger.Printf("[WARN] consul: ACL replication error (will retry if still leader): %v", err)
		} else {
			lastRemoteIndex = index
			status.ReplicatedIndex = index
			status.LastSuccess = time.Now()
			s.updateACLReplicationStatus(status)
			s.logger.Printf("[DEBUG] consul: ACL replication completed through remote index %d", index)
		}
	}
	pause := func() {
		if status.Running {
			lastRemoteIndex = 0 // Re-sync everything.
			status.Running = false
			s.updateACLReplicationStatus(status)
			s.logger.Printf("[INFO] consul: ACL replication stopped (no longer leader)")
		}
	}

	//

	for {
		select {
		case <-s.shutdownCh:
			return

		case <-time.After(s.config.ACLReplicationInterval):
			if s.IsLeader() {
				replicate()
			} else {
				pause()
			}
		}
	}
}
