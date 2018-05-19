package consul

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/lib"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/yamux"
)

const (
	maxQueryTime = 600 * time.Second

	defaultQueryTime = 300 * time.Second

	jitterFraction = 16

	raftWarnSize = 1024 * 1024

	enqueueLimit = 30 * time.Second
)

func (s *Server) listen(listener net.Listener) {
	for {

		conn, err := listener.Accept()
		if err != nil {
			if s.shutdown {
				return
			}
			s.logger.Printf("[ERR] consul.rpc: failed to accept RPC conn: %v", err)
			continue
		}

		go s.handleConn(conn, false)
		metrics.IncrCounter([]string{"consul", "rpc", "accept_conn"}, 1)
		metrics.IncrCounter([]string{"rpc", "accept_conn"}, 1)
	}
}

func logConn(conn net.Conn) string {
	return memberlist.LogConn(conn)
}

func (s *Server) handleConn(conn net.Conn, isTLS bool) {

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		if err != io.EOF {
			s.logger.Printf("[ERR] consul.rpc: failed to read byte: %v %s", err, logConn(conn))
		}
		conn.Close()
		return
	}
	typ := pool.RPCType(buf[0])

	if s.config.VerifyIncoming && !isTLS && typ != pool.RPCTLS {
		s.logger.Printf("[WARN] consul.rpc: Non-TLS connection attempted with VerifyIncoming set %s", logConn(conn))
		conn.Close()
		return
	}

	switch typ {
	case pool.RPCConsul:
		s.handleConsulConn(conn)

	case pool.RPCRaft:
		metrics.IncrCounter([]string{"consul", "rpc", "raft_handoff"}, 1)
		metrics.IncrCounter([]string{"rpc", "raft_handoff"}, 1)
		s.raftLayer.Handoff(conn)

	case pool.RPCTLS:
		if s.rpcTLS == nil {
			s.logger.Printf("[WARN] consul.rpc: TLS connection attempted, server not configured for TLS %s", logConn(conn))
			conn.Close()
			return
		}
		conn = tls.Server(conn, s.rpcTLS)
		s.handleConn(conn, true)

	case pool.RPCMultiplexV2:
		s.handleMultiplexV2(conn)

	case pool.RPCSnapshot:
		s.handleSnapshotConn(conn)

	default:
		s.logger.Printf("[ERR] consul.rpc: unrecognized RPC byte: %v %s", typ, logConn(conn))
		conn.Close()
		return
	}
}

func (s *Server) handleMultiplexV2(conn net.Conn) {
	defer conn.Close()
	conf := yamux.DefaultConfig()
	conf.LogOutput = s.config.LogOutput
	server, _ := yamux.Server(conn, conf)
	for {
		sub, err := server.Accept()
		if err != nil {
			if err != io.EOF {
				s.logger.Printf("[ERR] consul.rpc: multiplex conn accept failed: %v %s", err, logConn(conn))
			}
			return
		}
		go s.handleConsulConn(sub)
	}
}

func (s *Server) handleConsulConn(conn net.Conn) {
	defer conn.Close()
	rpcCodec := msgpackrpc.NewServerCodec(conn)
	for {
		select {
		case <-s.shutdownCh:
			return
		default:
		}

		if err := s.rpcServer.ServeRequest(rpcCodec); err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed") {
				s.logger.Printf("[ERR] consul.rpc: RPC error: %v %s", err, logConn(conn))
				metrics.IncrCounter([]string{"consul", "rpc", "request_error"}, 1)
				metrics.IncrCounter([]string{"rpc", "request_error"}, 1)
			}
			return
		}
		metrics.IncrCounter([]string{"consul", "rpc", "request"}, 1)
		metrics.IncrCounter([]string{"rpc", "request"}, 1)
	}
}

func (s *Server) handleSnapshotConn(conn net.Conn) {
	go func() {
		defer conn.Close()
		if err := s.handleSnapshotRequest(conn); err != nil {
			s.logger.Printf("[ERR] consul.rpc: Snapshot RPC error: %v %s", err, logConn(conn))
		}
	}()
}

func canRetry(args interface{}, err error) bool {

	if structs.IsErrNoLeader(err) {
		return true
	}

	info, ok := args.(structs.RPCInfo)
	if ok && info.IsRead() && lib.IsErrEOF(err) {
		return true
	}

	return false
}

func (s *Server) forward(method string, info structs.RPCInfo, args interface{}, reply interface{}) (bool, error) {
	var firstCheck time.Time

	dc := info.RequestDatacenter()
	if dc != s.config.Datacenter {
		err := s.forwardDC(method, dc, args, reply)
		return true, err
	}

	if info.IsRead() && info.AllowStaleRead() {
		return false, nil
	}

CHECK_LEADER:

	select {
	case <-s.leaveCh:
		return true, structs.ErrNoLeader
	default:
	}

	isLeader, leader := s.getLeader()

	if isLeader {
		return false, nil
	}

	rpcErr := structs.ErrNoLeader
	if leader != nil {
		rpcErr = s.connPool.RPC(s.config.Datacenter, leader.Addr,
			leader.Version, method, leader.UseTLS, args, reply)
		if rpcErr != nil && canRetry(info, rpcErr) {
			goto RETRY
		}
		return true, rpcErr
	}

RETRY:

	if firstCheck.IsZero() {
		firstCheck = time.Now()
	}
	if time.Since(firstCheck) < s.config.RPCHoldTimeout {
		jitter := lib.RandomStagger(s.config.RPCHoldTimeout / jitterFraction)
		select {
		case <-time.After(jitter):
			goto CHECK_LEADER
		case <-s.leaveCh:
		case <-s.shutdownCh:
		}
	}

	return true, rpcErr
}

func (s *Server) getLeader() (bool, *metadata.Server) {

	if s.IsLeader() {
		return true, nil
	}

	leader := s.raft.Leader()
	if leader == "" {
		return false, nil
	}

	server := s.serverLookup.Server(leader)

	return false, server
}

func (s *Server) forwardDC(method, dc string, args interface{}, reply interface{}) error {
	manager, server, ok := s.router.FindRoute(dc)
	if !ok {
		s.logger.Printf("[WARN] consul.rpc: RPC request for DC %q, no path found", dc)
		return structs.ErrNoDCPath
	}

	metrics.IncrCounterWithLabels([]string{"consul", "rpc", "cross-dc"}, 1,
		[]metrics.Label{{Name: "datacenter", Value: dc}})
	metrics.IncrCounterWithLabels([]string{"rpc", "cross-dc"}, 1,
		[]metrics.Label{{Name: "datacenter", Value: dc}})
	if err := s.connPool.RPC(dc, server.Addr, server.Version, method, server.UseTLS, args, reply); err != nil {
		manager.NotifyFailedServer(server)
		s.logger.Printf("[ERR] consul: RPC failed to server %s in DC %q: %v", server.Addr, dc, err)
		return err
	}

	return nil
}

func (s *Server) globalRPC(method string, args interface{},
	reply structs.CompoundResponse) error {

	dcs := s.router.GetDatacenters()

	replies, total := 0, len(dcs)
	errorCh := make(chan error, total)
	respCh := make(chan interface{}, total)

	for _, dc := range dcs {
		go func(dc string) {
			rr := reply.New()
			if err := s.forwardDC(method, dc, args, &rr); err != nil {
				errorCh <- err
				return
			}
			respCh <- rr
		}(dc)
	}

	for replies < total {
		select {
		case err := <-errorCh:
			return err
		case rr := <-respCh:
			reply.Add(rr)
			replies++
		}
	}
	return nil
}

func (s *Server) raftApply(t structs.MessageType, msg interface{}) (interface{}, error) {
	buf, err := structs.Encode(t, msg)
	if err != nil {
		return nil, fmt.Errorf("Failed to encode request: %v", err)
	}

	if n := len(buf); n > raftWarnSize {
		s.logger.Printf("[WARN] consul: Attempting to apply large raft entry (%d bytes)", n)
	}

	future := s.raft.Apply(buf, enqueueLimit)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Response(), nil
}

type queryFn func(memdb.WatchSet, *state.Store) error

func (s *Server) blockingQuery(queryOpts *structs.QueryOptions, queryMeta *structs.QueryMeta,
	fn queryFn) error {
	var timeout *time.Timer

	if queryOpts.MinQueryIndex == 0 {
		goto RUN_QUERY
	}

	if queryOpts.MaxQueryTime > maxQueryTime {
		queryOpts.MaxQueryTime = maxQueryTime
	} else if queryOpts.MaxQueryTime <= 0 {
		queryOpts.MaxQueryTime = defaultQueryTime
	}

	queryOpts.MaxQueryTime += lib.RandomStagger(queryOpts.MaxQueryTime / jitterFraction)

	timeout = time.NewTimer(queryOpts.MaxQueryTime)
	defer timeout.Stop()

RUN_QUERY:

	s.setQueryMeta(queryMeta)

	if queryOpts.RequireConsistent {
		if err := s.consistentRead(); err != nil {
			return err
		}
	}

	metrics.IncrCounter([]string{"consul", "rpc", "query"}, 1)
	metrics.IncrCounter([]string{"rpc", "query"}, 1)

	state := s.fsm.State()

	var ws memdb.WatchSet
	if queryOpts.MinQueryIndex > 0 {
		ws = memdb.NewWatchSet()

		ws.Add(state.AbandonCh())
	}

	err := fn(ws, state)
	if err == nil && queryMeta.Index > 0 && queryMeta.Index <= queryOpts.MinQueryIndex {
		if expired := ws.Watch(timeout.C); !expired {

			select {
			case <-state.AbandonCh():
			default:
				goto RUN_QUERY
			}
		}
	}
	return err
}

func (s *Server) setQueryMeta(m *structs.QueryMeta) {
	if s.IsLeader() {
		m.LastContact = 0
		m.KnownLeader = true
	} else {
		m.LastContact = time.Since(s.raft.LastContact())
		m.KnownLeader = (s.raft.Leader() != "")
	}
}

func (s *Server) consistentRead() error {
	defer metrics.MeasureSince([]string{"consul", "rpc", "consistentRead"}, time.Now())
	defer metrics.MeasureSince([]string{"rpc", "consistentRead"}, time.Now())
	future := s.raft.VerifyLeader()
	if err := future.Error(); err != nil {
		return err //fail fast if leader verification fails
	}

	if s.isReadyForConsistentReads() {
		return nil
	}
	jitter := lib.RandomStagger(s.config.RPCHoldTimeout / jitterFraction)
	deadline := time.Now().Add(s.config.RPCHoldTimeout)

	for time.Now().Before(deadline) {

		select {
		case <-time.After(jitter):

		case <-s.shutdownCh:
			return fmt.Errorf("shutdown waiting for leader")
		}

		if s.isReadyForConsistentReads() {
			return nil
		}
	}

	return structs.ErrNotReadyForConsistentReads
}
