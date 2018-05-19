// The snapshot endpoint is a special non-RPC endpoint that supports streaming
//
package consul

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/snapshot"
	"github.com/hashicorp/go-msgpack/codec"
)

func (s *Server) dispatchSnapshotRequest(args *structs.SnapshotRequest, in io.Reader,
	reply *structs.SnapshotResponse) (io.ReadCloser, error) {

	if dc := args.Datacenter; dc != s.config.Datacenter {
		manager, server, ok := s.router.FindRoute(dc)
		if !ok {
			return nil, structs.ErrNoDCPath
		}

		snap, err := SnapshotRPC(s.connPool, dc, server.Addr, server.UseTLS, args, in, reply)
		if err != nil {
			manager.NotifyFailedServer(server)
			return nil, err
		}

		return snap, nil
	}

	if !args.AllowStale {
		if isLeader, server := s.getLeader(); !isLeader {
			if server == nil {
				return nil, structs.ErrNoLeader
			}
			return SnapshotRPC(s.connPool, args.Datacenter, server.Addr, server.UseTLS, args, in, reply)
		}
	}

	if rule, err := s.resolveToken(args.Token); err != nil {
		return nil, err
	} else if rule != nil && !rule.Snapshot() {
		return nil, acl.ErrPermissionDenied
	}

	switch args.Op {
	case structs.SnapshotSave:
		if !args.AllowStale {
			if err := s.consistentRead(); err != nil {
				return nil, err
			}
		}

		s.setQueryMeta(&reply.QueryMeta)

		snap, err := snapshot.New(s.logger, s.raft)
		reply.Index = snap.Index()
		return snap, err

	case structs.SnapshotRestore:
		if args.AllowStale {
			return nil, fmt.Errorf("stale not allowed for restore")
		}

		if err := snapshot.Restore(s.logger, in, s.raft); err != nil {
			return nil, err
		}

		barrier := s.raft.Barrier(0)
		if err := barrier.Error(); err != nil {
			return nil, err
		}

		errCh := make(chan error, 1)
		timeoutCh := time.After(time.Minute)

		select {

		case s.reassertLeaderCh <- errCh:

		case <-timeoutCh:
			return nil, fmt.Errorf("timed out waiting to re-run leader actions")

		case <-s.shutdownCh:
		}

		select {

		case err := <-errCh:
			if err != nil {
				return nil, err
			}

		case <-timeoutCh:
			return nil, fmt.Errorf("timed out waiting for re-run of leader actions")

		case <-s.shutdownCh:
		}

		return ioutil.NopCloser(bytes.NewReader([]byte(""))), nil

	default:
		return nil, fmt.Errorf("unrecognized snapshot op %q", args.Op)
	}
}

func (s *Server) handleSnapshotRequest(conn net.Conn) error {
	var args structs.SnapshotRequest
	dec := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	if err := dec.Decode(&args); err != nil {
		return fmt.Errorf("failed to decode request: %v", err)
	}

	var reply structs.SnapshotResponse
	snap, err := s.dispatchSnapshotRequest(&args, conn, &reply)
	if err != nil {
		reply.Error = err.Error()
		goto RESPOND
	}
	defer func() {
		if err := snap.Close(); err != nil {
			s.logger.Printf("[ERR] consul: Failed to close snapshot: %v", err)
		}
	}()

RESPOND:
	enc := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	if err := enc.Encode(&reply); err != nil {
		return fmt.Errorf("failed to encode response: %v", err)
	}
	if snap != nil {
		if _, err := io.Copy(conn, snap); err != nil {
			return fmt.Errorf("failed to stream snapshot: %v", err)
		}
	}

	return nil
}

func SnapshotRPC(connPool *pool.ConnPool, dc string, addr net.Addr, useTLS bool,
	args *structs.SnapshotRequest, in io.Reader, reply *structs.SnapshotResponse) (io.ReadCloser, error) {

	conn, hc, err := connPool.DialTimeout(dc, addr, 10*time.Second, useTLS)
	if err != nil {
		return nil, err
	}

	var keep bool
	defer func() {
		if !keep {
			conn.Close()
		}
	}()

	if _, err := conn.Write([]byte{byte(pool.RPCSnapshot)}); err != nil {
		return nil, fmt.Errorf("failed to write stream type: %v", err)
	}

	enc := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	if err := enc.Encode(&args); err != nil {
		return nil, fmt.Errorf("failed to encode request: %v", err)
	}
	if _, err := io.Copy(conn, in); err != nil {
		return nil, fmt.Errorf("failed to copy snapshot in: %v", err)
	}

	if hc != nil {
		if err := hc.CloseWrite(); err != nil {
			return nil, fmt.Errorf("failed to half close snapshot connection: %v", err)
		}
	} else {
		return nil, fmt.Errorf("snapshot connection requires half-close support")
	}

	dec := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	if err := dec.Decode(reply); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}
	if reply.Error != "" {
		return nil, errors.New(reply.Error)
	}

	keep = true
	return conn, nil
}
