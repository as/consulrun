package consul

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/agent/router"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/time/rate"
)

const (
	clientRPCConnMaxIdle = 127 * time.Second

	clientMaxStreams = 32

	serfEventBacklog = 256

	serfEventBacklogWarning = 200
)

type Client struct {
	config *Config

	connPool *pool.ConnPool

	routers *router.Manager

	rpcLimiter *rate.Limiter

	eventCh chan serf.Event

	logger *log.Logger

	serf *serf.Serf

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewClient(config *Config) (*Client, error) {
	return NewClientLogger(config, nil)
}

func NewClientLogger(config *Config, logger *log.Logger) (*Client, error) {

	if err := config.CheckProtocolVersion(); err != nil {
		return nil, err
	}

	if config.DataDir == "" {
		return nil, fmt.Errorf("Config must provide a DataDir")
	}

	if err := config.CheckACL(); err != nil {
		return nil, err
	}

	if config.LogOutput == nil {
		config.LogOutput = os.Stderr
	}

	tlsWrap, err := config.tlsConfig().OutgoingTLSWrapper()
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = log.New(config.LogOutput, "", log.LstdFlags)
	}

	connPool := &pool.ConnPool{
		SrcAddr:    config.RPCSrcAddr,
		LogOutput:  config.LogOutput,
		MaxTime:    clientRPCConnMaxIdle,
		MaxStreams: clientMaxStreams,
		TLSWrapper: tlsWrap,
		ForceTLS:   config.VerifyOutgoing,
	}

	c := &Client{
		config:     config,
		connPool:   connPool,
		rpcLimiter: rate.NewLimiter(config.RPCRate, config.RPCMaxBurst),
		eventCh:    make(chan serf.Event, serfEventBacklog),
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}

	c.serf, err = c.setupSerf(config.SerfLANConfig,
		c.eventCh, serfLANSnapshot)
	if err != nil {
		c.Shutdown()
		return nil, fmt.Errorf("Failed to start lan serf: %v", err)
	}

	c.routers = router.New(c.logger, c.shutdownCh, c.serf, c.connPool)
	go c.routers.Start()

	go c.lanEventHandler()

	return c, nil
}

func (c *Client) Shutdown() error {
	c.logger.Printf("[INFO] consul: shutting down client")
	c.shutdownLock.Lock()
	defer c.shutdownLock.Unlock()

	if c.shutdown {
		return nil
	}

	c.shutdown = true
	close(c.shutdownCh)

	if c.serf != nil {
		c.serf.Shutdown()
	}

	c.connPool.Shutdown()
	return nil
}

func (c *Client) Leave() error {
	c.logger.Printf("[INFO] consul: client starting leave")

	if c.serf != nil {
		if err := c.serf.Leave(); err != nil {
			c.logger.Printf("[ERR] consul: Failed to leave LAN Serf cluster: %v", err)
		}
	}
	return nil
}

func (c *Client) JoinLAN(addrs []string) (int, error) {
	return c.serf.Join(addrs, true)
}

func (c *Client) LocalMember() serf.Member {
	return c.serf.LocalMember()
}

func (c *Client) LANMembers() []serf.Member {
	return c.serf.Members()
}

func (c *Client) LANMembersAllSegments() ([]serf.Member, error) {
	return c.serf.Members(), nil
}

func (c *Client) LANSegmentMembers(segment string) ([]serf.Member, error) {
	if segment == c.config.Segment {
		return c.LANMembers(), nil
	}

	return nil, fmt.Errorf("segment %q not found", segment)
}

func (c *Client) RemoveFailedNode(node string) error {
	return c.serf.RemoveFailedNode(node)
}

func (c *Client) KeyManagerLAN() *serf.KeyManager {
	return c.serf.KeyManager()
}

func (c *Client) Encrypted() bool {
	return c.serf.EncryptionEnabled()
}

func (c *Client) RPC(method string, args interface{}, reply interface{}) error {

	firstCheck := time.Now()

TRY:
	server := c.routers.FindServer()
	if server == nil {
		return structs.ErrNoServers
	}

	metrics.IncrCounter([]string{"consul", "client", "rpc"}, 1)
	metrics.IncrCounter([]string{"client", "rpc"}, 1)
	if !c.rpcLimiter.Allow() {
		metrics.IncrCounter([]string{"consul", "client", "rpc", "exceeded"}, 1)
		metrics.IncrCounter([]string{"client", "rpc", "exceeded"}, 1)
		return structs.ErrRPCRateExceeded
	}

	rpcErr := c.connPool.RPC(c.config.Datacenter, server.Addr, server.Version, method, server.UseTLS, args, reply)
	if rpcErr == nil {
		return nil
	}

	c.logger.Printf("[ERR] consul: %q RPC failed to server %s: %v", method, server.Addr, rpcErr)
	c.routers.NotifyFailedServer(server)
	if retry := canRetry(args, rpcErr); !retry {
		return rpcErr
	}

	if time.Since(firstCheck) < c.config.RPCHoldTimeout {
		jitter := lib.RandomStagger(c.config.RPCHoldTimeout / jitterFraction)
		select {
		case <-time.After(jitter):
			goto TRY
		case <-c.shutdownCh:
		}
	}
	return rpcErr
}

func (c *Client) SnapshotRPC(args *structs.SnapshotRequest, in io.Reader, out io.Writer,
	replyFn structs.SnapshotReplyFn) error {
	server := c.routers.FindServer()
	if server == nil {
		return structs.ErrNoServers
	}

	metrics.IncrCounter([]string{"consul", "client", "rpc"}, 1)
	metrics.IncrCounter([]string{"client", "rpc"}, 1)
	if !c.rpcLimiter.Allow() {
		metrics.IncrCounter([]string{"consul", "client", "rpc", "exceeded"}, 1)
		metrics.IncrCounter([]string{"client", "rpc", "exceeded"}, 1)
		return structs.ErrRPCRateExceeded
	}

	var reply structs.SnapshotResponse
	snap, err := SnapshotRPC(c.connPool, c.config.Datacenter, server.Addr, server.UseTLS, args, in, &reply)
	if err != nil {
		return err
	}
	defer func() {
		if err := snap.Close(); err != nil {
			c.logger.Printf("[WARN] consul: Failed closing snapshot stream: %v", err)
		}
	}()

	if replyFn != nil {
		if err := replyFn(&reply); err != nil {
			return nil
		}
	}

	if out != nil {
		if _, err := io.Copy(out, snap); err != nil {
			return fmt.Errorf("failed to stream snapshot: %v", err)
		}
	}

	return nil
}

func (c *Client) Stats() map[string]map[string]string {
	numServers := c.routers.NumServers()

	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	stats := map[string]map[string]string{
		"consul": {
			"server":        "false",
			"known_servers": toString(uint64(numServers)),
		},
		"serf_lan": c.serf.Stats(),
		"runtime":  runtimeStats(),
	}
	return stats
}

func (c *Client) GetLANCoordinate() (lib.CoordinateSet, error) {
	lan, err := c.serf.GetCoordinate()
	if err != nil {
		return nil, err
	}

	cs := lib.CoordinateSet{c.config.Segment: lan}
	return cs, nil
}
