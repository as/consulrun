package api

import (
	"fmt"
	"sync"
	"time"
)

const (
	DefaultLockSessionName = "Consul API Lock"

	DefaultLockSessionTTL = "15s"

	DefaultLockWaitTime = 15 * time.Second

	DefaultLockRetryTime = 5 * time.Second

	DefaultMonitorRetryTime = 2 * time.Second

	LockFlagValue = 0x2ddccbc058a50c18
)

var (
	ErrLockHeld = fmt.Errorf("Lock already held")

	ErrLockNotHeld = fmt.Errorf("Lock not held")

	ErrLockInUse = fmt.Errorf("Lock in use")

	ErrLockConflict = fmt.Errorf("Existing key does not match lock use")
)

type Lock struct {
	c    *Client
	opts *LockOptions

	isHeld       bool
	sessionRenew chan struct{}
	lockSession  string
	l            sync.Mutex
}

type LockOptions struct {
	Key              string        // Must be set and have write permissions
	Value            []byte        // Optional, value to associate with the lock
	Session          string        // Optional, created if not specified
	SessionOpts      *SessionEntry // Optional, options to use when creating a session
	SessionName      string        // Optional, defaults to DefaultLockSessionName (ignored if SessionOpts is given)
	SessionTTL       string        // Optional, defaults to DefaultLockSessionTTL (ignored if SessionOpts is given)
	MonitorRetries   int           // Optional, defaults to 0 which means no retries
	MonitorRetryTime time.Duration // Optional, defaults to DefaultMonitorRetryTime
	LockWaitTime     time.Duration // Optional, defaults to DefaultLockWaitTime
	LockTryOnce      bool          // Optional, defaults to false which means try forever
}

func (c *Client) LockKey(key string) (*Lock, error) {
	opts := &LockOptions{
		Key: key,
	}
	return c.LockOpts(opts)
}

func (c *Client) LockOpts(opts *LockOptions) (*Lock, error) {
	if opts.Key == "" {
		return nil, fmt.Errorf("missing key")
	}
	if opts.SessionName == "" {
		opts.SessionName = DefaultLockSessionName
	}
	if opts.SessionTTL == "" {
		opts.SessionTTL = DefaultLockSessionTTL
	} else {
		if _, err := time.ParseDuration(opts.SessionTTL); err != nil {
			return nil, fmt.Errorf("invalid SessionTTL: %v", err)
		}
	}
	if opts.MonitorRetryTime == 0 {
		opts.MonitorRetryTime = DefaultMonitorRetryTime
	}
	if opts.LockWaitTime == 0 {
		opts.LockWaitTime = DefaultLockWaitTime
	}
	l := &Lock{
		c:    c,
		opts: opts,
	}
	return l, nil
}

func (l *Lock) Lock(stopCh <-chan struct{}) (<-chan struct{}, error) {

	l.l.Lock()
	defer l.l.Unlock()

	if l.isHeld {
		return nil, ErrLockHeld
	}

	l.lockSession = l.opts.Session
	if l.lockSession == "" {
		s, err := l.createSession()
		if err != nil {
			return nil, fmt.Errorf("failed to create session: %v", err)
		}

		l.sessionRenew = make(chan struct{})
		l.lockSession = s
		session := l.c.Session()
		go session.RenewPeriodic(l.opts.SessionTTL, s, nil, l.sessionRenew)

		defer func() {
			if !l.isHeld {
				close(l.sessionRenew)
				l.sessionRenew = nil
			}
		}()
	}

	kv := l.c.KV()
	qOpts := &QueryOptions{
		WaitTime: l.opts.LockWaitTime,
	}

	start := time.Now()
	attempts := 0
WAIT:

	select {
	case <-stopCh:
		return nil, nil
	default:
	}

	if l.opts.LockTryOnce && attempts > 0 {
		elapsed := time.Since(start)
		if elapsed > qOpts.WaitTime {
			return nil, nil
		}

		qOpts.WaitTime -= elapsed
	}
	attempts++

	pair, meta, err := kv.Get(l.opts.Key, qOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to read lock: %v", err)
	}
	if pair != nil && pair.Flags != LockFlagValue {
		return nil, ErrLockConflict
	}
	locked := false
	if pair != nil && pair.Session == l.lockSession {
		goto HELD
	}
	if pair != nil && pair.Session != "" {
		qOpts.WaitIndex = meta.LastIndex
		goto WAIT
	}

	pair = l.lockEntry(l.lockSession)
	locked, _, err = kv.Acquire(pair, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}

	if !locked {

		qOpts.WaitIndex = 0
		pair, meta, err = kv.Get(l.opts.Key, qOpts)
		if pair != nil && pair.Session != "" {

			qOpts.WaitIndex = meta.LastIndex
			goto WAIT
		} else {

			select {
			case <-time.After(DefaultLockRetryTime):
				goto WAIT
			case <-stopCh:
				return nil, nil
			}
		}
	}

HELD:

	leaderCh := make(chan struct{})
	go l.monitorLock(l.lockSession, leaderCh)

	l.isHeld = true

	return leaderCh, nil
}

func (l *Lock) Unlock() error {

	l.l.Lock()
	defer l.l.Unlock()

	if !l.isHeld {
		return ErrLockNotHeld
	}

	l.isHeld = false

	if l.sessionRenew != nil {
		defer func() {
			close(l.sessionRenew)
			l.sessionRenew = nil
		}()
	}

	lockEnt := l.lockEntry(l.lockSession)
	l.lockSession = ""

	kv := l.c.KV()
	_, _, err := kv.Release(lockEnt, nil)
	if err != nil {
		return fmt.Errorf("failed to release lock: %v", err)
	}
	return nil
}

func (l *Lock) Destroy() error {

	l.l.Lock()
	defer l.l.Unlock()

	if l.isHeld {
		return ErrLockHeld
	}

	kv := l.c.KV()
	pair, _, err := kv.Get(l.opts.Key, nil)
	if err != nil {
		return fmt.Errorf("failed to read lock: %v", err)
	}

	if pair == nil {
		return nil
	}

	if pair.Flags != LockFlagValue {
		return ErrLockConflict
	}

	if pair.Session != "" {
		return ErrLockInUse
	}

	didRemove, _, err := kv.DeleteCAS(pair, nil)
	if err != nil {
		return fmt.Errorf("failed to remove lock: %v", err)
	}
	if !didRemove {
		return ErrLockInUse
	}
	return nil
}

func (l *Lock) createSession() (string, error) {
	session := l.c.Session()
	se := l.opts.SessionOpts
	if se == nil {
		se = &SessionEntry{
			Name: l.opts.SessionName,
			TTL:  l.opts.SessionTTL,
		}
	}
	id, _, err := session.Create(se, nil)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (l *Lock) lockEntry(session string) *KVPair {
	return &KVPair{
		Key:     l.opts.Key,
		Value:   l.opts.Value,
		Session: session,
		Flags:   LockFlagValue,
	}
}

func (l *Lock) monitorLock(session string, stopCh chan struct{}) {
	defer close(stopCh)
	kv := l.c.KV()
	opts := &QueryOptions{RequireConsistent: true}
WAIT:
	retries := l.opts.MonitorRetries
RETRY:
	pair, meta, err := kv.Get(l.opts.Key, opts)
	if err != nil {

		if retries > 0 && IsRetryableError(err) {
			time.Sleep(l.opts.MonitorRetryTime)
			retries--
			opts.WaitIndex = 0
			goto RETRY
		}
		return
	}
	if pair != nil && pair.Session == session {
		opts.WaitIndex = meta.LastIndex
		goto WAIT
	}
}
