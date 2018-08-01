package api

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"
)

const (
	DefaultSemaphoreSessionName = "Consul API Semaphore"

	DefaultSemaphoreSessionTTL = "15s"

	DefaultSemaphoreWaitTime = 15 * time.Second

	DefaultSemaphoreKey = ".lock"

	SemaphoreFlagValue = 0xe0f69a2baa414de0
)

var (
	ErrSemaphoreHeld = fmt.Errorf("Semaphore already held")

	ErrSemaphoreNotHeld = fmt.Errorf("Semaphore not held")

	ErrSemaphoreInUse = fmt.Errorf("Semaphore in use")

	ErrSemaphoreConflict = fmt.Errorf("Existing key does not match semaphore use")
)

type Semaphore struct {
	c    *Client
	opts *SemaphoreOptions

	isHeld       bool
	sessionRenew chan struct{}
	lockSession  string
	l            sync.Mutex
}

type SemaphoreOptions struct {
	Prefix            string        // Must be set and have write permissions
	Limit             int           // Must be set, and be positive
	Value             []byte        // Optional, value to associate with the contender entry
	Session           string        // Optional, created if not specified
	SessionName       string        // Optional, defaults to DefaultLockSessionName
	SessionTTL        string        // Optional, defaults to DefaultLockSessionTTL
	MonitorRetries    int           // Optional, defaults to 0 which means no retries
	MonitorRetryTime  time.Duration // Optional, defaults to DefaultMonitorRetryTime
	SemaphoreWaitTime time.Duration // Optional, defaults to DefaultSemaphoreWaitTime
	SemaphoreTryOnce  bool          // Optional, defaults to false which means try forever
}

type semaphoreLock struct {
	Limit int

	Holders map[string]bool
}

func (c *Client) SemaphorePrefix(prefix string, limit int) (*Semaphore, error) {
	opts := &SemaphoreOptions{
		Prefix: prefix,
		Limit:  limit,
	}
	return c.SemaphoreOpts(opts)
}

func (c *Client) SemaphoreOpts(opts *SemaphoreOptions) (*Semaphore, error) {
	if opts.Prefix == "" {
		return nil, fmt.Errorf("missing prefix")
	}
	if opts.Limit <= 0 {
		return nil, fmt.Errorf("semaphore limit must be positive")
	}
	if opts.SessionName == "" {
		opts.SessionName = DefaultSemaphoreSessionName
	}
	if opts.SessionTTL == "" {
		opts.SessionTTL = DefaultSemaphoreSessionTTL
	} else {
		if _, err := time.ParseDuration(opts.SessionTTL); err != nil {
			return nil, fmt.Errorf("invalid SessionTTL: %v", err)
		}
	}
	if opts.MonitorRetryTime == 0 {
		opts.MonitorRetryTime = DefaultMonitorRetryTime
	}
	if opts.SemaphoreWaitTime == 0 {
		opts.SemaphoreWaitTime = DefaultSemaphoreWaitTime
	}
	s := &Semaphore{
		c:    c,
		opts: opts,
	}
	return s, nil
}

func (s *Semaphore) Acquire(stopCh <-chan struct{}) (<-chan struct{}, error) {

	s.l.Lock()
	defer s.l.Unlock()

	if s.isHeld {
		return nil, ErrSemaphoreHeld
	}

	s.lockSession = s.opts.Session
	if s.lockSession == "" {
		sess, err := s.createSession()
		if err != nil {
			return nil, fmt.Errorf("failed to create session: %v", err)
		}

		s.sessionRenew = make(chan struct{})
		s.lockSession = sess
		session := s.c.Session()
		go session.RenewPeriodic(s.opts.SessionTTL, sess, nil, s.sessionRenew)

		defer func() {
			if !s.isHeld {
				close(s.sessionRenew)
				s.sessionRenew = nil
			}
		}()
	}

	kv := s.c.KV()
	made, _, err := kv.Acquire(s.contenderEntry(s.lockSession), nil)
	if err != nil || !made {
		return nil, fmt.Errorf("failed to make contender entry: %v", err)
	}

	qOpts := &QueryOptions{
		WaitTime: s.opts.SemaphoreWaitTime,
	}

	start := time.Now()
	attempts := 0
WAIT:

	select {
	case <-stopCh:
		return nil, nil
	default:
	}

	if s.opts.SemaphoreTryOnce && attempts > 0 {
		elapsed := time.Since(start)
		if elapsed > qOpts.WaitTime {
			return nil, nil
		}

		qOpts.WaitTime -= elapsed
	}
	attempts++

	pairs, meta, err := kv.List(s.opts.Prefix, qOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to read prefix: %v", err)
	}

	lockPair := s.findLock(pairs)
	if lockPair.Flags != SemaphoreFlagValue {
		return nil, ErrSemaphoreConflict
	}
	lock, err := s.decodeLock(lockPair)
	if err != nil {
		return nil, err
	}

	if lock.Limit != s.opts.Limit {
		return nil, fmt.Errorf("semaphore limit conflict (lock: %d, local: %d)",
			lock.Limit, s.opts.Limit)
	}

	s.pruneDeadHolders(lock, pairs)

	if len(lock.Holders) >= lock.Limit {
		qOpts.WaitIndex = meta.LastIndex
		goto WAIT
	}

	lock.Holders[s.lockSession] = true
	newLock, err := s.encodeLock(lock, lockPair.ModifyIndex)
	if err != nil {
		return nil, err
	}

	didSet, _, err := kv.CAS(newLock, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to update lock: %v", err)
	}
	if !didSet {

		goto WAIT
	}

	lockCh := make(chan struct{})
	go s.monitorLock(s.lockSession, lockCh)

	s.isHeld = true

	return lockCh, nil
}

func (s *Semaphore) Release() error {

	s.l.Lock()
	defer s.l.Unlock()

	if !s.isHeld {
		return ErrSemaphoreNotHeld
	}

	s.isHeld = false

	if s.sessionRenew != nil {
		defer func() {
			close(s.sessionRenew)
			s.sessionRenew = nil
		}()
	}

	lockSession := s.lockSession
	s.lockSession = ""

	kv := s.c.KV()
	key := path.Join(s.opts.Prefix, DefaultSemaphoreKey)
READ:
	pair, _, err := kv.Get(key, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		pair = &KVPair{}
	}
	lock, err := s.decodeLock(pair)
	if err != nil {
		return err
	}

	if _, ok := lock.Holders[lockSession]; ok {
		delete(lock.Holders, lockSession)
		newLock, err := s.encodeLock(lock, pair.ModifyIndex)
		if err != nil {
			return err
		}

		didSet, _, err := kv.CAS(newLock, nil)
		if err != nil {
			return fmt.Errorf("failed to update lock: %v", err)
		}
		if !didSet {
			goto READ
		}
	}

	contenderKey := path.Join(s.opts.Prefix, lockSession)
	if _, err := kv.Delete(contenderKey, nil); err != nil {
		return err
	}
	return nil
}

func (s *Semaphore) Destroy() error {

	s.l.Lock()
	defer s.l.Unlock()

	if s.isHeld {
		return ErrSemaphoreHeld
	}

	kv := s.c.KV()
	pairs, _, err := kv.List(s.opts.Prefix, nil)
	if err != nil {
		return fmt.Errorf("failed to read prefix: %v", err)
	}

	lockPair := s.findLock(pairs)
	if lockPair.ModifyIndex == 0 {
		return nil
	}
	if lockPair.Flags != SemaphoreFlagValue {
		return ErrSemaphoreConflict
	}

	lock, err := s.decodeLock(lockPair)
	if err != nil {
		return err
	}

	s.pruneDeadHolders(lock, pairs)

	if len(lock.Holders) > 0 {
		return ErrSemaphoreInUse
	}

	didRemove, _, err := kv.DeleteCAS(lockPair, nil)
	if err != nil {
		return fmt.Errorf("failed to remove semaphore: %v", err)
	}
	if !didRemove {
		return ErrSemaphoreInUse
	}
	return nil
}

func (s *Semaphore) createSession() (string, error) {
	session := s.c.Session()
	se := &SessionEntry{
		Name:     s.opts.SessionName,
		TTL:      s.opts.SessionTTL,
		Behavior: SessionBehaviorDelete,
	}
	id, _, err := session.Create(se, nil)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Semaphore) contenderEntry(session string) *KVPair {
	return &KVPair{
		Key:     path.Join(s.opts.Prefix, session),
		Value:   s.opts.Value,
		Session: session,
		Flags:   SemaphoreFlagValue,
	}
}

func (s *Semaphore) findLock(pairs KVPairs) *KVPair {
	key := path.Join(s.opts.Prefix, DefaultSemaphoreKey)
	for _, pair := range pairs {
		if pair.Key == key {
			return pair
		}
	}
	return &KVPair{Flags: SemaphoreFlagValue}
}

func (s *Semaphore) decodeLock(pair *KVPair) (*semaphoreLock, error) {

	if pair == nil || pair.Value == nil {
		return &semaphoreLock{
			Limit:   s.opts.Limit,
			Holders: make(map[string]bool),
		}, nil
	}

	l := &semaphoreLock{}
	if err := json.Unmarshal(pair.Value, l); err != nil {
		return nil, fmt.Errorf("lock decoding failed: %v", err)
	}
	return l, nil
}

func (s *Semaphore) encodeLock(l *semaphoreLock, oldIndex uint64) (*KVPair, error) {
	enc, err := json.Marshal(l)
	if err != nil {
		return nil, fmt.Errorf("lock encoding failed: %v", err)
	}
	pair := &KVPair{
		Key:         path.Join(s.opts.Prefix, DefaultSemaphoreKey),
		Value:       enc,
		Flags:       SemaphoreFlagValue,
		ModifyIndex: oldIndex,
	}
	return pair, nil
}

func (s *Semaphore) pruneDeadHolders(lock *semaphoreLock, pairs KVPairs) {

	alive := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		if pair.Session != "" {
			alive[pair.Session] = struct{}{}
		}
	}

	for holder := range lock.Holders {
		if _, ok := alive[holder]; !ok {
			delete(lock.Holders, holder)
		}
	}
}

func (s *Semaphore) monitorLock(session string, stopCh chan struct{}) {
	defer close(stopCh)
	kv := s.c.KV()
	opts := &QueryOptions{RequireConsistent: true}
WAIT:
	retries := s.opts.MonitorRetries
RETRY:
	pairs, meta, err := kv.List(s.opts.Prefix, opts)
	if err != nil {

		if retries > 0 && IsRetryableError(err) {
			time.Sleep(s.opts.MonitorRetryTime)
			retries--
			opts.WaitIndex = 0
			goto RETRY
		}
		return
	}
	lockPair := s.findLock(pairs)
	lock, err := s.decodeLock(lockPair)
	if err != nil {
		return
	}
	s.pruneDeadHolders(lock, pairs)
	if _, ok := lock.Holders[session]; ok {
		opts.WaitIndex = meta.LastIndex
		goto WAIT
	}
}
