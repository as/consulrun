package api

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAPI_LockLockUnlock(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	lock, err := c.LockKey("test/lock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = lock.Unlock()
	if err != ErrLockNotHeld {
		t.Fatalf("err: %v", err)
	}

	leaderCh, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}

	_, err = lock.Lock(nil)
	if err != ErrLockHeld {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-leaderCh:
		t.Fatalf("should be leader")
	default:
	}

	err = lock.Unlock()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = lock.Unlock()
	if err != ErrLockNotHeld {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-leaderCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be leader")
	}
}

func TestAPI_LockForceInvalidate(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	lock, err := c.LockKey("test/lock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	leaderCh, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}
	defer lock.Unlock()

	go func() {

		session := c.Session()
		session.Destroy(lock.lockSession, nil)
	}()

	select {
	case <-leaderCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be leader")
	}
}

func TestAPI_LockDeleteKey(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	for i := 0; i < 10; i++ {
		func() {
			lock, err := c.LockKey("test/lock")
			if err != nil {
				t.Fatalf("err: %v", err)
			}

			leaderCh, err := lock.Lock(nil)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if leaderCh == nil {
				t.Fatalf("not leader")
			}
			defer lock.Unlock()

			go func() {

				kv := c.KV()
				kv.Delete("test/lock", nil)
			}()

			select {
			case <-leaderCh:
			case <-time.After(10 * time.Second):
				t.Fatalf("should not be leader")
			}
		}()
	}
}

func TestAPI_LockContend(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	wg := &sync.WaitGroup{}
	acquired := make([]bool, 3)
	for idx := range acquired {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lock, err := c.LockKey("test/lock")
			if err != nil {
				t.Fatalf("err: %v", err)
			}

			leaderCh, err := lock.Lock(nil)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if leaderCh == nil {
				t.Fatalf("not leader")
			}
			defer lock.Unlock()
			log.Printf("Contender %d acquired", idx)

			acquired[idx] = true
		}(idx)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(3 * DefaultLockRetryTime):
		t.Fatalf("timeout")
	}

	for idx, did := range acquired {
		if !did {
			t.Fatalf("contender %d never acquired", idx)
		}
	}
}

func TestAPI_LockDestroy(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	lock, err := c.LockKey("test/lock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	leaderCh, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}

	if err := lock.Destroy(); err != ErrLockHeld {
		t.Fatalf("err: %v", err)
	}

	err = lock.Unlock()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l2, err := c.LockKey("test/lock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	leaderCh, err = l2.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}

	if err := lock.Destroy(); err != ErrLockInUse {
		t.Fatalf("err: %v", err)
	}

	err = l2.Unlock()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = lock.Destroy()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = l2.Destroy()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestAPI_LockConflict(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/lock/", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	lockCh, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lockCh == nil {
		t.Fatalf("not hold")
	}
	defer sema.Release()

	lock, err := c.LockKey("test/lock/.lock")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = lock.Lock(nil)
	if err != ErrLockConflict {
		t.Fatalf("err: %v", err)
	}

	err = lock.Destroy()
	if err != ErrLockConflict {
		t.Fatalf("err: %v", err)
	}
}

func TestAPI_LockReclaimLock(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	session, _, err := c.Session().Create(&SessionEntry{}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	lock, err := c.LockOpts(&LockOptions{Key: "test/lock", Session: session})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	leaderCh, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}
	defer lock.Unlock()

	l2, err := c.LockOpts(&LockOptions{Key: "test/lock", Session: session})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	reclaimed := make(chan (<-chan struct{}), 1)
	go func() {
		l2Ch, err := l2.Lock(nil)
		if err != nil {
			t.Fatalf("not locked: %v", err)
		}
		reclaimed <- l2Ch
	}()

	var leader2Ch <-chan struct{}

	select {
	case leader2Ch = <-reclaimed:
	case <-time.After(time.Second):
		t.Fatalf("should have locked")
	}

	err = l2.Unlock()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-leader2Ch:
	case <-time.After(time.Second):
		t.Fatalf("should not be leader")
	}

	select {
	case <-leaderCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be leader")
	}
}

func TestAPI_LockMonitorRetry(t *testing.T) {
	t.Parallel()
	raw, s := makeClient(t)
	defer s.Stop()

	failer := func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(500)
	}
	outage := httptest.NewServer(http.HandlerFunc(failer))
	defer outage.Close()

	var mutex sync.Mutex
	errors := 0
	director := func(req *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()

		req.URL.Scheme = "http"
		if errors > 0 && req.Method == "GET" && strings.Contains(req.URL.Path, "/v1/kv/test/lock") {
			req.URL.Host = outage.URL[7:] // Strip off "http://".
			errors--
		} else {
			req.URL.Host = raw.config.Address
		}
	}
	proxy := httptest.NewServer(&httputil.ReverseProxy{Director: director})
	defer proxy.Close()

	config := raw.config
	config.Address = proxy.URL[7:] // Strip off "http://".
	c, err := NewClient(&config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	opts := &LockOptions{
		Key:            "test/lock",
		SessionTTL:     "60s",
		MonitorRetries: 3,
	}
	lock, err := c.LockOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if lock.opts.MonitorRetryTime != DefaultMonitorRetryTime {
		t.Fatalf("bad: %d", lock.opts.MonitorRetryTime)
	}

	opts.MonitorRetryTime = 250 * time.Millisecond
	lock, err = c.LockOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lock.opts.MonitorRetryTime != 250*time.Millisecond {
		t.Fatalf("bad: %d", lock.opts.MonitorRetryTime)
	}

	leaderCh, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if leaderCh == nil {
		t.Fatalf("not leader")
	}

	mutex.Lock()
	errors = 2
	mutex.Unlock()
	pair, _, err := raw.KV().Get("test/lock", &QueryOptions{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := raw.KV().Put(pair, &WriteOptions{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(5 * opts.MonitorRetryTime)

	select {
	case <-leaderCh:
		t.Fatalf("should be leader")
	default:
	}

	mutex.Lock()
	errors = 10
	mutex.Unlock()
	if _, err := raw.KV().Put(pair, &WriteOptions{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(5 * opts.MonitorRetryTime)

	select {
	case <-leaderCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be leader")
	}
}

func TestAPI_LockOneShot(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	opts := &LockOptions{
		Key:         "test/lock",
		LockTryOnce: true,
	}
	lock, err := c.LockOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if lock.opts.LockWaitTime != DefaultLockWaitTime {
		t.Fatalf("bad: %d", lock.opts.LockWaitTime)
	}

	opts.LockWaitTime = 250 * time.Millisecond
	lock, err = c.LockOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lock.opts.LockWaitTime != 250*time.Millisecond {
		t.Fatalf("bad: %d", lock.opts.LockWaitTime)
	}

	ch, err := lock.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("not leader")
	}

	contender, err := c.LockOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	start := time.Now()
	ch, err = contender.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch != nil {
		t.Fatalf("should not be leader")
	}
	diff := time.Since(start)
	if diff < contender.opts.LockWaitTime || diff > 2*contender.opts.LockWaitTime {
		t.Fatalf("time out of bounds: %9.6f", diff.Seconds())
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("err: %v", err)
	}
	ch, err = contender.Lock(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("should be leader")
	}
}
