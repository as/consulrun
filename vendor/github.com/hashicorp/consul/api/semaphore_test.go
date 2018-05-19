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

func TestAPI_SemaphoreAcquireRelease(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = sema.Release()
	if err != ErrSemaphoreNotHeld {
		t.Fatalf("err: %v", err)
	}

	lockCh, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lockCh == nil {
		t.Fatalf("not hold")
	}

	_, err = sema.Acquire(nil)
	if err != ErrSemaphoreHeld {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-lockCh:
		t.Fatalf("should be held")
	default:
	}

	err = sema.Release()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = sema.Release()
	if err != ErrSemaphoreNotHeld {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-lockCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be held")
	}
}

func TestAPI_SemaphoreForceInvalidate(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	lockCh, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lockCh == nil {
		t.Fatalf("not acquired")
	}
	defer sema.Release()

	go func() {

		session := c.Session()
		session.Destroy(sema.lockSession, nil)
	}()

	select {
	case <-lockCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be locked")
	}
}

func TestAPI_SemaphoreDeleteKey(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	lockCh, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if lockCh == nil {
		t.Fatalf("not locked")
	}
	defer sema.Release()

	go func() {

		kv := c.KV()
		kv.DeleteTree("test/semaphore", nil)
	}()

	select {
	case <-lockCh:
	case <-time.After(time.Second):
		t.Fatalf("should not be locked")
	}
}

func TestAPI_SemaphoreContend(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	wg := &sync.WaitGroup{}
	acquired := make([]bool, 4)
	for idx := range acquired {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sema, err := c.SemaphorePrefix("test/semaphore", 2)
			if err != nil {
				t.Fatalf("err: %v", err)
			}

			lockCh, err := sema.Acquire(nil)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if lockCh == nil {
				t.Fatalf("not locked")
			}
			defer sema.Release()
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

func TestAPI_SemaphoreBadLimit(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/semaphore", 0)
	if err == nil {
		t.Fatalf("should error")
	}

	sema, err = c.SemaphorePrefix("test/semaphore", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	sema2, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = sema2.Acquire(nil)
	if err.Error() != "semaphore limit conflict (lock: 1, local: 2)" {
		t.Fatalf("err: %v", err)
	}
}

func TestAPI_SemaphoreDestroy(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	sema, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	sema2, err := c.SemaphorePrefix("test/semaphore", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = sema2.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := sema.Destroy(); err != ErrSemaphoreHeld {
		t.Fatalf("err: %v", err)
	}

	err = sema.Release()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := sema.Destroy(); err != ErrSemaphoreInUse {
		t.Fatalf("err: %v", err)
	}

	err = sema2.Release()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := sema.Destroy(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := sema2.Destroy(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestAPI_SemaphoreConflict(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	lock, err := c.LockKey("test/sema/.lock")
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

	sema, err := c.SemaphorePrefix("test/sema/", 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = sema.Acquire(nil)
	if err != ErrSemaphoreConflict {
		t.Fatalf("err: %v", err)
	}

	err = sema.Destroy()
	if err != ErrSemaphoreConflict {
		t.Fatalf("err: %v", err)
	}
}

func TestAPI_SemaphoreMonitorRetry(t *testing.T) {
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
		if errors > 0 && req.Method == "GET" && strings.Contains(req.URL.Path, "/v1/kv/test/sema/.lock") {
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

	opts := &SemaphoreOptions{
		Prefix:         "test/sema/.lock",
		Limit:          2,
		SessionTTL:     "60s",
		MonitorRetries: 3,
	}
	sema, err := c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if sema.opts.MonitorRetryTime != DefaultMonitorRetryTime {
		t.Fatalf("bad: %d", sema.opts.MonitorRetryTime)
	}

	opts.MonitorRetryTime = 250 * time.Millisecond
	sema, err = c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if sema.opts.MonitorRetryTime != 250*time.Millisecond {
		t.Fatalf("bad: %d", sema.opts.MonitorRetryTime)
	}

	ch, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("didn't acquire")
	}

	mutex.Lock()
	errors = 2
	mutex.Unlock()
	another, err := raw.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := another.Acquire(nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(5 * opts.MonitorRetryTime)

	select {
	case <-ch:
		t.Fatalf("lost the semaphore")
	default:
	}

	mutex.Lock()
	errors = 10
	mutex.Unlock()
	if err := another.Release(); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(5 * opts.MonitorRetryTime)

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("should not have the semaphore")
	}
}

func TestAPI_SemaphoreOneShot(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	opts := &SemaphoreOptions{
		Prefix:           "test/sema/.lock",
		Limit:            2,
		SemaphoreTryOnce: true,
	}
	sema, err := c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if sema.opts.SemaphoreWaitTime != DefaultSemaphoreWaitTime {
		t.Fatalf("bad: %d", sema.opts.SemaphoreWaitTime)
	}

	opts.SemaphoreWaitTime = 250 * time.Millisecond
	sema, err = c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if sema.opts.SemaphoreWaitTime != 250*time.Millisecond {
		t.Fatalf("bad: %d", sema.opts.SemaphoreWaitTime)
	}

	ch, err := sema.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("should have acquired the semaphore")
	}

	another, err := c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	ch, err = another.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("should have acquired the semaphore")
	}

	contender, err := c.SemaphoreOpts(opts)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	start := time.Now()
	ch, err = contender.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch != nil {
		t.Fatalf("should not have acquired the semaphore")
	}
	diff := time.Since(start)
	if diff < contender.opts.SemaphoreWaitTime {
		t.Fatalf("time out of bounds: %9.6f", diff.Seconds())
	}

	if err := another.Release(); err != nil {
		t.Fatalf("err: %v", err)
	}
	ch, err = contender.Acquire(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ch == nil {
		t.Fatalf("should have acquired the semaphore")
	}
}
