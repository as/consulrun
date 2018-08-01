package api

import (
	"errors"
	"fmt"
	"time"
)

const (
	SessionBehaviorRelease = "release"

	SessionBehaviorDelete = "delete"
)

var ErrSessionExpired = errors.New("session expired")

type SessionEntry struct {
	CreateIndex uint64
	ID          string
	Name        string
	Node        string
	Checks      []string
	LockDelay   time.Duration
	Behavior    string
	TTL         string
}

type Session struct {
	c *Client
}

func (c *Client) Session() *Session {
	return &Session{c}
}

func (s *Session) CreateNoChecks(se *SessionEntry, q *WriteOptions) (string, *WriteMeta, error) {
	body := make(map[string]interface{})
	body["Checks"] = []string{}
	if se != nil {
		if se.Name != "" {
			body["Name"] = se.Name
		}
		if se.Node != "" {
			body["Node"] = se.Node
		}
		if se.LockDelay != 0 {
			body["LockDelay"] = durToMsec(se.LockDelay)
		}
		if se.Behavior != "" {
			body["Behavior"] = se.Behavior
		}
		if se.TTL != "" {
			body["TTL"] = se.TTL
		}
	}
	return s.create(body, q)

}

func (s *Session) Create(se *SessionEntry, q *WriteOptions) (string, *WriteMeta, error) {
	var obj interface{}
	if se != nil {
		body := make(map[string]interface{})
		obj = body
		if se.Name != "" {
			body["Name"] = se.Name
		}
		if se.Node != "" {
			body["Node"] = se.Node
		}
		if se.LockDelay != 0 {
			body["LockDelay"] = durToMsec(se.LockDelay)
		}
		if len(se.Checks) > 0 {
			body["Checks"] = se.Checks
		}
		if se.Behavior != "" {
			body["Behavior"] = se.Behavior
		}
		if se.TTL != "" {
			body["TTL"] = se.TTL
		}
	}
	return s.create(obj, q)
}

func (s *Session) create(obj interface{}, q *WriteOptions) (string, *WriteMeta, error) {
	var out struct{ ID string }
	wm, err := s.c.write("/v1/session/create", obj, &out, q)
	if err != nil {
		return "", nil, err
	}
	return out.ID, wm, nil
}

func (s *Session) Destroy(id string, q *WriteOptions) (*WriteMeta, error) {
	wm, err := s.c.write("/v1/session/destroy/"+id, nil, nil, q)
	if err != nil {
		return nil, err
	}
	return wm, nil
}

func (s *Session) Renew(id string, q *WriteOptions) (*SessionEntry, *WriteMeta, error) {
	r := s.c.newRequest("PUT", "/v1/session/renew/"+id)
	r.setWriteOptions(q)
	rtt, resp, err := s.c.doRequest(r)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{RequestTime: rtt}

	if resp.StatusCode == 404 {
		return nil, wm, nil
	} else if resp.StatusCode != 200 {
		return nil, nil, fmt.Errorf("Unexpected response code: %d", resp.StatusCode)
	}

	var entries []*SessionEntry
	if err := decodeBody(resp, &entries); err != nil {
		return nil, nil, fmt.Errorf("Failed to read response: %v", err)
	}
	if len(entries) > 0 {
		return entries[0], wm, nil
	}
	return nil, wm, nil
}

func (s *Session) RenewPeriodic(initialTTL string, id string, q *WriteOptions, doneCh <-chan struct{}) error {
	ctx := q.Context()

	ttl, err := time.ParseDuration(initialTTL)
	if err != nil {
		return err
	}

	waitDur := ttl / 2
	lastRenewTime := time.Now()
	var lastErr error
	for {
		if time.Since(lastRenewTime) > ttl {
			return lastErr
		}
		select {
		case <-time.After(waitDur):
			entry, _, err := s.Renew(id, q)
			if err != nil {
				waitDur = time.Second
				lastErr = err
				continue
			}
			if entry == nil {
				return ErrSessionExpired
			}

			ttl, _ = time.ParseDuration(entry.TTL)
			waitDur = ttl / 2
			lastRenewTime = time.Now()

		case <-doneCh:

			s.Destroy(id, q)
			return nil

		case <-ctx.Done():

			return ctx.Err()
		}
	}
}

func (s *Session) Info(id string, q *QueryOptions) (*SessionEntry, *QueryMeta, error) {
	var entries []*SessionEntry
	qm, err := s.c.query("/v1/session/info/"+id, &entries, q)
	if err != nil {
		return nil, nil, err
	}
	if len(entries) > 0 {
		return entries[0], qm, nil
	}
	return nil, qm, nil
}

func (s *Session) Node(node string, q *QueryOptions) ([]*SessionEntry, *QueryMeta, error) {
	var entries []*SessionEntry
	qm, err := s.c.query("/v1/session/node/"+node, &entries, q)
	if err != nil {
		return nil, nil, err
	}
	return entries, qm, nil
}

func (s *Session) List(q *QueryOptions) ([]*SessionEntry, *QueryMeta, error) {
	var entries []*SessionEntry
	qm, err := s.c.query("/v1/session/list", &entries, q)
	if err != nil {
		return nil, nil, err
	}
	return entries, qm, nil
}
