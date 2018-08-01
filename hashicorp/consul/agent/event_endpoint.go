package agent

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
)

const (
	maxQueryTime = 600 * time.Second
)

func (s *HTTPServer) EventFire(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	var dc string
	s.parseDC(req, &dc)

	event := &UserEvent{}
	event.Name = strings.TrimPrefix(req.URL.Path, "/v1/event/fire/")
	if event.Name == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing name")
		return nil, nil
	}

	var token string
	s.parseToken(req, &token)

	if filt := req.URL.Query().Get("node"); filt != "" {
		event.NodeFilter = filt
	}
	if filt := req.URL.Query().Get("service"); filt != "" {
		event.ServiceFilter = filt
	}
	if filt := req.URL.Query().Get("tag"); filt != "" {
		event.TagFilter = filt
	}

	if req.ContentLength > 0 {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, req.Body); err != nil {
			return nil, err
		}
		event.Payload = buf.Bytes()
	}

	if err := s.agent.UserEvent(dc, token, event); err != nil {
		if acl.IsErrPermissionDenied(err) {
			resp.WriteHeader(http.StatusForbidden)
			fmt.Fprint(resp, acl.ErrPermissionDenied.Error())
			return nil, nil
		}
		resp.WriteHeader(http.StatusInternalServerError)
		return nil, err
	}

	return event, nil
}

func (s *HTTPServer) EventList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	var b structs.QueryOptions
	if parseWait(resp, req, &b) {
		return nil, nil
	}

	var token string
	s.parseToken(req, &token)
	acl, err := s.agent.resolveToken(token)
	if err != nil {
		return nil, err
	}

	var nameFilter string
	if filt := req.URL.Query().Get("name"); filt != "" {
		nameFilter = filt
	}

	var timeout <-chan time.Time
	var notifyCh chan struct{}

	if b.MinQueryIndex == 0 {
		goto RUN_QUERY
	}

	if b.MaxQueryTime > maxQueryTime {
		b.MaxQueryTime = maxQueryTime
	}

	if b.MinQueryIndex > 0 && b.MaxQueryTime == 0 {
		b.MaxQueryTime = maxQueryTime
	}

	if b.MaxQueryTime > 0 {
		timeout = time.After(b.MaxQueryTime)
	}

SETUP_NOTIFY:
	if b.MinQueryIndex > 0 {
		notifyCh = make(chan struct{}, 1)
		s.agent.eventNotify.Wait(notifyCh)
	}

RUN_QUERY:

	events := s.agent.UserEvents()

	if acl != nil {
		for i := 0; i < len(events); i++ {
			name := events[i].Name
			if acl.EventRead(name) {
				continue
			}
			s.agent.logger.Printf("[DEBUG] agent: dropping event %q from result due to ACLs", name)
			events = append(events[:i], events[i+1:]...)
			i--
		}
	}

	if nameFilter != "" {
		for i := 0; i < len(events); i++ {
			if events[i].Name != nameFilter {
				events = append(events[:i], events[i+1:]...)
				i--
			}
		}
	}

	var index uint64
	if len(events) == 0 {

		index = 1
	} else {
		last := events[len(events)-1]
		index = uuidToUint64(last.ID)
	}
	setIndex(resp, index)

	if index > 0 && index == b.MinQueryIndex {
		select {
		case <-notifyCh:
			goto SETUP_NOTIFY
		case <-timeout:
		}
	}
	return events, nil
}

func uuidToUint64(uuid string) uint64 {
	lower := uuid[0:8] + uuid[9:13] + uuid[14:18]
	upper := uuid[19:23] + uuid[24:36]
	lowVal, err := strconv.ParseUint(lower, 16, 64)
	if err != nil {
		panic("Failed to convert " + lower)
	}
	highVal, err := strconv.ParseUint(upper, 16, 64)
	if err != nil {
		panic("Failed to convert " + upper)
	}
	return lowVal ^ highVal
}
