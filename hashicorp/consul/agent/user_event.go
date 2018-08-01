package agent

import (
	"fmt"
	"regexp"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-uuid"
)

const (
	userEventMaxVersion = 1

	remoteExecName = "_rexec"
)

type UserEvent struct {
	ID string

	Name string `codec:"n"`

	Payload []byte `codec:"p,omitempty"`

	NodeFilter string `codec:"nf,omitempty"`

	ServiceFilter string `codec:"sf,omitempty"`

	TagFilter string `codec:"tf,omitempty"`

	Version int `codec:"v"`

	LTime uint64 `codec:"-"`
}

func validateUserEventParams(params *UserEvent) error {

	if params.Name == "" {
		return fmt.Errorf("User event missing name")
	}
	if params.TagFilter != "" && params.ServiceFilter == "" {
		return fmt.Errorf("Cannot provide tag filter without service filter")
	}
	if params.NodeFilter != "" {
		if _, err := regexp.Compile(params.NodeFilter); err != nil {
			return fmt.Errorf("Invalid node filter: %v", err)
		}
	}
	if params.ServiceFilter != "" {
		if _, err := regexp.Compile(params.ServiceFilter); err != nil {
			return fmt.Errorf("Invalid service filter: %v", err)
		}
	}
	if params.TagFilter != "" {
		if _, err := regexp.Compile(params.TagFilter); err != nil {
			return fmt.Errorf("Invalid tag filter: %v", err)
		}
	}
	return nil
}

func (a *Agent) UserEvent(dc, token string, params *UserEvent) error {

	if err := validateUserEventParams(params); err != nil {
		return err
	}

	var err error
	if params.ID, err = uuid.GenerateUUID(); err != nil {
		return fmt.Errorf("UUID generation failed: %v", err)
	}
	params.Version = userEventMaxVersion
	payload, err := encodeMsgPack(&params)
	if err != nil {
		return fmt.Errorf("UserEvent encoding failed: %v", err)
	}

	args := structs.EventFireRequest{
		Datacenter:   dc,
		Name:         params.Name,
		Payload:      payload,
		QueryOptions: structs.QueryOptions{Token: token},
	}

	args.AllowStale = true
	var out structs.EventFireResponse
	return a.RPC("Internal.EventFire", &args, &out)
}

func (a *Agent) handleEvents() {
	for {
		select {
		case e := <-a.eventCh:

			msg := new(UserEvent)
			if err := decodeMsgPack(e.Payload, msg); err != nil {
				a.logger.Printf("[ERR] agent: Failed to decode event: %v", err)
				continue
			}
			msg.LTime = uint64(e.LTime)

			if !a.shouldProcessUserEvent(msg) {
				continue
			}

			a.ingestUserEvent(msg)

		case <-a.shutdownCh:
			return
		}
	}
}

func (a *Agent) shouldProcessUserEvent(msg *UserEvent) bool {

	if msg.Version > userEventMaxVersion {
		a.logger.Printf("[WARN] agent: Event version %d may have unsupported features (%s)",
			msg.Version, msg.Name)
	}

	if msg.NodeFilter != "" {
		re, err := regexp.Compile(msg.NodeFilter)
		if err != nil {
			a.logger.Printf("[ERR] agent: Failed to parse node filter '%s' for event '%s': %v",
				msg.NodeFilter, msg.Name, err)
			return false
		}
		if !re.MatchString(a.config.NodeName) {
			return false
		}
	}

	if msg.ServiceFilter != "" {
		re, err := regexp.Compile(msg.ServiceFilter)
		if err != nil {
			a.logger.Printf("[ERR] agent: Failed to parse service filter '%s' for event '%s': %v",
				msg.ServiceFilter, msg.Name, err)
			return false
		}

		var tagRe *regexp.Regexp
		if msg.TagFilter != "" {
			re, err := regexp.Compile(msg.TagFilter)
			if err != nil {
				a.logger.Printf("[ERR] agent: Failed to parse tag filter '%s' for event '%s': %v",
					msg.TagFilter, msg.Name, err)
				return false
			}
			tagRe = re
		}

		services := a.State.Services()
		found := false
	OUTER:
		for name, info := range services {

			if !re.MatchString(name) {
				continue
			}
			if tagRe == nil {
				found = true
				break
			}

			for _, tag := range info.Tags {
				if !tagRe.MatchString(tag) {
					continue
				}
				found = true
				break OUTER
			}
		}

		if !found {
			return false
		}
	}
	return true
}

func (a *Agent) ingestUserEvent(msg *UserEvent) {

	switch msg.Name {
	case remoteExecName:
		if a.config.DisableRemoteExec {
			a.logger.Printf("[INFO] agent: ignoring remote exec event (%s), disabled.", msg.ID)
		} else {
			go a.handleRemoteExec(msg)
		}
		return
	default:
		a.logger.Printf("[DEBUG] agent: new event: %s (%s)", msg.Name, msg.ID)
	}

	a.eventLock.Lock()
	defer func() {
		a.eventLock.Unlock()
		a.eventNotify.Notify()
	}()

	idx := a.eventIndex
	a.eventBuf[idx] = msg
	a.eventIndex = (idx + 1) % len(a.eventBuf)
}

func (a *Agent) UserEvents() []*UserEvent {
	n := len(a.eventBuf)
	out := make([]*UserEvent, n)
	a.eventLock.RLock()
	defer a.eventLock.RUnlock()

	if a.eventBuf[a.eventIndex] != nil {
		if a.eventIndex == 0 {
			copy(out, a.eventBuf)
		} else {
			copy(out, a.eventBuf[a.eventIndex:])
			copy(out[n-a.eventIndex:], a.eventBuf[:a.eventIndex])
		}
	} else {

		copy(out, a.eventBuf[:a.eventIndex])
		out = out[:a.eventIndex]
	}
	return out
}

func (a *Agent) LastUserEvent() *UserEvent {
	a.eventLock.RLock()
	defer a.eventLock.RUnlock()
	n := len(a.eventBuf)
	idx := (((a.eventIndex - 1) % n) + n) % n
	return a.eventBuf[idx]
}
