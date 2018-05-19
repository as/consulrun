package serf

type latestUserEvents struct {
	LTime  LamportTime
	Events []Event
}

type userEventCoalescer struct {
	events map[string]*latestUserEvents
}

func (c *userEventCoalescer) Handle(e Event) bool {

	if e.EventType() != EventUser {
		return false
	}

	user := e.(UserEvent)
	return user.Coalesce
}

func (c *userEventCoalescer) Coalesce(e Event) {
	user := e.(UserEvent)
	latest, ok := c.events[user.Name]

	if !ok || latest.LTime < user.LTime {
		latest = &latestUserEvents{
			LTime:  user.LTime,
			Events: []Event{e},
		}
		c.events[user.Name] = latest
		return
	}

	if latest.LTime == user.LTime {
		latest.Events = append(latest.Events, e)
	}
}

func (c *userEventCoalescer) Flush(outChan chan<- Event) {
	for _, latest := range c.events {
		for _, e := range latest.Events {
			outChan <- e
		}
	}
	c.events = make(map[string]*latestUserEvents)
}
