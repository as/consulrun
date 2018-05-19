package api

type Status struct {
	c *Client
}

func (c *Client) Status() *Status {
	return &Status{c}
}

func (s *Status) Leader() (string, error) {
	r := s.c.newRequest("GET", "/v1/status/leader")
	_, resp, err := requireOK(s.c.doRequest(r))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var leader string
	if err := decodeBody(resp, &leader); err != nil {
		return "", err
	}
	return leader, nil
}

func (s *Status) Peers() ([]string, error) {
	r := s.c.newRequest("GET", "/v1/status/peers")
	_, resp, err := requireOK(s.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var peers []string
	if err := decodeBody(resp, &peers); err != nil {
		return nil, err
	}
	return peers, nil
}
