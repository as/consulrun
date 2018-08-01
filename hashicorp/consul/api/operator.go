package api

type Operator struct {
	c *Client
}

func (c *Client) Operator() *Operator {
	return &Operator{c}
}
