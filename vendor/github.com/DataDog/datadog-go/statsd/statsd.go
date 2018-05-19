// Copyright 2013 Ooyala, Inc.

/*
Package statsd provides a Go dogstatsd client. Dogstatsd extends the popular statsd,
adding tags and histograms and pushing upstream to Datadog.

Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

Example Usage:

    // Create the client
    c, err := statsd.New("127.0.0.1:8125")
    if err != nil {
        log.Fatal(err)
    }
    // Prefix every metric with the app name
    c.Namespace = "flubber."
    // Send the EC2 availability zone as a tag with every metric
    c.Tags = append(c.Tags, "us-east-1a")
    err = c.Gauge("request.duration", 1.2, nil, 1)

statsd is based on go-statsd-client.
*/
package statsd

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
OptimalPayloadSize defines the optimal payload size for a UDP datagram, 1432 bytes
is optimal for regular networks with an MTU of 1500 so datagrams don't get
fragmented. It's generally recommended not to fragment UDP datagrams as losing
a single fragment will cause the entire datagram to be lost.

This can be increased if your network has a greater MTU or you don't mind UDP
datagrams getting fragmented. The practical limit is MaxUDPPayloadSize
*/
const OptimalPayloadSize = 1432

/*
MaxUDPPayloadSize defines the maximum payload size for a UDP datagram.
Its value comes from the calculation: 65535 bytes Max UDP datagram size -
8byte UDP header - 60byte max IP headers
any number greater than that will see frames being cut out.
*/
const MaxUDPPayloadSize = 65467

type Client struct {
	conn net.Conn

	Namespace string

	Tags []string

	bufferLength int
	flushTime    time.Duration
	commands     []string
	buffer       bytes.Buffer
	stop         bool
	sync.Mutex
}

func New(addr string) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn}
	return client, nil
}

func NewBuffered(addr string, buflen int) (*Client, error) {
	client, err := New(addr)
	if err != nil {
		return nil, err
	}
	client.bufferLength = buflen
	client.commands = make([]string, 0, buflen)
	client.flushTime = time.Millisecond * 100
	go client.watch()
	return client, nil
}

func (c *Client) format(name, value string, tags []string, rate float64) string {
	var buf bytes.Buffer
	if c.Namespace != "" {
		buf.WriteString(c.Namespace)
	}
	buf.WriteString(name)
	buf.WriteString(":")
	buf.WriteString(value)
	if rate < 1 {
		buf.WriteString(`|@`)
		buf.WriteString(strconv.FormatFloat(rate, 'f', -1, 64))
	}

	tags = append(c.Tags, tags...)
	if len(tags) > 0 {
		buf.WriteString("|#")
		buf.WriteString(tags[0])
		for _, tag := range tags[1:] {
			buf.WriteString(",")
			buf.WriteString(tag)
		}
	}
	return buf.String()
}

func (c *Client) watch() {
	for range time.Tick(c.flushTime) {
		if c.stop {
			return
		}
		c.Lock()
		if len(c.commands) > 0 {

			c.flush()
		}
		c.Unlock()
	}
}

func (c *Client) append(cmd string) error {
	c.commands = append(c.commands, cmd)

	if len(c.commands) == c.bufferLength {
		if err := c.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) joinMaxSize(cmds []string, sep string, maxSize int) ([][]byte, []int) {
	c.buffer.Reset() //clear buffer

	var frames [][]byte
	var ncmds []int
	sepBytes := []byte(sep)
	sepLen := len(sep)

	elem := 0
	for _, cmd := range cmds {
		needed := len(cmd)

		if elem != 0 {
			needed = needed + sepLen
		}

		if c.buffer.Len()+needed <= maxSize {
			if elem != 0 {
				c.buffer.Write(sepBytes)
			}
			c.buffer.WriteString(cmd)
			elem++
		} else {
			frames = append(frames, copyAndResetBuffer(&c.buffer))
			ncmds = append(ncmds, elem)

			c.buffer.WriteString(cmd)
			elem = 1
		}
	}

	if c.buffer.Len() > 0 {
		frames = append(frames, copyAndResetBuffer(&c.buffer))
		ncmds = append(ncmds, elem)
	}

	return frames, ncmds
}

func copyAndResetBuffer(buf *bytes.Buffer) []byte {
	tmpBuf := make([]byte, buf.Len())
	copy(tmpBuf, buf.Bytes())
	buf.Reset()
	return tmpBuf
}

func (c *Client) flush() error {
	frames, flushable := c.joinMaxSize(c.commands, "\n", OptimalPayloadSize)
	var err error
	cmdsFlushed := 0
	for i, data := range frames {
		_, e := c.conn.Write(data)
		if e != nil {
			err = e
			break
		}
		cmdsFlushed += flushable[i]
	}

	if cmdsFlushed == len(c.commands) {
		c.commands = c.commands[:0]
	} else {

		c.commands = c.commands[cmdsFlushed+1:]
	}
	return err
}

func (c *Client) sendMsg(msg string) error {

	c.Lock()
	defer c.Unlock()
	if c.bufferLength > 0 {

		if len(msg) > MaxUDPPayloadSize {
			return errors.New("message size exceeds MaxUDPPayloadSize")
		}
		return c.append(msg)
	}
	_, err := c.conn.Write([]byte(msg))
	return err
}

func (c *Client) send(name, value string, tags []string, rate float64) error {
	if c == nil {
		return nil
	}
	if rate < 1 && rand.Float64() > rate {
		return nil
	}
	data := c.format(name, value, tags, rate)
	return c.sendMsg(data)
}

func (c *Client) Gauge(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|g", value)
	return c.send(name, stat, tags, rate)
}

func (c *Client) Count(name string, value int64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%d|c", value)
	return c.send(name, stat, tags, rate)
}

func (c *Client) Histogram(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|h", value)
	return c.send(name, stat, tags, rate)
}

func (c *Client) Set(name string, value string, tags []string, rate float64) error {
	stat := fmt.Sprintf("%s|s", value)
	return c.send(name, stat, tags, rate)
}

func (c *Client) TimeInMilliseconds(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|ms", value)
	return c.send(name, stat, tags, rate)
}

func (c *Client) Event(e *Event) error {
	stat, err := e.Encode(c.Tags...)
	if err != nil {
		return err
	}
	return c.sendMsg(stat)
}

func (c *Client) SimpleEvent(title, text string) error {
	e := NewEvent(title, text)
	return c.Event(e)
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.stop = true
	return c.conn.Close()
}

type eventAlertType string

const (
	Info eventAlertType = "info"

	Error eventAlertType = "error"

	Warning eventAlertType = "warning"

	Success eventAlertType = "success"
)

type eventPriority string

const (
	Normal eventPriority = "normal"

	Low eventPriority = "low"
)

type Event struct {
	Title string

	Text string

	Timestamp time.Time

	Hostname string

	AggregationKey string

	Priority eventPriority

	SourceTypeName string

	AlertType eventAlertType

	Tags []string
}

func NewEvent(title, text string) *Event {
	return &Event{
		Title: title,
		Text:  text,
	}
}

func (e Event) Check() error {
	if len(e.Title) == 0 {
		return fmt.Errorf("statsd.Event title is required")
	}
	if len(e.Text) == 0 {
		return fmt.Errorf("statsd.Event text is required")
	}
	return nil
}

func (e Event) Encode(tags ...string) (string, error) {
	err := e.Check()
	if err != nil {
		return "", err
	}
	text := e.escapedText()

	var buffer bytes.Buffer
	buffer.WriteString("_e{")
	buffer.WriteString(strconv.FormatInt(int64(len(e.Title)), 10))
	buffer.WriteRune(',')
	buffer.WriteString(strconv.FormatInt(int64(len(text)), 10))
	buffer.WriteString("}:")
	buffer.WriteString(e.Title)
	buffer.WriteRune('|')
	buffer.WriteString(text)

	if !e.Timestamp.IsZero() {
		buffer.WriteString("|d:")
		buffer.WriteString(strconv.FormatInt(int64(e.Timestamp.Unix()), 10))
	}

	if len(e.Hostname) != 0 {
		buffer.WriteString("|h:")
		buffer.WriteString(e.Hostname)
	}

	if len(e.AggregationKey) != 0 {
		buffer.WriteString("|k:")
		buffer.WriteString(e.AggregationKey)

	}

	if len(e.Priority) != 0 {
		buffer.WriteString("|p:")
		buffer.WriteString(string(e.Priority))
	}

	if len(e.SourceTypeName) != 0 {
		buffer.WriteString("|s:")
		buffer.WriteString(e.SourceTypeName)
	}

	if len(e.AlertType) != 0 {
		buffer.WriteString("|t:")
		buffer.WriteString(string(e.AlertType))
	}

	if len(tags)+len(e.Tags) > 0 {
		all := make([]string, 0, len(tags)+len(e.Tags))
		all = append(all, tags...)
		all = append(all, e.Tags...)
		buffer.WriteString("|#")
		buffer.WriteString(all[0])
		for _, tag := range all[1:] {
			buffer.WriteString(",")
			buffer.WriteString(tag)
		}
	}

	return buffer.String(), nil
}

func (e Event) escapedText() string {
	return strings.Replace(e.Text, "\n", "\\n", -1)
}
