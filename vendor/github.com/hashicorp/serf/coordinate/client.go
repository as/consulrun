package coordinate

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

type Client struct {
	coord *Coordinate

	origin *Coordinate

	config *Config

	adjustmentIndex uint

	adjustmentSamples []float64

	latencyFilterSamples map[string][]float64

	stats ClientStats

	mutex sync.RWMutex
}

type ClientStats struct {
	Resets int
}

func NewClient(config *Config) (*Client, error) {
	if !(config.Dimensionality > 0) {
		return nil, fmt.Errorf("dimensionality must be >0")
	}

	return &Client{
		coord:                NewCoordinate(config),
		origin:               NewCoordinate(config),
		config:               config,
		adjustmentIndex:      0,
		adjustmentSamples:    make([]float64, config.AdjustmentWindowSize),
		latencyFilterSamples: make(map[string][]float64),
	}, nil
}

func (c *Client) GetCoordinate() *Coordinate {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.coord.Clone()
}

func (c *Client) SetCoordinate(coord *Coordinate) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := c.checkCoordinate(coord); err != nil {
		return err
	}

	c.coord = coord.Clone()
	return nil
}

func (c *Client) ForgetNode(node string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.latencyFilterSamples, node)
}

func (c *Client) Stats() ClientStats {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.stats
}

func (c *Client) checkCoordinate(coord *Coordinate) error {
	if !c.coord.IsCompatibleWith(coord) {
		return fmt.Errorf("dimensions aren't compatible")
	}

	if !coord.IsValid() {
		return fmt.Errorf("coordinate is invalid")
	}

	return nil
}

func (c *Client) latencyFilter(node string, rttSeconds float64) float64 {
	samples, ok := c.latencyFilterSamples[node]
	if !ok {
		samples = make([]float64, 0, c.config.LatencyFilterSize)
	}

	samples = append(samples, rttSeconds)
	if len(samples) > int(c.config.LatencyFilterSize) {
		samples = samples[1:]
	}
	c.latencyFilterSamples[node] = samples

	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)
	return sorted[len(sorted)/2]
}

func (c *Client) updateVivaldi(other *Coordinate, rttSeconds float64) {
	const zeroThreshold = 1.0e-6

	dist := c.coord.DistanceTo(other).Seconds()
	if rttSeconds < zeroThreshold {
		rttSeconds = zeroThreshold
	}
	wrongness := math.Abs(dist-rttSeconds) / rttSeconds

	totalError := c.coord.Error + other.Error
	if totalError < zeroThreshold {
		totalError = zeroThreshold
	}
	weight := c.coord.Error / totalError

	c.coord.Error = c.config.VivaldiCE*weight*wrongness + c.coord.Error*(1.0-c.config.VivaldiCE*weight)
	if c.coord.Error > c.config.VivaldiErrorMax {
		c.coord.Error = c.config.VivaldiErrorMax
	}

	delta := c.config.VivaldiCC * weight
	force := delta * (rttSeconds - dist)
	c.coord = c.coord.ApplyForce(c.config, force, other)
}

func (c *Client) updateAdjustment(other *Coordinate, rttSeconds float64) {
	if c.config.AdjustmentWindowSize == 0 {
		return
	}

	dist := c.coord.rawDistanceTo(other)
	c.adjustmentSamples[c.adjustmentIndex] = rttSeconds - dist
	c.adjustmentIndex = (c.adjustmentIndex + 1) % c.config.AdjustmentWindowSize

	sum := 0.0
	for _, sample := range c.adjustmentSamples {
		sum += sample
	}
	c.coord.Adjustment = sum / (2.0 * float64(c.config.AdjustmentWindowSize))
}

func (c *Client) updateGravity() {
	dist := c.origin.DistanceTo(c.coord).Seconds()
	force := -1.0 * math.Pow(dist/c.config.GravityRho, 2.0)
	c.coord = c.coord.ApplyForce(c.config, force, c.origin)
}

func (c *Client) Update(node string, other *Coordinate, rtt time.Duration) (*Coordinate, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := c.checkCoordinate(other); err != nil {
		return nil, err
	}

	const maxRTT = 10 * time.Second
	if rtt < 0 || rtt > maxRTT {
		return nil, fmt.Errorf("round trip time not in valid range, duration %v is not a positive value less than %v ", rtt, maxRTT)
	}
	if rtt == 0 {
		metrics.IncrCounter([]string{"serf", "coordinate", "zero-rtt"}, 1)
	}

	rttSeconds := c.latencyFilter(node, rtt.Seconds())
	c.updateVivaldi(other, rttSeconds)
	c.updateAdjustment(other, rttSeconds)
	c.updateGravity()
	if !c.coord.IsValid() {
		c.stats.Resets++
		c.coord = NewCoordinate(c.config)
	}

	return c.coord.Clone(), nil
}

func (c *Client) DistanceTo(other *Coordinate) time.Duration {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.coord.DistanceTo(other)
}
