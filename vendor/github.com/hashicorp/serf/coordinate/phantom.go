package coordinate

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

func GenerateClients(nodes int, config *Config) ([]*Client, error) {
	clients := make([]*Client, nodes)
	for i := range clients {
		client, err := NewClient(config)
		if err != nil {
			return nil, err
		}

		clients[i] = client
	}
	return clients, nil
}

func GenerateLine(nodes int, spacing time.Duration) [][]time.Duration {
	truth := make([][]time.Duration, nodes)
	for i := range truth {
		truth[i] = make([]time.Duration, nodes)
	}

	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			rtt := time.Duration(j-i) * spacing
			truth[i][j], truth[j][i] = rtt, rtt
		}
	}
	return truth
}

func GenerateGrid(nodes int, spacing time.Duration) [][]time.Duration {
	truth := make([][]time.Duration, nodes)
	for i := range truth {
		truth[i] = make([]time.Duration, nodes)
	}

	n := int(math.Sqrt(float64(nodes)))
	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			x1, y1 := float64(i%n), float64(i/n)
			x2, y2 := float64(j%n), float64(j/n)
			dx, dy := x2-x1, y2-y1
			dist := math.Sqrt(dx*dx + dy*dy)
			rtt := time.Duration(dist * float64(spacing))
			truth[i][j], truth[j][i] = rtt, rtt
		}
	}
	return truth
}

func GenerateSplit(nodes int, lan time.Duration, wan time.Duration) [][]time.Duration {
	truth := make([][]time.Duration, nodes)
	for i := range truth {
		truth[i] = make([]time.Duration, nodes)
	}

	split := nodes / 2
	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			rtt := lan
			if (i <= split && j > split) || (i > split && j <= split) {
				rtt += wan
			}
			truth[i][j], truth[j][i] = rtt, rtt
		}
	}
	return truth
}

func GenerateCircle(nodes int, radius time.Duration) [][]time.Duration {
	truth := make([][]time.Duration, nodes)
	for i := range truth {
		truth[i] = make([]time.Duration, nodes)
	}

	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			var rtt time.Duration
			if i == 0 {
				rtt = 2 * radius
			} else {
				t1 := 2.0 * math.Pi * float64(i) / float64(nodes)
				x1, y1 := math.Cos(t1), math.Sin(t1)
				t2 := 2.0 * math.Pi * float64(j) / float64(nodes)
				x2, y2 := math.Cos(t2), math.Sin(t2)
				dx, dy := x2-x1, y2-y1
				dist := math.Sqrt(dx*dx + dy*dy)
				rtt = time.Duration(dist * float64(radius))
			}
			truth[i][j], truth[j][i] = rtt, rtt
		}
	}
	return truth
}

func GenerateRandom(nodes int, mean time.Duration, deviation time.Duration) [][]time.Duration {
	rand.Seed(1)

	truth := make([][]time.Duration, nodes)
	for i := range truth {
		truth[i] = make([]time.Duration, nodes)
	}

	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			rttSeconds := rand.NormFloat64()*deviation.Seconds() + mean.Seconds()
			rtt := time.Duration(rttSeconds * secondsToNanoseconds)
			truth[i][j], truth[j][i] = rtt, rtt
		}
	}
	return truth
}

func Simulate(clients []*Client, truth [][]time.Duration, cycles int) {
	rand.Seed(1)

	nodes := len(clients)
	for cycle := 0; cycle < cycles; cycle++ {
		for i := range clients {
			if j := rand.Intn(nodes); j != i {
				c := clients[j].GetCoordinate()
				rtt := truth[i][j]
				node := fmt.Sprintf("node_%d", j)
				clients[i].Update(node, c, rtt)
			}
		}
	}
}

type Stats struct {
	ErrorMax float64
	ErrorAvg float64
}

func Evaluate(clients []*Client, truth [][]time.Duration) (stats Stats) {
	nodes := len(clients)
	count := 0
	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			est := clients[i].DistanceTo(clients[j].GetCoordinate()).Seconds()
			actual := truth[i][j].Seconds()
			error := math.Abs(est-actual) / actual
			stats.ErrorMax = math.Max(stats.ErrorMax, error)
			stats.ErrorAvg += error
			count += 1
		}
	}

	stats.ErrorAvg /= float64(count)
	fmt.Printf("Error avg=%9.6f max=%9.6f\n", stats.ErrorAvg, stats.ErrorMax)
	return
}
