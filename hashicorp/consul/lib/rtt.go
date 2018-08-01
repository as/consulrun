package lib

import (
	"math"
	"time"

	"github.com/as/consulrun/hashicorp/serf/coordinate"
)

func ComputeDistance(a *coordinate.Coordinate, b *coordinate.Coordinate) float64 {
	if a == nil || b == nil {
		return math.Inf(1.0)
	}

	return a.DistanceTo(b).Seconds()
}

type CoordinateSet map[string]*coordinate.Coordinate

func (cs CoordinateSet) Intersect(other CoordinateSet) (*coordinate.Coordinate, *coordinate.Coordinate) {

	segment := ""

	if len(cs) == 1 {
		for s := range cs {
			segment = s
		}
	}

	if len(other) == 1 {
		for s := range other {
			segment = s
		}
	}

	return cs[segment], other[segment]
}

func GenerateCoordinate(rtt time.Duration) *coordinate.Coordinate {
	coord := coordinate.NewCoordinate(coordinate.DefaultConfig())
	coord.Vec[0] = rtt.Seconds()
	coord.Height = 0
	return coord
}
