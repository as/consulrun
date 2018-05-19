package coordinate

import (
	"math"
	"math/rand"
	"time"
)

type Coordinate struct {
	Vec []float64

	Error float64

	Adjustment float64

	Height float64
}

const (
	secondsToNanoseconds = 1.0e9

	zeroThreshold = 1.0e-6
)

type DimensionalityConflictError struct{}

func (e DimensionalityConflictError) Error() string {
	return "coordinate dimensionality does not match"
}

func NewCoordinate(config *Config) *Coordinate {
	return &Coordinate{
		Vec:        make([]float64, config.Dimensionality),
		Error:      config.VivaldiErrorMax,
		Adjustment: 0.0,
		Height:     config.HeightMin,
	}
}

func (c *Coordinate) Clone() *Coordinate {
	vec := make([]float64, len(c.Vec))
	copy(vec, c.Vec)
	return &Coordinate{
		Vec:        vec,
		Error:      c.Error,
		Adjustment: c.Adjustment,
		Height:     c.Height,
	}
}

func componentIsValid(f float64) bool {
	return !math.IsInf(f, 0) && !math.IsNaN(f)
}

func (c *Coordinate) IsValid() bool {
	for i := range c.Vec {
		if !componentIsValid(c.Vec[i]) {
			return false
		}
	}

	return componentIsValid(c.Error) &&
		componentIsValid(c.Adjustment) &&
		componentIsValid(c.Height)
}

func (c *Coordinate) IsCompatibleWith(other *Coordinate) bool {
	return len(c.Vec) == len(other.Vec)
}

func (c *Coordinate) ApplyForce(config *Config, force float64, other *Coordinate) *Coordinate {
	if !c.IsCompatibleWith(other) {
		panic(DimensionalityConflictError{})
	}

	ret := c.Clone()
	unit, mag := unitVectorAt(c.Vec, other.Vec)
	ret.Vec = add(ret.Vec, mul(unit, force))
	if mag > zeroThreshold {
		ret.Height = (ret.Height+other.Height)*force/mag + ret.Height
		ret.Height = math.Max(ret.Height, config.HeightMin)
	}
	return ret
}

func (c *Coordinate) DistanceTo(other *Coordinate) time.Duration {
	if !c.IsCompatibleWith(other) {
		panic(DimensionalityConflictError{})
	}

	dist := c.rawDistanceTo(other)
	adjustedDist := dist + c.Adjustment + other.Adjustment
	if adjustedDist > 0.0 {
		dist = adjustedDist
	}
	return time.Duration(dist * secondsToNanoseconds)
}

func (c *Coordinate) rawDistanceTo(other *Coordinate) float64 {
	return magnitude(diff(c.Vec, other.Vec)) + c.Height + other.Height
}

func add(vec1 []float64, vec2 []float64) []float64 {
	ret := make([]float64, len(vec1))
	for i := range ret {
		ret[i] = vec1[i] + vec2[i]
	}
	return ret
}

func diff(vec1 []float64, vec2 []float64) []float64 {
	ret := make([]float64, len(vec1))
	for i := range ret {
		ret[i] = vec1[i] - vec2[i]
	}
	return ret
}

func mul(vec []float64, factor float64) []float64 {
	ret := make([]float64, len(vec))
	for i := range vec {
		ret[i] = vec[i] * factor
	}
	return ret
}

func magnitude(vec []float64) float64 {
	sum := 0.0
	for i := range vec {
		sum += vec[i] * vec[i]
	}
	return math.Sqrt(sum)
}

func unitVectorAt(vec1 []float64, vec2 []float64) ([]float64, float64) {
	ret := diff(vec1, vec2)

	if mag := magnitude(ret); mag > zeroThreshold {
		return mul(ret, 1.0/mag), mag
	}

	for i := range ret {
		ret[i] = rand.Float64() - 0.5
	}
	if mag := magnitude(ret); mag > zeroThreshold {
		return mul(ret, 1.0/mag), 0.0
	}

	ret = make([]float64, len(ret))
	ret[0] = 1.0
	return ret, 0.0
}
