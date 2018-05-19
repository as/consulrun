package lib

import (
	"math/rand"
	"time"
)

const (
	minRate = 1.0 / 86400
)

func DurationMinusBuffer(intv time.Duration, buffer time.Duration, jitter int64) time.Duration {
	d := intv - buffer
	if jitter == 0 {
		d -= RandomStagger(d)
	} else {
		d -= RandomStagger(time.Duration(int64(d) / jitter))
	}
	return d
}

func DurationMinusBufferDomain(intv time.Duration, buffer time.Duration, jitter int64) (min time.Duration, max time.Duration) {
	max = intv - buffer
	if jitter == 0 {
		min = max
	} else {
		min = max - time.Duration(int64(max)/jitter)
	}
	return min, max
}

func RandomStagger(intv time.Duration) time.Duration {
	if intv == 0 {
		return 0
	}
	return time.Duration(uint64(rand.Int63()) % uint64(intv))
}

func RateScaledInterval(rate float64, min time.Duration, n int) time.Duration {
	if rate <= minRate {
		return min
	}
	interval := time.Duration(float64(time.Second) * float64(n) / rate)
	if interval < min {
		return min
	}

	return interval
}
