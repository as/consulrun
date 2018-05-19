package lib

import (
	crand "crypto/rand"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

var (
	once sync.Once

	SeededSecurely bool
)

func SeedMathRand() {
	once.Do(func() {
		n, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			rand.Seed(time.Now().UTC().UnixNano())
			return
		}
		rand.Seed(n.Int64())
		SeededSecurely = true
	})
}
