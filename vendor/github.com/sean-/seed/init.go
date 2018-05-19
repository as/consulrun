package seed

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

var (
	once   sync.Once
	secure bool
	seeded bool
)

func Init() (bool, error) {
	var err error
	once.Do(func() {
		var n *big.Int
		n, err = crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			rand.Seed(time.Now().UTC().UnixNano())
			return
		}
		rand.Seed(n.Int64())
		secure = true
		seeded = true
	})
	return seeded && secure, err
}

func MustInit() {
	once.Do(func() {
		n, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			panic(fmt.Sprintf("Unable to seed the random number generator: %v", err))
		}
		rand.Seed(n.Int64())
		secure = true
		seeded = true
	})
}

func Secure() bool {
	return secure
}

func Seeded() bool {
	return seeded
}
