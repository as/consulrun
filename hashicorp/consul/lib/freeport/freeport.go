// Package freeport provides a helper for allocating free ports across multiple
package freeport

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/mitchellh/go-testing-interface"
)

const (
	blockSize = 1500

	maxBlocks = 30

	lowPort = 10000

	attempts = 10
)

var (
	firstPort int

	lockLn net.Listener

	mu sync.Mutex

	once sync.Once

	port int
)

func initialize() {
	if lowPort+maxBlocks*blockSize > 65535 {
		panic("freeport: block size too big or too many blocks requested")
	}

	rand.Seed(time.Now().UnixNano())
	firstPort, lockLn = alloc()
}

func alloc() (int, net.Listener) {
	for i := 0; i < attempts; i++ {
		block := int(rand.Int31n(int32(maxBlocks)))
		firstPort := lowPort + block*blockSize
		ln, err := net.ListenTCP("tcp", tcpAddr("127.0.0.1", firstPort))
		if err != nil {
			continue
		}

		return firstPort, ln
	}
	panic("freeport: cannot allocate port block")
}

func tcpAddr(ip string, port int) *net.TCPAddr {
	return &net.TCPAddr{IP: net.ParseIP(ip), Port: port}
}

func Get(n int) (ports []int) {
	ports, err := Free(n)
	if err != nil {
		panic(err)
	}

	return ports
}

func GetT(t testing.T, n int) (ports []int) {
	ports, err := Free(n)
	if err != nil {
		t.Fatalf("Failed retrieving free port: %v", err)
	}

	return ports
}

func Free(n int) (ports []int, err error) {
	mu.Lock()
	defer mu.Unlock()

	if n > blockSize-1 {
		return nil, fmt.Errorf("freeport: block size too small")
	}

	once.Do(initialize)

	for len(ports) < n {
		port++

		if port < firstPort+1 || port >= firstPort+blockSize {
			port = firstPort + 1
		}

		ln, err := net.ListenTCP("tcp", tcpAddr("127.0.0.1", port))
		if err != nil {

			continue
		}
		ln.Close()

		ports = append(ports, port)
	}

	return ports, nil
}
