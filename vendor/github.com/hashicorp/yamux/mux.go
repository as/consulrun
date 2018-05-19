package yamux

import (
	"fmt"
	"io"
	"os"
	"time"
)

type Config struct {
	AcceptBacklog int

	EnableKeepAlive bool

	KeepAliveInterval time.Duration

	ConnectionWriteTimeout time.Duration

	MaxStreamWindowSize uint32

	LogOutput io.Writer
}

func DefaultConfig() *Config {
	return &Config{
		AcceptBacklog:          256,
		EnableKeepAlive:        true,
		KeepAliveInterval:      30 * time.Second,
		ConnectionWriteTimeout: 10 * time.Second,
		MaxStreamWindowSize:    initialStreamWindow,
		LogOutput:              os.Stderr,
	}
}

func VerifyConfig(config *Config) error {
	if config.AcceptBacklog <= 0 {
		return fmt.Errorf("backlog must be positive")
	}
	if config.KeepAliveInterval == 0 {
		return fmt.Errorf("keep-alive interval must be positive")
	}
	if config.MaxStreamWindowSize < initialStreamWindow {
		return fmt.Errorf("MaxStreamWindowSize must be larger than %d", initialStreamWindow)
	}
	return nil
}

func Server(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, false), nil
}

func Client(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, true), nil
}
