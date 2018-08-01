package watch

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	consulapi "github.com/as/consulrun/hashicorp/consul/api"
	"github.com/mitchellh/mapstructure"
)

const DefaultTimeout = 10 * time.Second

type Plan struct {
	Datacenter  string
	Token       string
	Type        string
	HandlerType string
	Exempt      map[string]interface{}

	Watcher   WatcherFunc
	Handler   HandlerFunc
	LogOutput io.Writer

	address    string
	client     *consulapi.Client
	lastIndex  uint64
	lastResult interface{}

	stop       bool
	stopCh     chan struct{}
	stopLock   sync.Mutex
	cancelFunc context.CancelFunc
}

type HttpHandlerConfig struct {
	Path          string              `mapstructure:"path"`
	Method        string              `mapstructure:"method"`
	Timeout       time.Duration       `mapstructure:"-"`
	TimeoutRaw    string              `mapstructure:"timeout"`
	Header        map[string][]string `mapstructure:"header"`
	TLSSkipVerify bool                `mapstructure:"tls_skip_verify"`
}

type WatcherFunc func(*Plan) (uint64, interface{}, error)

type HandlerFunc func(uint64, interface{})

func Parse(params map[string]interface{}) (*Plan, error) {
	return ParseExempt(params, nil)
}

func ParseExempt(params map[string]interface{}, exempt []string) (*Plan, error) {
	plan := &Plan{
		stopCh: make(chan struct{}),
		Exempt: make(map[string]interface{}),
	}

	if err := assignValue(params, "datacenter", &plan.Datacenter); err != nil {
		return nil, err
	}
	if err := assignValue(params, "token", &plan.Token); err != nil {
		return nil, err
	}
	if err := assignValue(params, "type", &plan.Type); err != nil {
		return nil, err
	}

	if plan.Type == "" {
		return nil, fmt.Errorf("Watch type must be specified")
	}

	if err := assignValue(params, "handler_type", &plan.HandlerType); err != nil {
		return nil, err
	}
	switch plan.HandlerType {
	case "http":
		if _, ok := params["http_handler_config"]; !ok {
			return nil, fmt.Errorf("Handler type 'http' requires 'http_handler_config' to be set")
		}
		config, err := parseHttpHandlerConfig(params["http_handler_config"])
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("Failed to parse 'http_handler_config': %v", err))
		}
		plan.Exempt["http_handler_config"] = config
		delete(params, "http_handler_config")

	case "script":

	}

	factory := watchFuncFactory[plan.Type]
	if factory == nil {
		return nil, fmt.Errorf("Unsupported watch type: %s", plan.Type)
	}

	fn, err := factory(params)
	if err != nil {
		return nil, err
	}
	plan.Watcher = fn

	if len(exempt) > 0 {
		for _, ex := range exempt {
			val, ok := params[ex]
			if ok {
				plan.Exempt[ex] = val
				delete(params, ex)
			}
		}
	}

	if len(params) != 0 {
		var bad []string
		for key := range params {
			bad = append(bad, key)
		}
		return nil, fmt.Errorf("Invalid parameters: %v", bad)
	}
	return plan, nil
}

func assignValue(params map[string]interface{}, name string, out *string) error {
	if raw, ok := params[name]; ok {
		val, ok := raw.(string)
		if !ok {
			return fmt.Errorf("Expecting %s to be a string", name)
		}
		*out = val
		delete(params, name)
	}
	return nil
}

func assignValueBool(params map[string]interface{}, name string, out *bool) error {
	if raw, ok := params[name]; ok {
		val, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("Expecting %s to be a boolean", name)
		}
		*out = val
		delete(params, name)
	}
	return nil
}

func parseHttpHandlerConfig(configParams interface{}) (*HttpHandlerConfig, error) {
	var config HttpHandlerConfig
	if err := mapstructure.Decode(configParams, &config); err != nil {
		return nil, err
	}

	if config.Path == "" {
		return nil, fmt.Errorf("Requires 'path' to be set")
	}
	if config.Method == "" {
		config.Method = "POST"
	}
	if config.TimeoutRaw == "" {
		config.Timeout = DefaultTimeout
	} else if timeout, err := time.ParseDuration(config.TimeoutRaw); err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("Failed to parse timeout: %v", err))
	} else {
		config.Timeout = timeout
	}

	return &config, nil
}
