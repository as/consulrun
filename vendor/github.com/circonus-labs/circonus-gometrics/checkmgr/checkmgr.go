// Copyright 2016 Circonus, Inc. All rights reserved.

package checkmgr

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/circonus-labs/circonus-gometrics/api"
)

//
//
//

const (
	defaultCheckType             = "httptrap"
	defaultTrapMaxURLAge         = "60s"   // 60 seconds
	defaultBrokerMaxResponseTime = "500ms" // 500 milliseconds
	defaultForceMetricActivation = "false"
	statusActive                 = "active"
)

type CheckConfig struct {
	SubmissionURL string

	ID string

	InstanceID string

	SearchTag string

	DisplayName string

	Secret string

	Tags string

	MaxURLAge string

	ForceMetricActivation string
}

type BrokerConfig struct {
	ID string

	SelectTag string

	MaxResponseTime string
}

type Config struct {
	Log   *log.Logger
	Debug bool

	API api.Config

	Check CheckConfig

	Broker BrokerConfig
}

type CheckTypeType string

type CheckInstanceIDType string

type CheckSecretType string

type CheckTagsType string

type CheckDisplayNameType string

type BrokerCNType string

type CheckManager struct {
	enabled bool
	Log     *log.Logger
	Debug   bool
	apih    *api.API

	checkType             CheckTypeType
	checkID               api.IDType
	checkInstanceID       CheckInstanceIDType
	checkTarget           string
	checkSearchTag        api.TagType
	checkSecret           CheckSecretType
	checkTags             api.TagType
	checkSubmissionURL    api.URLType
	checkDisplayName      CheckDisplayNameType
	forceMetricActivation bool
	forceCheckUpdate      bool

	metricTags map[string][]string
	mtmu       sync.Mutex

	brokerID              api.IDType
	brokerSelectTag       api.TagType
	brokerMaxResponseTime time.Duration

	checkBundle      *api.CheckBundle
	cbmu             sync.Mutex
	availableMetrics map[string]bool
	trapURL          api.URLType
	trapCN           BrokerCNType
	trapLastUpdate   time.Time
	trapMaxURLAge    time.Duration
	trapmu           sync.Mutex
	certPool         *x509.CertPool
}

type Trap struct {
	URL *url.URL
	TLS *tls.Config
}

func NewCheckManager(cfg *Config) (*CheckManager, error) {

	if cfg == nil {
		return nil, errors.New("Invalid Check Manager configuration (nil).")
	}

	cm := &CheckManager{
		enabled: false,
	}

	cm.Debug = cfg.Debug
	cm.Log = cfg.Log
	if cm.Debug && cm.Log == nil {
		cm.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	if cm.Log == nil {
		cm.Log = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	if cfg.Check.SubmissionURL != "" {
		cm.checkSubmissionURL = api.URLType(cfg.Check.SubmissionURL)
	}

	if cfg.API.TokenKey == "" {
		if cm.checkSubmissionURL == "" {
			return nil, errors.New("Invalid check manager configuration (no API token AND no submission url).")
		}
		if err := cm.initializeTrapURL(); err != nil {
			return nil, err
		}
		return cm, nil
	}

	cm.enabled = true

	cfg.API.Debug = cm.Debug
	cfg.API.Log = cm.Log

	apih, err := api.NewAPI(&cfg.API)
	if err != nil {
		return nil, err
	}
	cm.apih = apih

	cm.checkType = defaultCheckType

	idSetting := "0"
	if cfg.Check.ID != "" {
		idSetting = cfg.Check.ID
	}
	id, err := strconv.Atoi(idSetting)
	if err != nil {
		return nil, err
	}
	cm.checkID = api.IDType(id)

	cm.checkInstanceID = CheckInstanceIDType(cfg.Check.InstanceID)
	cm.checkDisplayName = CheckDisplayNameType(cfg.Check.DisplayName)
	cm.checkSecret = CheckSecretType(cfg.Check.Secret)

	fma := defaultForceMetricActivation
	if cfg.Check.ForceMetricActivation != "" {
		fma = cfg.Check.ForceMetricActivation
	}
	fm, err := strconv.ParseBool(fma)
	if err != nil {
		return nil, err
	}
	cm.forceMetricActivation = fm

	_, an := path.Split(os.Args[0])
	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}
	if cm.checkInstanceID == "" {
		cm.checkInstanceID = CheckInstanceIDType(fmt.Sprintf("%s:%s", hn, an))
	}
	cm.checkTarget = hn

	if cfg.Check.SearchTag == "" {
		cm.checkSearchTag = []string{fmt.Sprintf("service:%s", an)}
	} else {
		cm.checkSearchTag = strings.Split(strings.Replace(cfg.Check.SearchTag, " ", "", -1), ",")
	}

	if cfg.Check.Tags != "" {
		cm.checkTags = strings.Split(strings.Replace(cfg.Check.Tags, " ", "", -1), ",")
	}

	if cm.checkDisplayName == "" {
		cm.checkDisplayName = CheckDisplayNameType(fmt.Sprintf("%s", string(cm.checkInstanceID)))
	}

	dur := cfg.Check.MaxURLAge
	if dur == "" {
		dur = defaultTrapMaxURLAge
	}
	maxDur, err := time.ParseDuration(dur)
	if err != nil {
		return nil, err
	}
	cm.trapMaxURLAge = maxDur

	idSetting = "0"
	if cfg.Broker.ID != "" {
		idSetting = cfg.Broker.ID
	}
	id, err = strconv.Atoi(idSetting)
	if err != nil {
		return nil, err
	}
	cm.brokerID = api.IDType(id)

	if cfg.Broker.SelectTag != "" {
		cm.brokerSelectTag = strings.Split(strings.Replace(cfg.Broker.SelectTag, " ", "", -1), ",")
	}

	dur = cfg.Broker.MaxResponseTime
	if dur == "" {
		dur = defaultBrokerMaxResponseTime
	}
	maxDur, err = time.ParseDuration(dur)
	if err != nil {
		return nil, err
	}
	cm.brokerMaxResponseTime = maxDur

	cm.availableMetrics = make(map[string]bool)
	cm.metricTags = make(map[string][]string)

	if err := cm.initializeTrapURL(); err != nil {
		return nil, err
	}

	return cm, nil
}

func (cm *CheckManager) GetTrap() (*Trap, error) {
	if cm.trapURL == "" {
		if err := cm.initializeTrapURL(); err != nil {
			return nil, err
		}
	}

	trap := &Trap{}

	u, err := url.Parse(string(cm.trapURL))
	if err != nil {
		return nil, err
	}

	trap.URL = u

	if u.Scheme == "https" {
		if cm.certPool == nil {
			cm.loadCACert()
		}
		t := &tls.Config{
			RootCAs: cm.certPool,
		}
		if cm.trapCN != "" {
			t.ServerName = string(cm.trapCN)
		}
		trap.TLS = t
	}

	return trap, nil
}

func (cm *CheckManager) ResetTrap() error {
	if cm.trapURL == "" {
		return nil
	}

	cm.trapURL = ""
	cm.certPool = nil
	err := cm.initializeTrapURL()
	return err
}

func (cm *CheckManager) RefreshTrap() {
	if cm.trapURL == "" {
		return
	}

	if time.Since(cm.trapLastUpdate) >= cm.trapMaxURLAge {
		cm.ResetTrap()
	}
}
