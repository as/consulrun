// Copyright 2016 Circonus, Inc. All rights reserved.

package checkmgr

import (
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/circonus-labs/circonus-gometrics/api"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (cm *CheckManager) getBroker() (*api.Broker, error) {
	if cm.brokerID != 0 {
		broker, err := cm.apih.FetchBrokerByID(cm.brokerID)
		if err != nil {
			return nil, err
		}
		if !cm.isValidBroker(broker) {
			return nil, fmt.Errorf(
				"[ERROR] designated broker %d [%s] is invalid (not active, does not support required check type, or connectivity issue)",
				cm.brokerID,
				broker.Name)
		}
		return broker, nil
	}
	broker, err := cm.selectBroker()
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Unable to fetch suitable broker %s", err)
	}
	return broker, nil
}

func (cm *CheckManager) getBrokerCN(broker *api.Broker, submissionURL api.URLType) (string, error) {
	u, err := url.Parse(string(submissionURL))
	if err != nil {
		return "", err
	}

	hostParts := strings.Split(u.Host, ":")
	host := hostParts[0]

	if net.ParseIP(host) == nil { // it's a non-ip string
		return u.Host, nil
	}

	cn := ""

	for _, detail := range broker.Details {
		if detail.IP == host {
			cn = detail.CN
			break
		}
	}

	if cn == "" {
		return "", fmt.Errorf("[ERROR] Unable to match URL host (%s) to Broker", u.Host)
	}

	return cn, nil

}

func (cm *CheckManager) selectBroker() (*api.Broker, error) {
	var brokerList []api.Broker
	var err error

	if len(cm.brokerSelectTag) > 0 {
		brokerList, err = cm.apih.FetchBrokerListByTag(cm.brokerSelectTag)
		if err != nil {
			return nil, err
		}
	} else {
		brokerList, err = cm.apih.FetchBrokerList()
		if err != nil {
			return nil, err
		}
	}

	if len(brokerList) == 0 {
		return nil, fmt.Errorf("zero brokers found")
	}

	validBrokers := make(map[string]api.Broker)
	haveEnterprise := false

	for _, broker := range brokerList {
		if cm.isValidBroker(&broker) {
			validBrokers[broker.Cid] = broker
			if broker.Type == "enterprise" {
				haveEnterprise = true
			}
		}
	}

	if haveEnterprise { // eliminate non-enterprise brokers from valid brokers
		for k, v := range validBrokers {
			if v.Type != "enterprise" {
				delete(validBrokers, k)
			}
		}
	}

	if len(validBrokers) == 0 {
		return nil, fmt.Errorf("found %d broker(s), zero are valid", len(brokerList))
	}

	validBrokerKeys := reflect.ValueOf(validBrokers).MapKeys()
	selectedBroker := validBrokers[validBrokerKeys[rand.Intn(len(validBrokerKeys))].String()]

	if cm.Debug {
		cm.Log.Printf("[DEBUG] Selected broker '%s'\n", selectedBroker.Name)
	}

	return &selectedBroker, nil

}

func (cm *CheckManager) brokerSupportsCheckType(checkType CheckTypeType, details *api.BrokerDetail) bool {

	for _, module := range details.Modules {
		if CheckTypeType(module) == checkType {
			return true
		}
	}

	return false

}

func (cm *CheckManager) isValidBroker(broker *api.Broker) bool {
	brokerHost := ""
	brokerPort := ""
	valid := false
	for _, detail := range broker.Details {

		if detail.Status != statusActive {
			if cm.Debug {
				cm.Log.Printf("[DEBUG] Broker '%s' is not active.\n", broker.Name)
			}
			continue
		}

		if !cm.brokerSupportsCheckType(cm.checkType, &detail) {
			if cm.Debug {
				cm.Log.Printf("[DEBUG] Broker '%s' does not support '%s' checks.\n", broker.Name, cm.checkType)
			}
			continue
		}

		if detail.ExternalPort != 0 {
			brokerPort = strconv.Itoa(detail.ExternalPort)
		} else {
			if detail.Port != 0 {
				brokerPort = strconv.Itoa(detail.Port)
			} else {
				brokerPort = "43191"
			}
		}

		if detail.ExternalHost != "" {
			brokerHost = detail.ExternalHost
		} else {
			brokerHost = detail.IP
		}

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", brokerHost, brokerPort), cm.brokerMaxResponseTime)
		if err != nil {
			if detail.CN != "trap.noit.circonus.net" {
				if cm.Debug {
					cm.Log.Printf("[DEBUG] Broker '%s' unable to connect, %v\n", broker.Name, err)
				}
				continue // not able to reach the broker (or respone slow enough for it to be considered not usable)
			}

			brokerPort = "443"
			conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%s", detail.CN, brokerPort), cm.brokerMaxResponseTime)
			if err != nil {
				if cm.Debug {
					cm.Log.Printf("[DEBUG] Broker '%s' unable to connect %v\n", broker.Name, err)
				}
				continue // not able to reach the broker on 443 either (or respone slow enough for it to be considered not usable)
			}
		}
		conn.Close()

		if cm.Debug {
			cm.Log.Printf("[DEBUG] Broker '%s' is valid\n", broker.Name)
		}

		valid = true
		break

	}
	return valid
}
