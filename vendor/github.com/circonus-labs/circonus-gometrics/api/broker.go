// Copyright 2016 Circonus, Inc. All rights reserved.

package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type BrokerDetail struct {
	CN           string   `json:"cn"`
	ExternalHost string   `json:"external_host"`
	ExternalPort int      `json:"external_port"`
	IP           string   `json:"ipaddress"`
	MinVer       int      `json:"minimum_version_required"`
	Modules      []string `json:"modules"`
	Port         int      `json:"port"`
	Skew         string   `json:"skew"`
	Status       string   `json:"status"`
	Version      int      `json:"version"`
}

type Broker struct {
	Cid       string         `json:"_cid"`
	Details   []BrokerDetail `json:"_details"`
	Latitude  string         `json:"_latitude"`
	Longitude string         `json:"_longitude"`
	Name      string         `json:"_name"`
	Tags      []string       `json:"_tags"`
	Type      string         `json:"_type"`
}

func (a *API) FetchBrokerByID(id IDType) (*Broker, error) {
	cid := CIDType(fmt.Sprintf("/broker/%d", id))
	return a.FetchBrokerByCID(cid)
}

func (a *API) FetchBrokerByCID(cid CIDType) (*Broker, error) {
	result, err := a.Get(string(cid))
	if err != nil {
		return nil, err
	}

	response := new(Broker)
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, err
	}

	return response, nil

}

func (a *API) FetchBrokerListByTag(searchTag TagType) ([]Broker, error) {
	query := SearchQueryType(fmt.Sprintf("f__tags_has=%s", strings.Replace(strings.Join(searchTag, ","), ",", "&f__tags_has=", -1)))
	return a.BrokerSearch(query)
}

func (a *API) BrokerSearch(query SearchQueryType) ([]Broker, error) {
	queryURL := fmt.Sprintf("/broker?%s", string(query))

	result, err := a.Get(queryURL)
	if err != nil {
		return nil, err
	}

	var brokers []Broker
	if err := json.Unmarshal(result, &brokers); err != nil {
		return nil, err
	}

	return brokers, nil
}

func (a *API) FetchBrokerList() ([]Broker, error) {
	result, err := a.Get("/broker")
	if err != nil {
		return nil, err
	}

	var response []Broker
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, err
	}

	return response, nil
}
