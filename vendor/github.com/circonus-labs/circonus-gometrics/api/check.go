// Copyright 2016 Circonus, Inc. All rights reserved.

package api

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type CheckDetails struct {
	SubmissionURL string `json:"submission_url"`
}

type Check struct {
	Cid            string       `json:"_cid"`
	Active         bool         `json:"_active"`
	BrokerCid      string       `json:"_broker"`
	CheckBundleCid string       `json:"_check_bundle"`
	CheckUUID      string       `json:"_check_uuid"`
	Details        CheckDetails `json:"_details"`
}

func (a *API) FetchCheckByID(id IDType) (*Check, error) {
	cid := CIDType(fmt.Sprintf("/check/%d", int(id)))
	return a.FetchCheckByCID(cid)
}

func (a *API) FetchCheckByCID(cid CIDType) (*Check, error) {
	result, err := a.Get(string(cid))
	if err != nil {
		return nil, err
	}

	check := new(Check)
	if err := json.Unmarshal(result, check); err != nil {
		return nil, err
	}

	return check, nil
}

func (a *API) FetchCheckBySubmissionURL(submissionURL URLType) (*Check, error) {

	u, err := url.Parse(string(submissionURL))
	if err != nil {
		return nil, err
	}

	if !strings.Contains(u.Path, "/module/httptrap/") {
		return nil, fmt.Errorf("[ERROR] Invalid submission URL '%s', unrecognized path", submissionURL)
	}

	pathParts := strings.Split(strings.Replace(u.Path, "/module/httptrap/", "", 1), "/")
	if len(pathParts) != 2 {
		return nil, fmt.Errorf("[ERROR] Invalid submission URL '%s', UUID not where expected", submissionURL)
	}
	uuid := pathParts[0]

	filter := SearchFilterType(fmt.Sprintf("f__check_uuid=%s", uuid))

	checks, err := a.CheckFilterSearch(filter)
	if err != nil {
		return nil, err
	}

	if len(checks) == 0 {
		return nil, fmt.Errorf("[ERROR] No checks found with UUID %s", uuid)
	}

	numActive := 0
	checkID := -1

	for idx, check := range checks {
		if check.Active {
			numActive++
			checkID = idx
		}
	}

	if numActive > 1 {
		return nil, fmt.Errorf("[ERROR] Multiple checks with same UUID %s", uuid)
	}

	return &checks[checkID], nil

}

func (a *API) CheckSearch(query SearchQueryType) ([]Check, error) {
	queryURL := fmt.Sprintf("/check?search=%s", string(query))

	result, err := a.Get(queryURL)
	if err != nil {
		return nil, err
	}

	var checks []Check
	if err := json.Unmarshal(result, &checks); err != nil {
		return nil, err
	}

	return checks, nil
}

func (a *API) CheckFilterSearch(filter SearchFilterType) ([]Check, error) {
	filterURL := fmt.Sprintf("/check?%s", string(filter))

	result, err := a.Get(filterURL)
	if err != nil {
		return nil, err
	}

	var checks []Check
	if err := json.Unmarshal(result, &checks); err != nil {
		return nil, err
	}

	return checks, nil
}
