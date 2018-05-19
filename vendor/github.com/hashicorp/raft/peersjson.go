package raft

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
)

func ReadPeersJSON(path string) (Configuration, error) {

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return Configuration{}, err
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return Configuration{}, err
	}

	var configuration Configuration
	for _, peer := range peers {
		server := Server{
			Suffrage: Voter,
			ID:       ServerID(peer),
			Address:  ServerAddress(peer),
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	if err := checkConfiguration(configuration); err != nil {
		return Configuration{}, err
	}
	return configuration, nil
}

type configEntry struct {
	ID ServerID `json:"id"`

	Address ServerAddress `json:"address"`

	NonVoter bool `json:"non_voter"`
}

func ReadConfigJSON(path string) (Configuration, error) {

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return Configuration{}, err
	}

	var peers []configEntry
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return Configuration{}, err
	}

	var configuration Configuration
	for _, peer := range peers {
		suffrage := Voter
		if peer.NonVoter {
			suffrage = Nonvoter
		}
		server := Server{
			Suffrage: suffrage,
			ID:       peer.ID,
			Address:  peer.Address,
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	if err := checkConfiguration(configuration); err != nil {
		return Configuration{}, err
	}
	return configuration, nil
}
