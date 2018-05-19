// +build freebsd

package net

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
)

func IOCounters(pernic bool) ([]IOCountersStat, error) {
	netstat, err := exec.LookPath("/usr/bin/netstat")
	if err != nil {
		return nil, err
	}
	out, err := invoke.Command(netstat, "-ibdnW")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(out), "\n")
	ret := make([]IOCountersStat, 0, len(lines)-1)
	exists := make([]string, 0, len(ret))

	for _, line := range lines {
		values := strings.Fields(line)
		if len(values) < 1 || values[0] == "Name" {
			continue
		}
		if common.StringsHas(exists, values[0]) {

			continue
		}
		exists = append(exists, values[0])

		if len(values) < 12 {
			continue
		}
		base := 1

		if len(values) < 13 {
			base = 0
		}

		parsed := make([]uint64, 0, 8)
		vv := []string{
			values[base+3],  // PacketsRecv
			values[base+4],  // Errin
			values[base+5],  // Dropin
			values[base+6],  // BytesRecvn
			values[base+7],  // PacketSent
			values[base+8],  // Errout
			values[base+9],  // BytesSent
			values[base+11], // Dropout
		}
		for _, target := range vv {
			if target == "-" {
				parsed = append(parsed, 0)
				continue
			}

			t, err := strconv.ParseUint(target, 10, 64)
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, t)
		}

		n := IOCountersStat{
			Name:        values[0],
			PacketsRecv: parsed[0],
			Errin:       parsed[1],
			Dropin:      parsed[2],
			BytesRecv:   parsed[3],
			PacketsSent: parsed[4],
			Errout:      parsed[5],
			BytesSent:   parsed[6],
			Dropout:     parsed[7],
		}
		ret = append(ret, n)
	}

	if pernic == false {
		return getIOCountersAll(ret)
	}

	return ret, nil
}

func IOCountersByFile(pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCounters(pernic)
}

func FilterCounters() ([]FilterStat, error) {
	return nil, errors.New("NetFilterCounters not implemented for freebsd")
}

func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for freebsd")
}
