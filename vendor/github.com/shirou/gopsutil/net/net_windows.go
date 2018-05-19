// +build windows

package net

import (
	"errors"
	"net"
	"os"
	"syscall"

	"github.com/shirou/gopsutil/internal/common"
)

var (
	modiphlpapi             = syscall.NewLazyDLL("iphlpapi.dll")
	procGetExtendedTCPTable = modiphlpapi.NewProc("GetExtendedTcpTable")
	procGetExtendedUDPTable = modiphlpapi.NewProc("GetExtendedUdpTable")
)

const (
	TCPTableBasicListener = iota
	TCPTableBasicConnections
	TCPTableBasicAll
	TCPTableOwnerPIDListener
	TCPTableOwnerPIDConnections
	TCPTableOwnerPIDAll
	TCPTableOwnerModuleListener
	TCPTableOwnerModuleConnections
	TCPTableOwnerModuleAll
)

func IOCounters(pernic bool) ([]IOCountersStat, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ret []IOCountersStat

	for _, ifi := range ifs {
		c := IOCountersStat{
			Name: ifi.Name,
		}

		row := syscall.MibIfRow{Index: uint32(ifi.Index)}
		e := syscall.GetIfEntry(&row)
		if e != nil {
			return nil, os.NewSyscallError("GetIfEntry", e)
		}
		c.BytesSent = uint64(row.OutOctets)
		c.BytesRecv = uint64(row.InOctets)
		c.PacketsSent = uint64(row.OutUcastPkts)
		c.PacketsRecv = uint64(row.InUcastPkts)
		c.Errin = uint64(row.InErrors)
		c.Errout = uint64(row.OutErrors)
		c.Dropin = uint64(row.InDiscards)
		c.Dropout = uint64(row.OutDiscards)

		ret = append(ret, c)
	}

	if pernic == false {
		return getIOCountersAll(ret)
	}
	return ret, nil
}

func IOCountersByFile(pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCounters(pernic)
}

func Connections(kind string) ([]ConnectionStat, error) {
	var ret []ConnectionStat

	return ret, common.ErrNotImplementedError
}

func ConnectionsMax(kind string, max int) ([]ConnectionStat, error) {
	return []ConnectionStat{}, common.ErrNotImplementedError
}

func FilterCounters() ([]FilterStat, error) {
	return nil, errors.New("NetFilterCounters not implemented for windows")
}

func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for windows")
}
