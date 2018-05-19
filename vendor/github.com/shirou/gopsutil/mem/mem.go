package mem

import (
	"encoding/json"

	"github.com/shirou/gopsutil/internal/common"
)

var invoke common.Invoker

func init() {
	invoke = common.Invoke{}
}

//
type VirtualMemoryStat struct {
	Total uint64 `json:"total"`

	//

	Available uint64 `json:"available"`

	//

	Used uint64 `json:"used"`

	//

	UsedPercent float64 `json:"usedPercent"`

	Free uint64 `json:"free"`

	Active   uint64 `json:"active"`
	Inactive uint64 `json:"inactive"`
	Wired    uint64 `json:"wired"`

	Buffers      uint64 `json:"buffers"`
	Cached       uint64 `json:"cached"`
	Writeback    uint64 `json:"writeback"`
	Dirty        uint64 `json:"dirty"`
	WritebackTmp uint64 `json:"writebacktmp"`
	Shared       uint64 `json:"shared"`
	Slab         uint64 `json:"slab"`
	PageTables   uint64 `json:"pagetables"`
	SwapCached   uint64 `json:"swapcached"`
}

type SwapMemoryStat struct {
	Total       uint64  `json:"total"`
	Used        uint64  `json:"used"`
	Free        uint64  `json:"free"`
	UsedPercent float64 `json:"usedPercent"`
	Sin         uint64  `json:"sin"`
	Sout        uint64  `json:"sout"`
}

func (m VirtualMemoryStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}

func (m SwapMemoryStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}
