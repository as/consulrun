// Copyright 2015 The Prometheus Authors
//
//

package prometheus

import "github.com/prometheus/procfs"

type processCollector struct {
	pid             int
	collectFn       func(chan<- Metric)
	pidFn           func() (int, error)
	cpuTotal        *Desc
	openFDs, maxFDs *Desc
	vsize, rss      *Desc
	startTime       *Desc
}

//
func NewProcessCollector(pid int, namespace string) Collector {
	return NewProcessCollectorPIDFn(
		func() (int, error) { return pid, nil },
		namespace,
	)
}

func NewProcessCollectorPIDFn(
	pidFn func() (int, error),
	namespace string,
) Collector {
	ns := ""
	if len(namespace) > 0 {
		ns = namespace + "_"
	}

	c := processCollector{
		pidFn:     pidFn,
		collectFn: func(chan<- Metric) {},

		cpuTotal: NewDesc(
			ns+"process_cpu_seconds_total",
			"Total user and system CPU time spent in seconds.",
			nil, nil,
		),
		openFDs: NewDesc(
			ns+"process_open_fds",
			"Number of open file descriptors.",
			nil, nil,
		),
		maxFDs: NewDesc(
			ns+"process_max_fds",
			"Maximum number of open file descriptors.",
			nil, nil,
		),
		vsize: NewDesc(
			ns+"process_virtual_memory_bytes",
			"Virtual memory size in bytes.",
			nil, nil,
		),
		rss: NewDesc(
			ns+"process_resident_memory_bytes",
			"Resident memory size in bytes.",
			nil, nil,
		),
		startTime: NewDesc(
			ns+"process_start_time_seconds",
			"Start time of the process since unix epoch in seconds.",
			nil, nil,
		),
	}

	if _, err := procfs.NewStat(); err == nil {
		c.collectFn = c.processCollect
	}

	return &c
}

func (c *processCollector) Describe(ch chan<- *Desc) {
	ch <- c.cpuTotal
	ch <- c.openFDs
	ch <- c.maxFDs
	ch <- c.vsize
	ch <- c.rss
	ch <- c.startTime
}

func (c *processCollector) Collect(ch chan<- Metric) {
	c.collectFn(ch)
}

func (c *processCollector) processCollect(ch chan<- Metric) {
	pid, err := c.pidFn()
	if err != nil {
		return
	}

	p, err := procfs.NewProc(pid)
	if err != nil {
		return
	}

	if stat, err := p.NewStat(); err == nil {
		ch <- MustNewConstMetric(c.cpuTotal, CounterValue, stat.CPUTime())
		ch <- MustNewConstMetric(c.vsize, GaugeValue, float64(stat.VirtualMemory()))
		ch <- MustNewConstMetric(c.rss, GaugeValue, float64(stat.ResidentMemory()))
		if startTime, err := stat.StartTime(); err == nil {
			ch <- MustNewConstMetric(c.startTime, GaugeValue, startTime)
		}
	}

	if fds, err := p.FileDescriptorsLen(); err == nil {
		ch <- MustNewConstMetric(c.openFDs, GaugeValue, float64(fds))
	}

	if limits, err := p.NewLimits(); err == nil {
		ch <- MustNewConstMetric(c.maxFDs, GaugeValue, float64(limits.OpenFiles))
	}
}
