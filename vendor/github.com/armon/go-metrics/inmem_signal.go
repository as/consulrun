package metrics

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type InmemSignal struct {
	signal syscall.Signal
	inm    *InmemSink
	w      io.Writer
	sigCh  chan os.Signal

	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func NewInmemSignal(inmem *InmemSink, sig syscall.Signal, w io.Writer) *InmemSignal {
	i := &InmemSignal{
		signal: sig,
		inm:    inmem,
		w:      w,
		sigCh:  make(chan os.Signal, 1),
		stopCh: make(chan struct{}),
	}
	signal.Notify(i.sigCh, sig)
	go i.run()
	return i
}

func DefaultInmemSignal(inmem *InmemSink) *InmemSignal {
	return NewInmemSignal(inmem, DefaultSignal, os.Stderr)
}

func (i *InmemSignal) Stop() {
	i.stopLock.Lock()
	defer i.stopLock.Unlock()

	if i.stop {
		return
	}
	i.stop = true
	close(i.stopCh)
	signal.Stop(i.sigCh)
}

func (i *InmemSignal) run() {
	for {
		select {
		case <-i.sigCh:
			i.dumpStats()
		case <-i.stopCh:
			return
		}
	}
}

func (i *InmemSignal) dumpStats() {
	buf := bytes.NewBuffer(nil)

	data := i.inm.Data()

	for j := 0; j < len(data)-1; j++ {
		intv := data[j]
		intv.RLock()
		for _, val := range intv.Gauges {
			name := i.flattenLabels(val.Name, val.Labels)
			fmt.Fprintf(buf, "[%v][G] '%s': %0.3f\n", intv.Interval, name, val.Value)
		}
		for name, vals := range intv.Points {
			for _, val := range vals {
				fmt.Fprintf(buf, "[%v][P] '%s': %0.3f\n", intv.Interval, name, val)
			}
		}
		for _, agg := range intv.Counters {
			name := i.flattenLabels(agg.Name, agg.Labels)
			fmt.Fprintf(buf, "[%v][C] '%s': %s\n", intv.Interval, name, agg.AggregateSample)
		}
		for _, agg := range intv.Samples {
			name := i.flattenLabels(agg.Name, agg.Labels)
			fmt.Fprintf(buf, "[%v][S] '%s': %s\n", intv.Interval, name, agg.AggregateSample)
		}
		intv.RUnlock()
	}

	i.w.Write(buf.Bytes())
}

func (i *InmemSignal) flattenLabels(name string, labels []Label) string {
	buf := bytes.NewBufferString(name)
	replacer := strings.NewReplacer(" ", "_", ":", "_")

	for _, label := range labels {
		replacer.WriteString(buf, ".")
		replacer.WriteString(buf, label.Value)
	}

	return buf.String()
}
