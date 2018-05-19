// Copyright 2016 Circonus, Inc. All rights reserved.

package circonusgometrics

//

import (
	"fmt"
)

func (m *CirconusMetrics) Gauge(metric string, val interface{}) {
	m.SetGauge(metric, val)
}

func (m *CirconusMetrics) SetGauge(metric string, val interface{}) {
	m.gm.Lock()
	defer m.gm.Unlock()
	m.gauges[metric] = m.gaugeValString(val)
}

func (m *CirconusMetrics) RemoveGauge(metric string) {
	m.gm.Lock()
	defer m.gm.Unlock()
	delete(m.gauges, metric)
}

func (m *CirconusMetrics) SetGaugeFunc(metric string, fn func() int64) {
	m.gfm.Lock()
	defer m.gfm.Unlock()
	m.gaugeFuncs[metric] = fn
}

func (m *CirconusMetrics) RemoveGaugeFunc(metric string) {
	m.gfm.Lock()
	defer m.gfm.Unlock()
	delete(m.gaugeFuncs, metric)
}

func (m *CirconusMetrics) gaugeValString(val interface{}) string {
	vs := ""
	switch v := val.(type) {
	default:

	case int:
		vs = fmt.Sprintf("%d", v)
	case int8:
		vs = fmt.Sprintf("%d", v)
	case int16:
		vs = fmt.Sprintf("%d", v)
	case int32:
		vs = fmt.Sprintf("%d", v)
	case int64:
		vs = fmt.Sprintf("%d", v)
	case uint:
		vs = fmt.Sprintf("%d", v)
	case uint8:
		vs = fmt.Sprintf("%d", v)
	case uint16:
		vs = fmt.Sprintf("%d", v)
	case uint32:
		vs = fmt.Sprintf("%d", v)
	case uint64:
		vs = fmt.Sprintf("%d", v)
	case float32:
		vs = fmt.Sprintf("%f", v)
	case float64:
		vs = fmt.Sprintf("%f", v)
	}
	return vs
}
