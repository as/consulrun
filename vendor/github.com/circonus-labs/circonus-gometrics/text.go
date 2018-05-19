// Copyright 2016 Circonus, Inc. All rights reserved.

package circonusgometrics

//

func (m *CirconusMetrics) SetText(metric string, val string) {
	m.SetTextValue(metric, val)
}

func (m *CirconusMetrics) SetTextValue(metric string, val string) {
	m.tm.Lock()
	defer m.tm.Unlock()
	m.text[metric] = val
}

func (m *CirconusMetrics) RemoveText(metric string) {
	m.tm.Lock()
	defer m.tm.Unlock()
	delete(m.text, metric)
}

func (m *CirconusMetrics) SetTextFunc(metric string, fn func() string) {
	m.tfm.Lock()
	defer m.tfm.Unlock()
	m.textFuncs[metric] = fn
}

func (m *CirconusMetrics) RemoveTextFunc(metric string) {
	m.tfm.Lock()
	defer m.tfm.Unlock()
	delete(m.textFuncs, metric)
}
