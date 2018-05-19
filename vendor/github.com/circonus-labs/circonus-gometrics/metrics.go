// Copyright 2016 Circonus, Inc. All rights reserved.

package circonusgometrics

func (m *CirconusMetrics) SetMetricTags(name string, tags []string) bool {
	return m.check.AddMetricTags(name, tags, false)
}

func (m *CirconusMetrics) AddMetricTags(name string, tags []string) bool {
	return m.check.AddMetricTags(name, tags, true)
}
