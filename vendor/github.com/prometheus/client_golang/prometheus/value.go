// Copyright 2014 The Prometheus Authors
//
//

package prometheus

import (
	"fmt"
	"sort"

	dto "github.com/prometheus/client_model/go"

	"github.com/golang/protobuf/proto"
)

type ValueType int

const (
	_ ValueType = iota
	CounterValue
	GaugeValue
	UntypedValue
)

// determined by ValueType. This is a low-level building block used by the
type valueFunc struct {
	selfCollector

	desc       *Desc
	valType    ValueType
	function   func() float64
	labelPairs []*dto.LabelPair
}

func newValueFunc(desc *Desc, valueType ValueType, function func() float64) *valueFunc {
	result := &valueFunc{
		desc:       desc,
		valType:    valueType,
		function:   function,
		labelPairs: makeLabelPairs(desc, nil),
	}
	result.init(result)
	return result
}

func (v *valueFunc) Desc() *Desc {
	return v.desc
}

func (v *valueFunc) Write(out *dto.Metric) error {
	return populateMetric(v.valType, v.function(), v.labelPairs, out)
}

func NewConstMetric(desc *Desc, valueType ValueType, value float64, labelValues ...string) (Metric, error) {
	if err := validateLabelValues(labelValues, len(desc.variableLabels)); err != nil {
		return nil, err
	}
	return &constMetric{
		desc:       desc,
		valType:    valueType,
		val:        value,
		labelPairs: makeLabelPairs(desc, labelValues),
	}, nil
}

func MustNewConstMetric(desc *Desc, valueType ValueType, value float64, labelValues ...string) Metric {
	m, err := NewConstMetric(desc, valueType, value, labelValues...)
	if err != nil {
		panic(err)
	}
	return m
}

type constMetric struct {
	desc       *Desc
	valType    ValueType
	val        float64
	labelPairs []*dto.LabelPair
}

func (m *constMetric) Desc() *Desc {
	return m.desc
}

func (m *constMetric) Write(out *dto.Metric) error {
	return populateMetric(m.valType, m.val, m.labelPairs, out)
}

func populateMetric(
	t ValueType,
	v float64,
	labelPairs []*dto.LabelPair,
	m *dto.Metric,
) error {
	m.Label = labelPairs
	switch t {
	case CounterValue:
		m.Counter = &dto.Counter{Value: proto.Float64(v)}
	case GaugeValue:
		m.Gauge = &dto.Gauge{Value: proto.Float64(v)}
	case UntypedValue:
		m.Untyped = &dto.Untyped{Value: proto.Float64(v)}
	default:
		return fmt.Errorf("encountered unknown type %v", t)
	}
	return nil
}

func makeLabelPairs(desc *Desc, labelValues []string) []*dto.LabelPair {
	totalLen := len(desc.variableLabels) + len(desc.constLabelPairs)
	if totalLen == 0 {

		return nil
	}
	if len(desc.variableLabels) == 0 {

		return desc.constLabelPairs
	}
	labelPairs := make([]*dto.LabelPair, 0, totalLen)
	for i, n := range desc.variableLabels {
		labelPairs = append(labelPairs, &dto.LabelPair{
			Name:  proto.String(n),
			Value: proto.String(labelValues[i]),
		})
	}
	for _, lp := range desc.constLabelPairs {
		labelPairs = append(labelPairs, lp)
	}
	sort.Sort(LabelPairSorter(labelPairs))
	return labelPairs
}
