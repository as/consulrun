package config

import (
	"strconv"
	"strings"
	"time"
)

type boolPtrValue struct {
	v **bool
	b bool
}

func newBoolPtrValue(p **bool) *boolPtrValue {
	return &boolPtrValue{p, false}
}

func (s *boolPtrValue) IsBoolFlag() bool { return true }

func (s *boolPtrValue) Set(val string) error {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return err
	}
	*s.v, s.b = &b, true
	return nil
}

func (s *boolPtrValue) Get() interface{} {
	if s.b {
		return *s.v
	}
	return (*bool)(nil)
}

func (s *boolPtrValue) String() string {
	if s.b {
		return strconv.FormatBool(**s.v)
	}
	return ""
}

type durationPtrValue struct {
	v **time.Duration
	b bool
}

func newDurationPtrValue(p **time.Duration) *durationPtrValue {
	return &durationPtrValue{p, false}
}

func (s *durationPtrValue) Set(val string) error {
	d, err := time.ParseDuration(val)
	if err != nil {
		return err
	}
	*s.v, s.b = &d, true
	return nil
}

func (s *durationPtrValue) Get() interface{} {
	if s.b {
		return *s.v
	}
	return (*time.Duration)(nil)
}

func (s *durationPtrValue) String() string {
	if s.b {
		return (*(*s).v).String()
	}
	return ""
}

type intPtrValue struct {
	v **int
	b bool
}

func newIntPtrValue(p **int) *intPtrValue {
	return &intPtrValue{p, false}
}

func (s *intPtrValue) Set(val string) error {
	n, err := strconv.Atoi(val)
	if err != nil {
		return err
	}
	*s.v, s.b = &n, true
	return nil
}

func (s *intPtrValue) Get() interface{} {
	if s.b {
		return *s.v
	}
	return (*int)(nil)
}

func (s *intPtrValue) String() string {
	if s.b {
		return strconv.Itoa(**s.v)
	}
	return ""
}

type stringMapValue map[string]string

func newStringMapValue(p *map[string]string) *stringMapValue {
	*p = map[string]string{}
	return (*stringMapValue)(p)
}

func (s *stringMapValue) Set(val string) error {
	p := strings.SplitN(val, ":", 2)
	k, v := p[0], ""
	if len(p) == 2 {
		v = p[1]
	}
	(*s)[k] = v
	return nil
}

func (s *stringMapValue) Get() interface{} {
	return s
}

func (s *stringMapValue) String() string {
	var x []string
	for k, v := range *s {
		if v == "" {
			x = append(x, k)
		} else {
			x = append(x, k+":"+v)
		}
	}
	return strings.Join(x, " ")
}

type stringPtrValue struct {
	v **string
	b bool
}

func newStringPtrValue(p **string) *stringPtrValue {
	return &stringPtrValue{p, false}
}

func (s *stringPtrValue) Set(val string) error {
	*s.v, s.b = &val, true
	return nil
}

func (s *stringPtrValue) Get() interface{} {
	if s.b {
		return *s.v
	}
	return (*string)(nil)
}

func (s *stringPtrValue) String() string {
	if s.b {
		return **s.v
	}
	return ""
}

type stringSliceValue []string

func newStringSliceValue(p *[]string) *stringSliceValue {
	return (*stringSliceValue)(p)
}

func (s *stringSliceValue) Set(val string) error {
	*s = append(*s, val)
	return nil
}

func (s *stringSliceValue) Get() interface{} {
	return s
}

func (s *stringSliceValue) String() string {
	return strings.Join(*s, " ")
}
