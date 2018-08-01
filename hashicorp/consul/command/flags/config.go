package flags

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

var ConfigDecodeHook = mapstructure.ComposeDecodeHookFunc(
	BoolToBoolValueFunc(),
	StringToDurationValueFunc(),
	StringToStringValueFunc(),
	Float64ToUintValueFunc(),
)

type BoolValue struct {
	v *bool
}

func (b *BoolValue) IsBoolFlag() bool {
	return true
}

func (b *BoolValue) Merge(onto *bool) {
	if b.v != nil {
		*onto = *(b.v)
	}
}

func (b *BoolValue) Set(v string) error {
	if b.v == nil {
		b.v = new(bool)
	}
	var err error
	*(b.v), err = strconv.ParseBool(v)
	return err
}

func (b *BoolValue) String() string {
	var current bool
	if b.v != nil {
		current = *(b.v)
	}
	return fmt.Sprintf("%v", current)
}

func BoolToBoolValueFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.Bool {
			return data, nil
		}

		val := BoolValue{}
		if t != reflect.TypeOf(val) {
			return data, nil
		}

		val.v = new(bool)
		*(val.v) = data.(bool)
		return val, nil
	}
}

type DurationValue struct {
	v *time.Duration
}

func (d *DurationValue) Merge(onto *time.Duration) {
	if d.v != nil {
		*onto = *(d.v)
	}
}

func (d *DurationValue) Set(v string) error {
	if d.v == nil {
		d.v = new(time.Duration)
	}
	var err error
	*(d.v), err = time.ParseDuration(v)
	return err
}

func (d *DurationValue) String() string {
	var current time.Duration
	if d.v != nil {
		current = *(d.v)
	}
	return current.String()
}

func StringToDurationValueFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		val := DurationValue{}
		if t != reflect.TypeOf(val) {
			return data, nil
		}
		if err := val.Set(data.(string)); err != nil {
			return nil, err
		}
		return val, nil
	}
}

type StringValue struct {
	v *string
}

func (s *StringValue) Merge(onto *string) {
	if s.v != nil {
		*onto = *(s.v)
	}
}

func (s *StringValue) Set(v string) error {
	if s.v == nil {
		s.v = new(string)
	}
	*(s.v) = v
	return nil
}

func (s *StringValue) String() string {
	var current string
	if s.v != nil {
		current = *(s.v)
	}
	return current
}

func StringToStringValueFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		val := StringValue{}
		if t != reflect.TypeOf(val) {
			return data, nil
		}
		val.v = new(string)
		*(val.v) = data.(string)
		return val, nil
	}
}

type UintValue struct {
	v *uint
}

func (u *UintValue) Merge(onto *uint) {
	if u.v != nil {
		*onto = *(u.v)
	}
}

func (u *UintValue) Set(v string) error {
	if u.v == nil {
		u.v = new(uint)
	}
	parsed, err := strconv.ParseUint(v, 0, 64)
	*(u.v) = (uint)(parsed)
	return err
}

func (u *UintValue) String() string {
	var current uint
	if u.v != nil {
		current = *(u.v)
	}
	return fmt.Sprintf("%v", current)
}

func Float64ToUintValueFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.Float64 {
			return data, nil
		}

		val := UintValue{}
		if t != reflect.TypeOf(val) {
			return data, nil
		}

		fv := data.(float64)
		if fv < 0 {
			return nil, fmt.Errorf("value cannot be negative")
		}

		if fv > (1<<32 - 1) {
			return nil, fmt.Errorf("value is too large")
		}

		val.v = new(uint)
		*(val.v) = (uint)(fv)
		return val, nil
	}
}

type VisitFn func(path string) error

func Visit(path string, visitor VisitFn) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error reading %q: %v", path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("error checking %q: %v", path, err)
	}

	if !fi.IsDir() {
		if err := visitor(path); err != nil {
			return fmt.Errorf("error in %q: %v", path, err)
		}
		return nil
	}

	contents, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("error listing %q: %v", path, err)
	}

	sort.Sort(dirEnts(contents))
	for _, fi := range contents {
		if fi.IsDir() {
			continue
		}

		fullPath := filepath.Join(path, fi.Name())
		if err := visitor(fullPath); err != nil {
			return fmt.Errorf("error in %q: %v", fullPath, err)
		}
	}

	return nil
}

type dirEnts []os.FileInfo

func (d dirEnts) Len() int           { return len(d) }
func (d dirEnts) Less(i, j int) bool { return d[i].Name() < d[j].Name() }
func (d dirEnts) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
