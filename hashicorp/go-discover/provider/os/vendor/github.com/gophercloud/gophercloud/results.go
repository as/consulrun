package gophercloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

/*
Result is an internal type to be used by individual resource packages, but its
methods will be available on a wide variety of user-facing embedding types.

It acts as a base struct that other Result types, returned from request
functions, can embed for convenience. All Results capture basic information
from the HTTP transaction that was performed, including the response body,
HTTP headers, and any errors that happened.

Generally, each Result type will have an Extract method that can be used to
further interpret the result's payload in a specific context. Extensions or
providers can then provide additional extraction functions to pull out
provider- or extension-specific information as well.
*/
type Result struct {
	Body interface{}

	Header http.Header

	Err error
}

func (r Result) ExtractInto(to interface{}) error {
	if r.Err != nil {
		return r.Err
	}

	if reader, ok := r.Body.(io.Reader); ok {
		if readCloser, ok := reader.(io.Closer); ok {
			defer readCloser.Close()
		}
		return json.NewDecoder(reader).Decode(to)
	}

	b, err := json.Marshal(r.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, to)

	return err
}

func (r Result) extractIntoPtr(to interface{}, label string) error {
	if label == "" {
		return r.ExtractInto(&to)
	}

	var m map[string]interface{}
	err := r.ExtractInto(&m)
	if err != nil {
		return err
	}

	b, err := json.Marshal(m[label])
	if err != nil {
		return err
	}

	toValue := reflect.ValueOf(to)
	if toValue.Kind() == reflect.Ptr {
		toValue = toValue.Elem()
	}

	switch toValue.Kind() {
	case reflect.Slice:
		typeOfV := toValue.Type().Elem()
		if typeOfV.Kind() == reflect.Struct {
			if typeOfV.NumField() > 0 && typeOfV.Field(0).Anonymous {
				newSlice := reflect.MakeSlice(reflect.SliceOf(typeOfV), 0, 0)
				newType := reflect.New(typeOfV).Elem()

				for _, v := range m[label].([]interface{}) {
					b, err := json.Marshal(v)
					if err != nil {
						return err
					}

					for i := 0; i < newType.NumField(); i++ {
						s := newType.Field(i).Addr().Interface()
						err = json.NewDecoder(bytes.NewReader(b)).Decode(s)
						if err != nil {
							return err
						}
					}
					newSlice = reflect.Append(newSlice, newType)
				}
				toValue.Set(newSlice)
			}
		}
	case reflect.Struct:
		typeOfV := toValue.Type()
		if typeOfV.NumField() > 0 && typeOfV.Field(0).Anonymous {
			for i := 0; i < toValue.NumField(); i++ {
				toField := toValue.Field(i)
				if toField.Kind() == reflect.Struct {
					s := toField.Addr().Interface()
					err = json.NewDecoder(bytes.NewReader(b)).Decode(s)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	err = json.Unmarshal(b, &to)
	return err
}

//
//
//
func (r Result) ExtractIntoStructPtr(to interface{}, label string) error {
	if r.Err != nil {
		return r.Err
	}

	t := reflect.TypeOf(to)
	if k := t.Kind(); k != reflect.Ptr {
		return fmt.Errorf("Expected pointer, got %v", k)
	}
	switch t.Elem().Kind() {
	case reflect.Struct:
		return r.extractIntoPtr(to, label)
	default:
		return fmt.Errorf("Expected pointer to struct, got: %v", t)
	}
}

//
//
//
func (r Result) ExtractIntoSlicePtr(to interface{}, label string) error {
	if r.Err != nil {
		return r.Err
	}

	t := reflect.TypeOf(to)
	if k := t.Kind(); k != reflect.Ptr {
		return fmt.Errorf("Expected pointer, got %v", k)
	}
	switch t.Elem().Kind() {
	case reflect.Slice:
		return r.extractIntoPtr(to, label)
	default:
		return fmt.Errorf("Expected pointer to slice, got: %v", t)
	}
}

func (r Result) PrettyPrintJSON() string {
	pretty, err := json.MarshalIndent(r.Body, "", "  ")
	if err != nil {
		panic(err.Error())
	}
	return string(pretty)
}

//
type ErrResult struct {
	Result
}

func (r ErrResult) ExtractErr() error {
	return r.Err
}

/*
HeaderResult is an internal type to be used by individual resource packages, but
its methods will be available on a wide variety of user-facing embedding types.

It represents a result that only contains an error (possibly nil) and an
http.Header. This is used, for example, by the objectstorage packages in
openstack, because most of the operations don't return response bodies, but do
have relevant information in headers.
*/
type HeaderResult struct {
	Result
}

func (r HeaderResult) ExtractInto(to interface{}) error {
	if r.Err != nil {
		return r.Err
	}

	tmpHeaderMap := map[string]string{}
	for k, v := range r.Header {
		if len(v) > 0 {
			tmpHeaderMap[k] = v[0]
		}
	}

	b, err := json.Marshal(tmpHeaderMap)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, to)

	return err
}

const RFC3339Milli = "2006-01-02T15:04:05.999999Z"

type JSONRFC3339Milli time.Time

func (jt *JSONRFC3339Milli) UnmarshalJSON(data []byte) error {
	b := bytes.NewBuffer(data)
	dec := json.NewDecoder(b)
	var s string
	if err := dec.Decode(&s); err != nil {
		return err
	}
	t, err := time.Parse(RFC3339Milli, s)
	if err != nil {
		return err
	}
	*jt = JSONRFC3339Milli(t)
	return nil
}

const RFC3339MilliNoZ = "2006-01-02T15:04:05.999999"

type JSONRFC3339MilliNoZ time.Time

func (jt *JSONRFC3339MilliNoZ) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	t, err := time.Parse(RFC3339MilliNoZ, s)
	if err != nil {
		return err
	}
	*jt = JSONRFC3339MilliNoZ(t)
	return nil
}

type JSONRFC1123 time.Time

func (jt *JSONRFC1123) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC1123, s)
	if err != nil {
		return err
	}
	*jt = JSONRFC1123(t)
	return nil
}

type JSONUnix time.Time

func (jt *JSONUnix) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	unix, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	t = time.Unix(unix, 0)
	*jt = JSONUnix(t)
	return nil
}

const RFC3339NoZ = "2006-01-02T15:04:05"

type JSONRFC3339NoZ time.Time

func (jt *JSONRFC3339NoZ) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	t, err := time.Parse(RFC3339NoZ, s)
	if err != nil {
		return err
	}
	*jt = JSONRFC3339NoZ(t)
	return nil
}

/*
Link is an internal type to be used in packages of collection resources that are
paginated in a certain way.

It's a response substructure common to many paginated collection results that is
used to point to related pages. Usually, the one we care about is the one with
Rel field set to "next".
*/
type Link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
}

/*
ExtractNextURL is an internal function useful for packages of collection
resources that are paginated in a certain way.

It attempts to extract the "next" URL from slice of Link structs, or
"" if no such URL is present.
*/
func ExtractNextURL(links []Link) (string, error) {
	var url string

	for _, l := range links {
		if l.Rel == "next" {
			url = l.Href
		}
	}

	if url == "" {
		return "", nil
	}

	return url, nil
}
