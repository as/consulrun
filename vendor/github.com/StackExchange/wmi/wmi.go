// +build windows

/*
Package wmi provides a WQL interface for WMI on Windows.

Example code to print names of running processes:

	type Win32_Process struct {
		Name string
	}

	func main() {
		var dst []Win32_Process
		q := wmi.CreateQuery(&dst, "")
		err := wmi.Query(q, &dst)
		if err != nil {
			log.Fatal(err)
		}
		for i, v := range dst {
			println(i, v.Name)
		}
	}

*/
package wmi

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
)

var l = log.New(os.Stdout, "", log.LstdFlags)

var (
	ErrInvalidEntityType = errors.New("wmi: invalid entity type")

	ErrNilCreateObject = errors.New("wmi: create object returned nil")
	lock               sync.Mutex
)

const S_FALSE = 0x00000001

func QueryNamespace(query string, dst interface{}, namespace string) error {
	return Query(query, dst, nil, namespace)
}

//
//
//
func Query(query string, dst interface{}, connectServerArgs ...interface{}) error {
	return DefaultClient.Query(query, dst, connectServerArgs...)
}

//
type Client struct {

	//

	NonePtrZero bool

	//

	PtrNil bool

	//

	AllowMissingFields bool
}

var DefaultClient = &Client{}

//
//
func (c *Client) Query(query string, dst interface{}, connectServerArgs ...interface{}) error {
	dv := reflect.ValueOf(dst)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return ErrInvalidEntityType
	}
	dv = dv.Elem()
	mat, elemType := checkMultiArg(dv)
	if mat == multiArgTypeInvalid {
		return ErrInvalidEntityType
	}

	lock.Lock()
	defer lock.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	err := ole.CoInitializeEx(0, ole.COINIT_MULTITHREADED)
	if err != nil {
		oleCode := err.(*ole.OleError).Code()
		if oleCode != ole.S_OK && oleCode != S_FALSE {
			return err
		}
	}
	defer ole.CoUninitialize()

	unknown, err := oleutil.CreateObject("WbemScripting.SWbemLocator")
	if err != nil {
		return err
	} else if unknown == nil {
		return ErrNilCreateObject
	}
	defer unknown.Release()

	wmi, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return err
	}
	defer wmi.Release()

	serviceRaw, err := oleutil.CallMethod(wmi, "ConnectServer", connectServerArgs...)
	if err != nil {
		return err
	}
	service := serviceRaw.ToIDispatch()
	defer serviceRaw.Clear()

	resultRaw, err := oleutil.CallMethod(service, "ExecQuery", query)
	if err != nil {
		return err
	}
	result := resultRaw.ToIDispatch()
	defer resultRaw.Clear()

	count, err := oleInt64(result, "Count")
	if err != nil {
		return err
	}

	enumProperty, err := result.GetProperty("_NewEnum")
	if err != nil {
		return err
	}
	defer enumProperty.Clear()

	enum, err := enumProperty.ToIUnknown().IEnumVARIANT(ole.IID_IEnumVariant)
	if err != nil {
		return err
	}
	if enum == nil {
		return fmt.Errorf("can't get IEnumVARIANT, enum is nil")
	}
	defer enum.Release()

	dv.Set(reflect.MakeSlice(dv.Type(), 0, int(count)))

	var errFieldMismatch error
	for itemRaw, length, err := enum.Next(1); length > 0; itemRaw, length, err = enum.Next(1) {
		if err != nil {
			return err
		}

		err := func() error {

			item := itemRaw.ToIDispatch()
			defer item.Release()

			ev := reflect.New(elemType)
			if err = c.loadEntity(ev.Interface(), item); err != nil {
				if _, ok := err.(*ErrFieldMismatch); ok {

					errFieldMismatch = err
				} else {
					return err
				}
			}
			if mat != multiArgTypeStructPtr {
				ev = ev.Elem()
			}
			dv.Set(reflect.Append(dv, ev))
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return errFieldMismatch
}

type ErrFieldMismatch struct {
	StructType reflect.Type
	FieldName  string
	Reason     string
}

func (e *ErrFieldMismatch) Error() string {
	return fmt.Sprintf("wmi: cannot load field %q into a %q: %s",
		e.FieldName, e.StructType, e.Reason)
}

var timeType = reflect.TypeOf(time.Time{})

func (c *Client) loadEntity(dst interface{}, src *ole.IDispatch) (errFieldMismatch error) {
	v := reflect.ValueOf(dst).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		of := f
		isPtr := f.Kind() == reflect.Ptr
		if isPtr {
			ptr := reflect.New(f.Type().Elem())
			f.Set(ptr)
			f = f.Elem()
		}
		n := v.Type().Field(i).Name
		if !f.CanSet() {
			return &ErrFieldMismatch{
				StructType: of.Type(),
				FieldName:  n,
				Reason:     "CanSet() is false",
			}
		}
		prop, err := oleutil.GetProperty(src, n)
		if err != nil {
			if !c.AllowMissingFields {
				errFieldMismatch = &ErrFieldMismatch{
					StructType: of.Type(),
					FieldName:  n,
					Reason:     "no such struct field",
				}
			}
			continue
		}
		defer prop.Clear()

		switch val := prop.Value().(type) {
		case int8, int16, int32, int64, int:
			v := reflect.ValueOf(val).Int()
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetInt(v)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				f.SetUint(uint64(v))
			default:
				return &ErrFieldMismatch{
					StructType: of.Type(),
					FieldName:  n,
					Reason:     "not an integer class",
				}
			}
		case uint8, uint16, uint32, uint64:
			v := reflect.ValueOf(val).Uint()
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetInt(int64(v))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				f.SetUint(v)
			default:
				return &ErrFieldMismatch{
					StructType: of.Type(),
					FieldName:  n,
					Reason:     "not an integer class",
				}
			}
		case string:
			switch f.Kind() {
			case reflect.String:
				f.SetString(val)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				iv, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return err
				}
				f.SetInt(iv)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				uv, err := strconv.ParseUint(val, 10, 64)
				if err != nil {
					return err
				}
				f.SetUint(uv)
			case reflect.Struct:
				switch f.Type() {
				case timeType:
					if len(val) == 25 {
						mins, err := strconv.Atoi(val[22:])
						if err != nil {
							return err
						}
						val = val[:22] + fmt.Sprintf("%02d%02d", mins/60, mins%60)
					}
					t, err := time.Parse("20060102150405.000000-0700", val)
					if err != nil {
						return err
					}
					f.Set(reflect.ValueOf(t))
				}
			}
		case bool:
			switch f.Kind() {
			case reflect.Bool:
				f.SetBool(val)
			default:
				return &ErrFieldMismatch{
					StructType: of.Type(),
					FieldName:  n,
					Reason:     "not a bool",
				}
			}
		case float32:
			switch f.Kind() {
			case reflect.Float32:
				f.SetFloat(float64(val))
			default:
				return &ErrFieldMismatch{
					StructType: of.Type(),
					FieldName:  n,
					Reason:     "not a Float32",
				}
			}
		default:
			typeof := reflect.TypeOf(val)
			if typeof == nil && (isPtr || c.NonePtrZero) {
				if (isPtr && c.PtrNil) || (!isPtr && c.NonePtrZero) {
					of.Set(reflect.Zero(of.Type()))
				}
				break
			}
			return &ErrFieldMismatch{
				StructType: of.Type(),
				FieldName:  n,
				Reason:     fmt.Sprintf("unsupported type (%T)", val),
			}
		}
	}
	return errFieldMismatch
}

type multiArgType int

const (
	multiArgTypeInvalid multiArgType = iota
	multiArgTypeStruct
	multiArgTypeStructPtr
)

//
func checkMultiArg(v reflect.Value) (m multiArgType, elemType reflect.Type) {
	if v.Kind() != reflect.Slice {
		return multiArgTypeInvalid, nil
	}
	elemType = v.Type().Elem()
	switch elemType.Kind() {
	case reflect.Struct:
		return multiArgTypeStruct, elemType
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return multiArgTypeStructPtr, elemType
		}
	}
	return multiArgTypeInvalid, nil
}

func oleInt64(item *ole.IDispatch, prop string) (int64, error) {
	v, err := oleutil.GetProperty(item, prop)
	if err != nil {
		return 0, err
	}
	defer v.Clear()

	i := int64(v.Val)
	return i, nil
}

func CreateQuery(src interface{}, where string) string {
	var b bytes.Buffer
	b.WriteString("SELECT ")
	s := reflect.Indirect(reflect.ValueOf(src))
	t := s.Type()
	if s.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return ""
	}
	var fields []string
	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i).Name)
	}
	b.WriteString(strings.Join(fields, ", "))
	b.WriteString(" FROM ")
	b.WriteString(t.Name())
	b.WriteString(" " + where)
	return b.String()
}
