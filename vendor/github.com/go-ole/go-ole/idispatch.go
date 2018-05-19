package ole

import "unsafe"

type IDispatch struct {
	IUnknown
}

type IDispatchVtbl struct {
	IUnknownVtbl
	GetTypeInfoCount uintptr
	GetTypeInfo      uintptr
	GetIDsOfNames    uintptr
	Invoke           uintptr
}

func (v *IDispatch) VTable() *IDispatchVtbl {
	return (*IDispatchVtbl)(unsafe.Pointer(v.RawVTable))
}

func (v *IDispatch) GetIDsOfName(names []string) (dispid []int32, err error) {
	dispid, err = getIDsOfName(v, names)
	return
}

func (v *IDispatch) Invoke(dispid int32, dispatch int16, params ...interface{}) (result *VARIANT, err error) {
	result, err = invoke(v, dispid, dispatch, params...)
	return
}

func (v *IDispatch) GetTypeInfoCount() (c uint32, err error) {
	c, err = getTypeInfoCount(v)
	return
}

func (v *IDispatch) GetTypeInfo() (tinfo *ITypeInfo, err error) {
	tinfo, err = getTypeInfo(v)
	return
}

//
func (v *IDispatch) GetSingleIDOfName(name string) (displayID int32, err error) {
	var displayIDs []int32
	displayIDs, err = v.GetIDsOfName([]string{name})
	if err != nil {
		return
	}
	displayID = displayIDs[0]
	return
}

//
//
func (v *IDispatch) InvokeWithOptionalArgs(name string, dispatch int16, params []interface{}) (result *VARIANT, err error) {
	displayID, err := v.GetSingleIDOfName(name)
	if err != nil {
		return
	}

	if len(params) < 1 {
		result, err = v.Invoke(displayID, dispatch)
	} else {
		result, err = v.Invoke(displayID, dispatch, params...)
	}

	return
}

func (v *IDispatch) CallMethod(name string, params ...interface{}) (*VARIANT, error) {
	return v.InvokeWithOptionalArgs(name, DISPATCH_METHOD, params)
}

//
func (v *IDispatch) GetProperty(name string, params ...interface{}) (*VARIANT, error) {
	return v.InvokeWithOptionalArgs(name, DISPATCH_PROPERTYGET, params)
}

func (v *IDispatch) PutProperty(name string, params ...interface{}) (*VARIANT, error) {
	return v.InvokeWithOptionalArgs(name, DISPATCH_PROPERTYPUT, params)
}
