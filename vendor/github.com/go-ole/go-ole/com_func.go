// +build !windows

package ole

import (
	"time"
	"unsafe"
)

//
//
func coInitialize() error {
	return NewError(E_NOTIMPL)
}

func coInitializeEx(coinit uint32) error {
	return NewError(E_NOTIMPL)
}

//
//
func CoInitialize(p uintptr) error {
	return NewError(E_NOTIMPL)
}

func CoInitializeEx(p uintptr, coinit uint32) error {
	return NewError(E_NOTIMPL)
}

func CoUninitialize() {}

func CoTaskMemFree(memptr uintptr) {}

//
//
//
func CLSIDFromProgID(progId string) (*GUID, error) {
	return nil, NewError(E_NOTIMPL)
}

//
//
func CLSIDFromString(str string) (*GUID, error) {
	return nil, NewError(E_NOTIMPL)
}

func StringFromCLSID(clsid *GUID) (string, error) {
	return "", NewError(E_NOTIMPL)
}

func IIDFromString(progId string) (*GUID, error) {
	return nil, NewError(E_NOTIMPL)
}

func StringFromIID(iid *GUID) (string, error) {
	return "", NewError(E_NOTIMPL)
}

func CreateInstance(clsid *GUID, iid *GUID) (*IUnknown, error) {
	return nil, NewError(E_NOTIMPL)
}

func GetActiveObject(clsid *GUID, iid *GUID) (*IUnknown, error) {
	return nil, NewError(E_NOTIMPL)
}

func VariantInit(v *VARIANT) error {
	return NewError(E_NOTIMPL)
}

func VariantClear(v *VARIANT) error {
	return NewError(E_NOTIMPL)
}

func SysAllocString(v string) *int16 {
	u := int16(0)
	return &u
}

func SysAllocStringLen(v string) *int16 {
	u := int16(0)
	return &u
}

func SysFreeString(v *int16) error {
	return NewError(E_NOTIMPL)
}

func SysStringLen(v *int16) uint32 {
	return uint32(0)
}

//
func CreateStdDispatch(unk *IUnknown, v uintptr, ptinfo *IUnknown) (*IDispatch, error) {
	return nil, NewError(E_NOTIMPL)
}

//
func CreateDispTypeInfo(idata *INTERFACEDATA) (*IUnknown, error) {
	return nil, NewError(E_NOTIMPL)
}

func copyMemory(dest unsafe.Pointer, src unsafe.Pointer, length uint32) {}

func GetUserDefaultLCID() uint32 {
	return uint32(0)
}

//
func GetMessage(msg *Msg, hwnd uint32, MsgFilterMin uint32, MsgFilterMax uint32) (int32, error) {
	return int32(0), NewError(E_NOTIMPL)
}

func DispatchMessage(msg *Msg) int32 {
	return int32(0)
}

func GetVariantDate(value float64) (time.Time, error) {
	return time.Now(), NewError(E_NOTIMPL)
}
