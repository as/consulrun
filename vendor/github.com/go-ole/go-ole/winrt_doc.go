// +build !windows

package ole

func RoInitialize(thread_type uint32) (err error) {
	return NewError(E_NOTIMPL)
}

func RoActivateInstance(clsid string) (ins *IInspectable, err error) {
	return nil, NewError(E_NOTIMPL)
}

func RoGetActivationFactory(clsid string, iid *GUID) (ins *IInspectable, err error) {
	return nil, NewError(E_NOTIMPL)
}

type HString uintptr

func NewHString(s string) (hstring HString, err error) {
	return HString(uintptr(0)), NewError(E_NOTIMPL)
}

func DeleteHString(hstring HString) (err error) {
	return NewError(E_NOTIMPL)
}

func (h HString) String() string {
	return ""
}
