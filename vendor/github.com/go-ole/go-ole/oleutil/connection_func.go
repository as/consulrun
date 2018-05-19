// +build !windows

package oleutil

import ole "github.com/go-ole/go-ole"

func ConnectObject(disp *ole.IDispatch, iid *ole.GUID, idisp interface{}) (uint32, error) {
	return 0, ole.NewError(ole.E_NOTIMPL)
}
