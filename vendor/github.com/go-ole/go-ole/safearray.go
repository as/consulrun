// Package is meant to retrieve and process safe array data returned from COM.

package ole

type SafeArrayBound struct {
	Elements   uint32
	LowerBound int32
}

type SafeArray struct {
	Dimensions   uint16
	FeaturesFlag uint16
	ElementsSize uint32
	LocksAmount  uint32
	Data         uint32
	Bounds       [16]byte
}

type SAFEARRAY SafeArray

type SAFEARRAYBOUND SafeArrayBound
