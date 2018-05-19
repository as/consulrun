package reflectwalk

type Location uint

const (
	None Location = iota
	Map
	MapKey
	MapValue
	Slice
	SliceElem
	Struct
	StructField
	WalkLoc
)
