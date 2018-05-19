package dns

import "fmt"

var Version = V{1, 0, 4}

type V struct {
	Major, Minor, Patch int
}

func (v V) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
