package parser

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/hcl/hcl/token"
)

type PosError struct {
	Pos token.Pos
	Err error
}

func (e *PosError) Error() string {
	return fmt.Sprintf("At %s: %s", e.Pos, e.Err)
}
