// +build !ent

package sentinel

import (
	"log"
)

func New(logger *log.Logger) Evaluator {
	return nil
}
