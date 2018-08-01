package lib

import (
	"io"
	"strings"

	"github.com/as/consulrun/hashicorp/yamux"
)

var yamuxStreamClosed = yamux.ErrStreamClosed.Error()
var yamuxSessionShutdown = yamux.ErrSessionShutdown.Error()

func IsErrEOF(err error) bool {
	if err == io.EOF {
		return true
	}

	errStr := err.Error()
	if strings.Contains(errStr, yamuxStreamClosed) ||
		strings.Contains(errStr, yamuxSessionShutdown) {
		return true
	}

	return false
}
