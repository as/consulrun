package multierror

import (
	"fmt"
	"strings"
)

type ErrorFormatFunc func([]error) string

func ListFormatFunc(es []error) string {
	points := make([]string, len(es))
	for i, err := range es {
		points[i] = fmt.Sprintf("* %s", err)
	}

	return fmt.Sprintf(
		"%d error(s) occurred:\n\n%s",
		len(es), strings.Join(points, "\n"))
}
